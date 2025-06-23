/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.proto.Common;

@Slf4j
public final class ArrowUtils {

    private ArrowUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static List<ColumnMetaData> toColumnMetaData(List<Field> fields) {
        AtomicInteger index = new AtomicInteger();
        return fields.stream()
                .map(field -> {
                    try {
                        ColumnType type = ArrowToColumnTypeMapper.toColumnType(field);
                        val avaticaType = getAvaticaType(type).toProto();

                        final Common.ColumnMetaData.Builder builder = Common.ColumnMetaData.newBuilder()
                                .setOrdinal(index.getAndIncrement())
                                .setColumnName(field.getName())
                                .setLabel(field.getName())
                                .setType(avaticaType)
                                .setPrecision(type.getPrecisionOrStringLength())
                                .setScale(type.getScale())
                                .setCaseSensitive(type.isCaseSensitive())
                                .setDisplaySize(type.getDisplaySize())
                                .setSigned(type.isSigned())
                                .setNullable(
                                        field.isNullable()
                                                ? ResultSetMetaData.columnNullable
                                                : ResultSetMetaData.columnNoNulls)
                                .setSearchable(true)
                                .setWritable(false);
                        return ColumnMetaData.fromProto(builder.build());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    /** Converts from JDBC metadata to Avatica columns. */
    public static List<ColumnMetaData> convertJDBCMetadataToAvaticaColumns(ResultSetMetaData metaData, int maxSize) {
        if (metaData == null) {
            return Collections.emptyList();
        }

        return Stream.iterate(1, i -> i + 1)
                .limit(maxSize)
                .map(i -> {
                    try {
                        val columnType = new ColumnType(JDBCType.valueOf(metaData.getColumnType(i)));
                        val avaticaType = getAvaticaType(columnType);
                        return new ColumnMetaData(
                                i - 1,
                                metaData.isAutoIncrement(i),
                                metaData.isCaseSensitive(i),
                                metaData.isSearchable(i),
                                metaData.isCurrency(i),
                                metaData.isNullable(i),
                                metaData.isSigned(i),
                                metaData.getColumnDisplaySize(i),
                                metaData.getColumnLabel(i),
                                metaData.getColumnName(i),
                                metaData.getSchemaName(i),
                                metaData.getPrecision(i),
                                metaData.getScale(i),
                                metaData.getTableName(i),
                                metaData.getCatalogName(i),
                                avaticaType,
                                metaData.isReadOnly(i),
                                metaData.isWritable(i),
                                metaData.isDefinitelyWritable(i),
                                metaData.getColumnClassName(i));
                    } catch (SQLException e) {
                        log.error("Error converting JDBC Metadata to Avatica Columns");
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private static final Map<Integer, Function<ParameterBinding, FieldType>> SQL_TYPE_TO_FIELD_TYPE =
            ImmutableMap.ofEntries(
                    Maps.immutableEntry(Types.VARCHAR, pb -> FieldType.nullable(new ArrowType.Utf8())),
                    Maps.immutableEntry(Types.INTEGER, pb -> FieldType.nullable(new ArrowType.Int(32, true))),
                    Maps.immutableEntry(Types.BIGINT, pb -> FieldType.nullable(new ArrowType.Int(64, true))),
                    Maps.immutableEntry(Types.BOOLEAN, pb -> FieldType.nullable(new ArrowType.Bool())),
                    Maps.immutableEntry(Types.TINYINT, pb -> FieldType.nullable(new ArrowType.Int(8, true))),
                    Maps.immutableEntry(Types.SMALLINT, pb -> FieldType.nullable(new ArrowType.Int(16, true))),
                    Maps.immutableEntry(Types.DATE, pb -> FieldType.nullable(new ArrowType.Date(DateUnit.DAY))),
                    Maps.immutableEntry(
                            Types.TIME, pb -> FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64))),
                    Maps.immutableEntry(
                            Types.TIMESTAMP,
                            pb -> FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))),
                    Maps.immutableEntry(
                            Types.FLOAT,
                            pb -> FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))),
                    Maps.immutableEntry(
                            Types.DOUBLE,
                            pb -> FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))),
                    Maps.immutableEntry(Types.DECIMAL, ArrowUtils::createDecimalFieldType),
                    Maps.immutableEntry(Types.ARRAY, pb -> FieldType.nullable(new ArrowType.List())));

    /**
     * Creates a Schema from a list of ParameterBinding.
     *
     * @param parameterBindings a list of ParameterBinding objects
     * @return a Schema object corresponding to the provided parameters
     */
    public static Schema createSchemaFromParameters(List<ParameterBinding> parameterBindings) {
        if (parameterBindings == null) {
            throw new IllegalArgumentException("ParameterBindings list cannot be null");
        }
        List<Field> fields = IntStream.range(0, parameterBindings.size())
                .mapToObj(i -> createField(parameterBindings.get(i), i + 1))
                .collect(Collectors.toList());

        return new Schema(fields);
    }

    /**
     * Creates a Field based on the ParameterBinding and its index.
     *
     * @param parameterBinding the ParameterBinding object
     * @param index the index of the parameter in the list
     * @return a Field object with a name based on the index and a FieldType based on the parameter
     */
    private static Field createField(ParameterBinding parameterBinding, int index) {
        FieldType fieldType = determineFieldType(parameterBinding);
        return new Field(String.valueOf(index), fieldType, null);
    }

    /**
     * Determines the Arrow FieldType for a given ParameterBinding.
     *
     * @param parameterBinding the ParameterBinding object
     * @return the corresponding Arrow FieldType
     */
    private static FieldType determineFieldType(ParameterBinding parameterBinding) {
        if (parameterBinding == null) {
            // Default type for null values, using VARCHAR for simplicity
            return FieldType.nullable(new ArrowType.Utf8());
        }

        int sqlType = parameterBinding.getSqlType();
        Function<ParameterBinding, FieldType> fieldTypeFunction = SQL_TYPE_TO_FIELD_TYPE.get(sqlType);

        if (fieldTypeFunction != null) {
            return fieldTypeFunction.apply(parameterBinding);
        } else {
            throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
        }
    }

    /**
     * Creates a Decimal Arrow FieldType based on a ParameterBinding.
     *
     * @param parameterBinding the ParameterBinding object
     * @return the corresponding Arrow FieldType for Decimal
     */
    private static FieldType createDecimalFieldType(ParameterBinding parameterBinding) {
        if (parameterBinding.getValue() instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) parameterBinding.getValue();
            return FieldType.nullable(new ArrowType.Decimal(bd.precision(), bd.scale(), 128));
        }
        throw new IllegalArgumentException("Decimal type requires a BigDecimal value");
    }

    public static byte[] toArrowByteArray(List<ParameterBinding> parameters, Calendar calendar) throws IOException {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Schema schema = ArrowUtils.createSchemaFromParameters(parameters);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            VectorPopulator.populateVectors(root, parameters, calendar);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            return outputStream.toByteArray();
        }
    }

    private static ColumnMetaData.AvaticaType getAvaticaType(ColumnType columnType) throws SQLException {
        final ColumnMetaData.AvaticaType avaticaType;
        int typeId = columnType.getType().getVendorTypeNumber();
        if (typeId == JDBCType.TIMESTAMP_WITH_TIMEZONE.getVendorTypeNumber()) {
            // We have to expose as `TIMESTAMP` here as Avatica doesn't support TIMESTAMPT_WITH_TIMEZONE accessors
            typeId = JDBCType.TIMESTAMP.getVendorTypeNumber();
        }
        String typeName = columnType.getType().getName();
        final SqlType sqlType = SqlType.valueOf(typeId);
        final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(sqlType.internal);
        if (sqlType == SqlType.ARRAY) {
            ColumnMetaData.AvaticaType arrayValueType = getAvaticaType(columnType.getArrayElementType());
            avaticaType = ColumnMetaData.array(arrayValueType, typeName, rep);
        } else {
            avaticaType = ColumnMetaData.scalar(typeId, typeName, rep);
        }
        return avaticaType;
    }
}
