/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnType;
import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public final class ArrowUtils {

    private ArrowUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static List<ColumnMetadata> toColumnMetaData(List<Field> fields) {
        return fields.stream()
                .map(field -> {
                    ColumnType type = ArrowToColumnTypeMapper.toColumnType(field);
                    return new ColumnMetadata(
                            field.getName(), type, type.getType().getName());
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
                            pb -> FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))),
                    Maps.immutableEntry(
                            Types.TIMESTAMP_WITH_TIMEZONE,
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
}
