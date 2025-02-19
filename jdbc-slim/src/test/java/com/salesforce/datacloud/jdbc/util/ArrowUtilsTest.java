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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.QueryResultSetMetadata;
import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.val;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.proto.Common;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
class ArrowUtilsTest {
    @InjectSoftAssertions
    SoftAssertions softly;

    @Test
    void testConvertArrowFieldsToColumnMetaData() {
        List<Field> arrowFields = ImmutableList.of(new Field("id", FieldType.nullable(new ArrowType.Utf8()), null));
        final List<ColumnMetaData> expectedColumnMetadata =
                ImmutableList.of(ColumnMetaData.fromProto(Common.ColumnMetaData.newBuilder()
                        .setColumnName("id")
                        .setOrdinal(0)
                        .build()));
        List<ColumnMetaData> actualColumnMetadata = ArrowUtils.toColumnMetaData(arrowFields);
        assertThat(actualColumnMetadata).hasSameSizeAs(expectedColumnMetadata);
        val expected = expectedColumnMetadata.get(0);
        val actual = actualColumnMetadata.get(0);

        softly.assertThat(actual.columnName).isEqualTo(expected.columnName);
        softly.assertThat(actual.ordinal).isEqualTo(expected.ordinal);
        softly.assertThat(actual.type.name)
                .isEqualTo(JDBCType.valueOf(Types.VARCHAR).getName());
    }

    @Test
    void testConvertArrowFieldsToColumnMetaDataTypes() {
        Map<String, List<Field>> testCases = new HashMap<>();
        testCases.put(
                JDBCType.valueOf(Types.TINYINT).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Int(8, true)), null)));
        testCases.put(
                JDBCType.valueOf(Types.SMALLINT).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Int(16, true)), null)));
        testCases.put(
                JDBCType.valueOf(Types.INTEGER).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        testCases.put(
                JDBCType.valueOf(Types.BIGINT).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        testCases.put(
                JDBCType.valueOf(Types.BOOLEAN).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Bool()), null)));
        testCases.put(
                JDBCType.valueOf(Types.VARCHAR).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Utf8()), null)));
        testCases.put(
                JDBCType.valueOf(Types.FLOAT).getName(),
                ImmutableList.of(new Field(
                        "", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)));
        testCases.put(
                JDBCType.valueOf(Types.DOUBLE).getName(),
                ImmutableList.of(new Field(
                        "", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
        testCases.put(
                JDBCType.valueOf(Types.DATE).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)));
        testCases.put(
                JDBCType.valueOf(Types.TIME).getName(),
                ImmutableList.of(
                        new Field("", FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), null)));
        testCases.put(
                JDBCType.valueOf(Types.TIMESTAMP).getName(),
                ImmutableList.of(
                        new Field("", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null)));
        testCases.put(
                JDBCType.valueOf(Types.DECIMAL).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.Decimal(1, 1, 128)), null)));
        testCases.put(
                JDBCType.valueOf(Types.ARRAY).getName(),
                ImmutableList.of(new Field("", FieldType.nullable(new ArrowType.List()), null)));

        for (val entry : testCases.entrySet()) {
            List<ColumnMetaData> actual = ArrowUtils.toColumnMetaData(entry.getValue());
            softly.assertThat(actual.get(0).type.name).isEqualTo(entry.getKey());
        }
    }

    private static Stream<Arguments> arrowTypes() {
        return Stream.of(
                Arguments.of(new ArrowType.Int(8, true), Types.TINYINT),
                Arguments.of(new ArrowType.Int(16, true), Types.SMALLINT),
                Arguments.of(new ArrowType.Int(32, true), Types.INTEGER),
                Arguments.of(new ArrowType.Int(64, true), Types.BIGINT),
                Arguments.of(new ArrowType.Bool(), Types.BOOLEAN),
                Arguments.of(new ArrowType.Utf8(), Types.VARCHAR),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), Types.FLOAT),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), Types.DOUBLE),
                Arguments.of(new ArrowType.LargeUtf8(), Types.LONGVARCHAR),
                Arguments.of(new ArrowType.Binary(), Types.VARBINARY),
                Arguments.of(new ArrowType.FixedSizeBinary(8), Types.BINARY),
                Arguments.of(new ArrowType.LargeBinary(), Types.LONGVARBINARY),
                Arguments.of(new ArrowType.Decimal(1, 1, 128), Types.DECIMAL),
                Arguments.of(new ArrowType.Date(DateUnit.DAY), Types.DATE),
                Arguments.of(new ArrowType.Time(TimeUnit.MICROSECOND, 64), Types.TIME),
                Arguments.of(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), Types.TIMESTAMP),
                Arguments.of(new ArrowType.List(), Types.ARRAY),
                Arguments.of(new ArrowType.LargeList(), Types.ARRAY),
                Arguments.of(new ArrowType.FixedSizeList(1), Types.ARRAY),
                Arguments.of(new ArrowType.Map(true), Types.JAVA_OBJECT),
                Arguments.of(new ArrowType.Duration(TimeUnit.MICROSECOND), Types.JAVA_OBJECT),
                Arguments.of(new ArrowType.Interval(IntervalUnit.DAY_TIME), Types.JAVA_OBJECT),
                Arguments.of(new ArrowType.Struct(), Types.STRUCT),
                Arguments.of(new ArrowType.Null(), Types.NULL));
    }

    @ParameterizedTest
    @MethodSource("arrowTypes")
    void testGetSQLTypeFromArrowTypes(ArrowType arrowType, int expectedSqlType) {
        softly.assertThat(ArrowUtils.getSQLTypeFromArrowType(arrowType)).isEqualTo(expectedSqlType);
    }

    @Test
    void testConvertJDBCMetadataToAvaticaColumns() throws SQLException {
        ResultSetMetaData resultSetMetaData = mockResultSetMetadata();
        List<ColumnMetaData> columnMetaDataList = ArrowUtils.convertJDBCMetadataToAvaticaColumns(resultSetMetaData, 7);

        for (int i = 0; i < columnMetaDataList.size(); i++) {
            val actual = columnMetaDataList.get(i);

            softly.assertThat(actual.type.id).isEqualTo(resultSetMetaData.getColumnType(i + 1));
            softly.assertThat(actual.columnName).isEqualTo(resultSetMetaData.getColumnName(i + 1));
            softly.assertThat(actual.type.name).isEqualTo(resultSetMetaData.getColumnTypeName(i + 1));
        }
    }

    @Test
    void testConvertJDBCMetadataToAvaticaColumnsEmptyMetadata() {
        assertThat(ArrowUtils.convertJDBCMetadataToAvaticaColumns(null, 7)).isEmpty();
    }

    private ResultSetMetaData mockResultSetMetadata() {
        String[] columnNames = {"col1", "col2", "col3", "col4", "col5", "col6", "col7"};
        String[] columnTypes = {
            "INTEGER", "VARCHAR", "DECIMAL", "TIMESTAMP WITH TIME ZONE", "ARRAY", "STRUCT", "MULTISET"
        };
        Integer[] columnTypeIds = {4, 12, 3, 2013, 2003, 2002, 2003};

        return new QueryResultSetMetadata(
                Arrays.asList(columnNames), Arrays.asList(columnTypes), Arrays.asList(columnTypeIds));
    }

    @Test
    void testCreateSchemaFromParametersValid() {
        List<ParameterBinding> parameterBindings = Arrays.asList(
                new ParameterBinding(Types.VARCHAR, "string"),
                new ParameterBinding(Types.INTEGER, 1),
                new ParameterBinding(Types.BIGINT, 123456789L),
                new ParameterBinding(Types.BOOLEAN, true),
                new ParameterBinding(Types.TINYINT, (byte) 1),
                new ParameterBinding(Types.SMALLINT, (short) 1),
                new ParameterBinding(Types.DATE, new Date(1)),
                new ParameterBinding(Types.TIME, new Time(1)),
                new ParameterBinding(Types.TIMESTAMP, new Timestamp(1)),
                new ParameterBinding(Types.DECIMAL, new BigDecimal("123.45")),
                new ParameterBinding(Types.ARRAY, ImmutableList.of(1, 2, 3)));

        Schema schema = ArrowUtils.createSchemaFromParameters(parameterBindings);

        Assertions.assertNotNull(schema);
        assertEquals(11, schema.getFields().size());

        Field field = schema.getFields().get(0);
        assertEquals("1", field.getName());
        assertInstanceOf(ArrowType.Utf8.class, field.getType());

        field = schema.getFields().get(1);
        assertInstanceOf(ArrowType.Int.class, field.getType());
        ArrowType.Int intType = (ArrowType.Int) field.getType();
        assertEquals(32, intType.getBitWidth());

        field = schema.getFields().get(2);
        assertInstanceOf(ArrowType.Int.class, field.getType());
        ArrowType.Int longType = (ArrowType.Int) field.getType();
        assertEquals(64, longType.getBitWidth());

        field = schema.getFields().get(3);
        assertInstanceOf(ArrowType.Bool.class, field.getType());

        field = schema.getFields().get(4);
        assertInstanceOf(ArrowType.Int.class, field.getType());
        ArrowType.Int byteType = (ArrowType.Int) field.getType();
        assertEquals(8, byteType.getBitWidth());

        field = schema.getFields().get(5);
        assertInstanceOf(ArrowType.Int.class, field.getType());
        ArrowType.Int shortType = (ArrowType.Int) field.getType();
        assertEquals(16, shortType.getBitWidth());

        field = schema.getFields().get(6);
        assertInstanceOf(ArrowType.Date.class, field.getType());
        ArrowType.Date dateType = (ArrowType.Date) field.getType();
        assertEquals(DateUnit.DAY, dateType.getUnit());

        field = schema.getFields().get(7);
        assertInstanceOf(ArrowType.Time.class, field.getType());
        ArrowType.Time timeType = (ArrowType.Time) field.getType();
        assertEquals(TimeUnit.MICROSECOND, timeType.getUnit());
        assertEquals(64, timeType.getBitWidth());

        field = schema.getFields().get(8);
        assertInstanceOf(ArrowType.Timestamp.class, field.getType());
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) field.getType();
        assertEquals(TimeUnit.MICROSECOND, timestampType.getUnit());
        assertEquals("UTC", timestampType.getTimezone());

        field = schema.getFields().get(9);
        assertInstanceOf(ArrowType.Decimal.class, field.getType());
        ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
        assertEquals(5, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());

        field = schema.getFields().get(10);
        assertInstanceOf(ArrowType.List.class, field.getType());
    }
}
