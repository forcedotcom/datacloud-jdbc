/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

class ColumnTypeTest {

    @Test
    void getJavaTypeForBooleanReturnsBoolean() {
        assertThat(new ColumnType(JDBCType.BOOLEAN, true).getJavaType()).isEqualTo(Boolean.class);
    }

    @Test
    void getJavaTypeForTinyintReturnsShort() {
        assertThat(new ColumnType(JDBCType.TINYINT, true).getJavaType()).isEqualTo(Short.class);
    }

    @Test
    void getJavaTypeForSmallintReturnsInteger() {
        assertThat(new ColumnType(JDBCType.SMALLINT, true).getJavaType()).isEqualTo(Integer.class);
    }

    @Test
    void getJavaTypeForIntegerReturnsInteger() {
        assertThat(new ColumnType(JDBCType.INTEGER, true).getJavaType()).isEqualTo(Integer.class);
    }

    @Test
    void getJavaTypeForBigintReturnsLong() {
        assertThat(new ColumnType(JDBCType.BIGINT, true).getJavaType()).isEqualTo(Long.class);
    }

    @Test
    void getJavaTypeForDecimalReturnsBigDecimal() {
        assertThat(new ColumnType(JDBCType.DECIMAL, 10, 2, true).getJavaType()).isEqualTo(BigDecimal.class);
    }

    @Test
    void getJavaTypeForNumericReturnsBigDecimal() {
        assertThat(new ColumnType(JDBCType.NUMERIC, 10, 2, true).getJavaType()).isEqualTo(BigDecimal.class);
    }

    @Test
    void getJavaTypeForFloatReturnsFloat() {
        assertThat(new ColumnType(JDBCType.FLOAT, true).getJavaType()).isEqualTo(Float.class);
    }

    @Test
    void getJavaTypeForRealReturnsFloat() {
        assertThat(new ColumnType(JDBCType.REAL, true).getJavaType()).isEqualTo(Float.class);
    }

    @Test
    void getJavaTypeForDoubleReturnsDouble() {
        assertThat(new ColumnType(JDBCType.DOUBLE, true).getJavaType()).isEqualTo(Double.class);
    }

    @Test
    void getJavaTypeForCharReturnsString() {
        assertThat(new ColumnType(JDBCType.CHAR, 10, 0, true).getJavaType()).isEqualTo(String.class);
    }

    @Test
    void getJavaTypeForVarcharReturnsString() {
        assertThat(new ColumnType(JDBCType.VARCHAR, 255, 0, true).getJavaType()).isEqualTo(String.class);
    }

    @Test
    void getJavaTypeForBinaryReturnsByteArray() {
        assertThat(new ColumnType(JDBCType.BINARY, true).getJavaType()).isEqualTo(byte[].class);
    }

    @Test
    void getJavaTypeForVarbinaryReturnsByteArray() {
        assertThat(new ColumnType(JDBCType.VARBINARY, true).getJavaType()).isEqualTo(byte[].class);
    }

    @Test
    void getJavaTypeForDateReturnsDate() {
        assertThat(new ColumnType(JDBCType.DATE, true).getJavaType()).isEqualTo(Date.class);
    }

    @Test
    void getJavaTypeForTimeReturnsTime() {
        assertThat(new ColumnType(JDBCType.TIME, true).getJavaType()).isEqualTo(Time.class);
    }

    @Test
    void getJavaTypeForTimestampReturnsTimestamp() {
        assertThat(new ColumnType(JDBCType.TIMESTAMP, true).getJavaType()).isEqualTo(Timestamp.class);
    }

    @Test
    void getJavaTypeForTimestampWithTimezoneReturnsTimestamp() {
        assertThat(new ColumnType(JDBCType.TIMESTAMP_WITH_TIMEZONE, true).getJavaType())
                .isEqualTo(Timestamp.class);
    }

    @Test
    void getJavaTypeForArrayReturnsArray() {
        ColumnType elementType = new ColumnType(JDBCType.VARCHAR, 255, 0, true);
        assertThat(new ColumnType(JDBCType.ARRAY, elementType, true).getJavaType())
                .isEqualTo(Array.class);
    }

    @Test
    void getJavaTypeForNullReturnsObject() {
        assertThat(new ColumnType(JDBCType.NULL, true).getJavaType()).isEqualTo(Object.class);
    }

    @Test
    void getJavaTypeForUnsupportedTypeThrows() {
        assertThatThrownBy(() -> new ColumnType(JDBCType.BLOB, true).getJavaType())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getPrecisionForIntegerTypes() {
        assertThat(new ColumnType(JDBCType.TINYINT, true).getPrecisionOrStringLength())
                .isEqualTo(3);
        assertThat(new ColumnType(JDBCType.SMALLINT, true).getPrecisionOrStringLength())
                .isEqualTo(5);
        assertThat(new ColumnType(JDBCType.INTEGER, true).getPrecisionOrStringLength())
                .isEqualTo(10);
        assertThat(new ColumnType(JDBCType.BIGINT, true).getPrecisionOrStringLength())
                .isEqualTo(19);
    }

    @Test
    void getPrecisionForDecimal() {
        assertThat(new ColumnType(JDBCType.DECIMAL, 18, 4, true).getPrecisionOrStringLength())
                .isEqualTo(18);
    }

    @Test
    void getScaleForDecimal() {
        assertThat(new ColumnType(JDBCType.DECIMAL, 18, 4, true).getScale()).isEqualTo(4);
    }

    @Test
    void getScaleForIntegerTypesIsZero() {
        assertThat(new ColumnType(JDBCType.INTEGER, true).getScale()).isEqualTo(0);
        assertThat(new ColumnType(JDBCType.BIGINT, true).getScale()).isEqualTo(0);
    }

    @Test
    void isSignedForNumericTypes() {
        assertThat(new ColumnType(JDBCType.TINYINT, true).isSigned()).isTrue();
        assertThat(new ColumnType(JDBCType.INTEGER, true).isSigned()).isTrue();
        assertThat(new ColumnType(JDBCType.DOUBLE, true).isSigned()).isTrue();
        assertThat(new ColumnType(JDBCType.DECIMAL, 10, 2, true).isSigned()).isTrue();
    }

    @Test
    void isSignedForNonNumericTypes() {
        assertThat(new ColumnType(JDBCType.VARCHAR, 255, 0, true).isSigned()).isFalse();
        assertThat(new ColumnType(JDBCType.BOOLEAN, true).isSigned()).isFalse();
        assertThat(new ColumnType(JDBCType.DATE, true).isSigned()).isFalse();
    }

    @Test
    void getDisplaySizeForIntegerIncludesSign() {
        assertThat(new ColumnType(JDBCType.INTEGER, true).getDisplaySize()).isEqualTo(11);
    }

    @Test
    void getDisplaySizeForDecimalWithScaleIncludesSignAndDecimalPoint() {
        assertThat(new ColumnType(JDBCType.DECIMAL, 10, 2, true).getDisplaySize())
                .isEqualTo(12);
    }

    @Test
    void getDisplaySizeForDecimalWithoutScale() {
        assertThat(new ColumnType(JDBCType.DECIMAL, 10, 0, true).getDisplaySize())
                .isEqualTo(11);
    }
}
