/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.types;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.junit.jupiter.api.Test;

class HyperTypesTest {

    @Test
    void jdbcTypeMapping() {
        assertThat(HyperTypes.toJdbcType(HyperType.bool(true))).isEqualTo(JDBCType.BOOLEAN);
        assertThat(HyperTypes.toJdbcType(HyperType.int8(true))).isEqualTo(JDBCType.TINYINT);
        assertThat(HyperTypes.toJdbcType(HyperType.int16(true))).isEqualTo(JDBCType.SMALLINT);
        assertThat(HyperTypes.toJdbcType(HyperType.int32(true))).isEqualTo(JDBCType.INTEGER);
        assertThat(HyperTypes.toJdbcType(HyperType.int64(true))).isEqualTo(JDBCType.BIGINT);
        assertThat(HyperTypes.toJdbcType(HyperType.oid(true))).isEqualTo(JDBCType.BIGINT);
        assertThat(HyperTypes.toJdbcType(HyperType.float4(true))).isEqualTo(JDBCType.REAL);
        assertThat(HyperTypes.toJdbcType(HyperType.float8(true))).isEqualTo(JDBCType.DOUBLE);
        assertThat(HyperTypes.toJdbcType(HyperType.decimal(10, 2, true))).isEqualTo(JDBCType.DECIMAL);
        assertThat(HyperTypes.toJdbcType(HyperType.fixedChar(1, true))).isEqualTo(JDBCType.CHAR);
        assertThat(HyperTypes.toJdbcType(HyperType.varchar(255, true))).isEqualTo(JDBCType.VARCHAR);
        assertThat(HyperTypes.toJdbcType(HyperType.varcharUnlimited(true))).isEqualTo(JDBCType.VARCHAR);
        assertThat(HyperTypes.toJdbcType(HyperType.binary(10, true))).isEqualTo(JDBCType.BINARY);
        assertThat(HyperTypes.toJdbcType(HyperType.varbinary(true))).isEqualTo(JDBCType.VARBINARY);
        assertThat(HyperTypes.toJdbcType(HyperType.date(true))).isEqualTo(JDBCType.DATE);
        assertThat(HyperTypes.toJdbcType(HyperType.time(true))).isEqualTo(JDBCType.TIME);
        assertThat(HyperTypes.toJdbcType(HyperType.timeTz(true))).isEqualTo(JDBCType.TIME_WITH_TIMEZONE);
        assertThat(HyperTypes.toJdbcType(HyperType.timestamp(true))).isEqualTo(JDBCType.TIMESTAMP);
        assertThat(HyperTypes.toJdbcType(HyperType.timestampTz(true))).isEqualTo(JDBCType.TIMESTAMP_WITH_TIMEZONE);
        assertThat(HyperTypes.toJdbcType(HyperType.array(HyperType.int32(true), true)))
                .isEqualTo(JDBCType.ARRAY);
        assertThat(HyperTypes.toJdbcType(HyperType.nullType())).isEqualTo(JDBCType.NULL);
        assertThat(HyperTypes.toJdbcType(HyperType.interval(true))).isEqualTo(JDBCType.OTHER);
        assertThat(HyperTypes.toJdbcType(HyperType.json(true))).isEqualTo(JDBCType.OTHER);
    }

    @Test
    void jdbcTypeCodeReturnsJavaSqlTypesValue() {
        assertThat(HyperTypes.toJdbcTypeCode(HyperType.int32(true))).isEqualTo(Types.INTEGER);
        assertThat(HyperTypes.toJdbcTypeCode(HyperType.varcharUnlimited(true))).isEqualTo(Types.VARCHAR);
        assertThat(HyperTypes.toJdbcTypeCode(HyperType.decimal(10, 2, true))).isEqualTo(Types.DECIMAL);
    }

    @Test
    void jdbcTypeNameIsUppercase() {
        assertThat(HyperTypes.toJdbcTypeName(HyperType.bool(true))).isEqualTo("BOOLEAN");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.int64(true))).isEqualTo("BIGINT");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.oid(true))).isEqualTo("BIGINT");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.varcharUnlimited(true))).isEqualTo("VARCHAR");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.decimal(18, 2, true))).isEqualTo("DECIMAL");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.timestampTz(true))).isEqualTo("TIMESTAMP_WITH_TIMEZONE");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.interval(true))).isEqualTo("INTERVAL");
        assertThat(HyperTypes.toJdbcTypeName(HyperType.json(true))).isEqualTo("JSON");
    }

    @Test
    void javaClassMapping() {
        assertThat(HyperTypes.toJavaClass(HyperType.bool(true))).isEqualTo(Boolean.class);
        assertThat(HyperTypes.toJavaClass(HyperType.int8(true))).isEqualTo(Short.class);
        assertThat(HyperTypes.toJavaClass(HyperType.int16(true))).isEqualTo(Integer.class);
        assertThat(HyperTypes.toJavaClass(HyperType.int32(true))).isEqualTo(Integer.class);
        assertThat(HyperTypes.toJavaClass(HyperType.int64(true))).isEqualTo(Long.class);
        assertThat(HyperTypes.toJavaClass(HyperType.oid(true))).isEqualTo(Long.class);
        assertThat(HyperTypes.toJavaClass(HyperType.float4(true))).isEqualTo(Float.class);
        assertThat(HyperTypes.toJavaClass(HyperType.float8(true))).isEqualTo(Double.class);
        assertThat(HyperTypes.toJavaClass(HyperType.decimal(10, 2, true))).isEqualTo(BigDecimal.class);
        assertThat(HyperTypes.toJavaClass(HyperType.varchar(10, true))).isEqualTo(String.class);
        assertThat(HyperTypes.toJavaClass(HyperType.fixedChar(1, true))).isEqualTo(String.class);
        assertThat(HyperTypes.toJavaClass(HyperType.binary(10, true))).isEqualTo(byte[].class);
        assertThat(HyperTypes.toJavaClass(HyperType.varbinary(true))).isEqualTo(byte[].class);
        assertThat(HyperTypes.toJavaClass(HyperType.date(true))).isEqualTo(Date.class);
        assertThat(HyperTypes.toJavaClass(HyperType.time(true))).isEqualTo(Time.class);
        assertThat(HyperTypes.toJavaClass(HyperType.timestamp(true))).isEqualTo(Timestamp.class);
        assertThat(HyperTypes.toJavaClass(HyperType.timestampTz(true))).isEqualTo(Timestamp.class);
        assertThat(HyperTypes.toJavaClass(HyperType.array(HyperType.int32(true), true)))
                .isEqualTo(Array.class);
        assertThat(HyperTypes.toJavaClass(HyperType.nullType())).isEqualTo(Object.class);
        assertThat(HyperTypes.toJavaClass(HyperType.interval(true))).isEqualTo(String.class);
        assertThat(HyperTypes.toJavaClass(HyperType.json(true))).isEqualTo(String.class);
    }

    @Test
    void precisionForFixedTypes() {
        assertThat(HyperTypes.getPrecision(HyperType.bool(true))).isEqualTo(1);
        assertThat(HyperTypes.getPrecision(HyperType.int8(true))).isEqualTo(3);
        assertThat(HyperTypes.getPrecision(HyperType.int16(true))).isEqualTo(5);
        assertThat(HyperTypes.getPrecision(HyperType.int32(true))).isEqualTo(10);
        assertThat(HyperTypes.getPrecision(HyperType.int64(true))).isEqualTo(19);
        assertThat(HyperTypes.getPrecision(HyperType.oid(true))).isEqualTo(10);
        assertThat(HyperTypes.getPrecision(HyperType.float4(true))).isEqualTo(8);
        assertThat(HyperTypes.getPrecision(HyperType.float8(true))).isEqualTo(17);
        assertThat(HyperTypes.getPrecision(HyperType.date(true))).isEqualTo(13);
        assertThat(HyperTypes.getPrecision(HyperType.time(true))).isEqualTo(15);
        assertThat(HyperTypes.getPrecision(HyperType.timestamp(true))).isEqualTo(29);
        assertThat(HyperTypes.getPrecision(HyperType.timestampTz(true))).isEqualTo(35);
    }

    @Test
    void precisionForLengthyTypes() {
        assertThat(HyperTypes.getPrecision(HyperType.varchar(255, true))).isEqualTo(255);
        assertThat(HyperTypes.getPrecision(HyperType.fixedChar(10, true))).isEqualTo(10);
        assertThat(HyperTypes.getPrecision(HyperType.decimal(18, 2, true))).isEqualTo(18);
        assertThat(HyperTypes.getPrecision(HyperType.array(HyperType.int32(true), true)))
                .isEqualTo(10);
    }

    @Test
    void precisionForNullIsZero() {
        assertThat(HyperTypes.getPrecision(HyperType.nullType())).isEqualTo(0);
    }

    @Test
    void scaleForNumeric() {
        assertThat(HyperTypes.getScale(HyperType.decimal(18, 2, true))).isEqualTo(2);
        assertThat(HyperTypes.getScale(HyperType.decimal(18, 0, true))).isEqualTo(0);
    }

    @Test
    void scaleForIntegerIsZero() {
        assertThat(HyperTypes.getScale(HyperType.int32(true))).isEqualTo(0);
        assertThat(HyperTypes.getScale(HyperType.int64(true))).isEqualTo(0);
    }

    @Test
    void scaleForTimeTypesIsSixMicroseconds() {
        assertThat(HyperTypes.getScale(HyperType.time(true))).isEqualTo(6);
        assertThat(HyperTypes.getScale(HyperType.timestamp(true))).isEqualTo(6);
        assertThat(HyperTypes.getScale(HyperType.timestampTz(true))).isEqualTo(6);
    }

    @Test
    void displaySizeIntegerIncludesSign() {
        assertThat(HyperTypes.getDisplaySize(HyperType.int32(true))).isEqualTo(11);
        assertThat(HyperTypes.getDisplaySize(HyperType.int64(true))).isEqualTo(20);
    }

    @Test
    void displaySizeDecimalWithScaleAddsPointAndSign() {
        assertThat(HyperTypes.getDisplaySize(HyperType.decimal(10, 2, true))).isEqualTo(12);
        assertThat(HyperTypes.getDisplaySize(HyperType.decimal(10, 0, true))).isEqualTo(11);
    }

    @Test
    void isSignedForNumerics() {
        assertThat(HyperTypes.isSigned(HyperType.int8(true))).isTrue();
        assertThat(HyperTypes.isSigned(HyperType.int32(true))).isTrue();
        assertThat(HyperTypes.isSigned(HyperType.float8(true))).isTrue();
        assertThat(HyperTypes.isSigned(HyperType.decimal(10, 2, true))).isTrue();
    }

    @Test
    void isSignedForNonNumerics() {
        assertThat(HyperTypes.isSigned(HyperType.bool(true))).isFalse();
        assertThat(HyperTypes.isSigned(HyperType.varcharUnlimited(true))).isFalse();
        assertThat(HyperTypes.isSigned(HyperType.date(true))).isFalse();
    }

    @Test
    void isCaseSensitiveForStringAndVarbinary() {
        assertThat(HyperTypes.isCaseSensitive(HyperType.fixedChar(10, true))).isTrue();
        assertThat(HyperTypes.isCaseSensitive(HyperType.varcharUnlimited(true))).isTrue();
        assertThat(HyperTypes.isCaseSensitive(HyperType.varbinary(true))).isTrue();
        assertThat(HyperTypes.isCaseSensitive(HyperType.int32(true))).isFalse();
        assertThat(HyperTypes.isCaseSensitive(HyperType.binary(10, true))).isFalse();
    }

    @Test
    void needsDecimalDigits() {
        assertThat(HyperTypes.needsDecimalDigits(HyperType.decimal(10, 2, true)))
                .isTrue();
        // Binary floating-point types have no meaningful decimal scale.
        assertThat(HyperTypes.needsDecimalDigits(HyperType.float4(true))).isFalse();
        assertThat(HyperTypes.needsDecimalDigits(HyperType.float8(true))).isFalse();
        assertThat(HyperTypes.needsDecimalDigits(HyperType.int32(true))).isFalse();
        assertThat(HyperTypes.needsDecimalDigits(HyperType.varcharUnlimited(true)))
                .isFalse();
    }

    @Test
    void needsCharOctetLength() {
        assertThat(HyperTypes.needsCharOctetLength(HyperType.fixedChar(10, true)))
                .isTrue();
        assertThat(HyperTypes.needsCharOctetLength(HyperType.varcharUnlimited(true)))
                .isTrue();
        assertThat(HyperTypes.needsCharOctetLength(HyperType.int32(true))).isFalse();
        assertThat(HyperTypes.needsCharOctetLength(HyperType.varbinary(true))).isFalse();
    }
}
