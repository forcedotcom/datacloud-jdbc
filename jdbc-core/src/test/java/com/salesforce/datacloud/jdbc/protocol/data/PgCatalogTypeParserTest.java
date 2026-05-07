/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PgCatalogTypeParserTest {

    @Test
    void booleanDisplayName() {
        assertThat(PgCatalogTypeParser.parse("boolean", true)).isEqualTo(HyperType.bool(true));
    }

    @Test
    void booleanInternalAlias() {
        assertThat(PgCatalogTypeParser.parse("bool", false)).isEqualTo(HyperType.bool(false));
    }

    @Test
    void integerDisplayNames() {
        assertThat(PgCatalogTypeParser.parse("smallint", true)).isEqualTo(HyperType.int16(true));
        assertThat(PgCatalogTypeParser.parse("integer", true)).isEqualTo(HyperType.int32(true));
        assertThat(PgCatalogTypeParser.parse("bigint", true)).isEqualTo(HyperType.int64(true));
    }

    @Test
    void integerInternalAliases() {
        assertThat(PgCatalogTypeParser.parse("int2", true)).isEqualTo(HyperType.int16(true));
        assertThat(PgCatalogTypeParser.parse("int4", true)).isEqualTo(HyperType.int32(true));
        assertThat(PgCatalogTypeParser.parse("int8", true)).isEqualTo(HyperType.int64(true));
    }

    @Test
    void oidMapsToHyperOid() {
        assertThat(PgCatalogTypeParser.parse("oid", true)).isEqualTo(HyperType.oid(true));
    }

    @Test
    void floatingPointDisplayNames() {
        assertThat(PgCatalogTypeParser.parse("real", true)).isEqualTo(HyperType.float4(true));
        assertThat(PgCatalogTypeParser.parse("double precision", true)).isEqualTo(HyperType.float8(true));
    }

    @Test
    void floatingPointInternalAliases() {
        assertThat(PgCatalogTypeParser.parse("float4", true)).isEqualTo(HyperType.float4(true));
        assertThat(PgCatalogTypeParser.parse("float8", true)).isEqualTo(HyperType.float8(true));
        assertThat(PgCatalogTypeParser.parse("float", true)).isEqualTo(HyperType.float8(true));
    }

    @Test
    void numericWithoutPrecisionAndScale() {
        assertThat(PgCatalogTypeParser.parse("numeric", true)).isEqualTo(HyperType.decimal(0, 0, true));
    }

    @Test
    void numericWithPrecisionOnly() {
        assertThat(PgCatalogTypeParser.parse("numeric(18)", true)).isEqualTo(HyperType.decimal(18, 0, true));
    }

    @Test
    void numericWithPrecisionAndScale() {
        assertThat(PgCatalogTypeParser.parse("numeric(18,2)", true)).isEqualTo(HyperType.decimal(18, 2, true));
        assertThat(PgCatalogTypeParser.parse("numeric(18, 2)", true)).isEqualTo(HyperType.decimal(18, 2, true));
    }

    @Test
    void charParsing() {
        assertThat(PgCatalogTypeParser.parse("character(1)", true)).isEqualTo(HyperType.fixedChar(1, true));
        assertThat(PgCatalogTypeParser.parse("character(10)", true)).isEqualTo(HyperType.fixedChar(10, true));
        assertThat(PgCatalogTypeParser.parse("char(5)", true)).isEqualTo(HyperType.fixedChar(5, true));
        // Default length for unbound "char" is 1 (SQL standard).
        assertThat(PgCatalogTypeParser.parse("character", true)).isEqualTo(HyperType.fixedChar(1, true));
        assertThat(PgCatalogTypeParser.parse("bpchar(3)", true)).isEqualTo(HyperType.fixedChar(3, true));
    }

    @Test
    void varcharParsing() {
        assertThat(PgCatalogTypeParser.parse("character varying(255)", true)).isEqualTo(HyperType.varchar(255, true));
        assertThat(PgCatalogTypeParser.parse("character varying", true)).isEqualTo(HyperType.varcharUnlimited(true));
        assertThat(PgCatalogTypeParser.parse("varchar(30)", true)).isEqualTo(HyperType.varchar(30, true));
        assertThat(PgCatalogTypeParser.parse("varchar", true)).isEqualTo(HyperType.varcharUnlimited(true));
    }

    @Test
    void textMapsToUnlimitedVarchar() {
        assertThat(PgCatalogTypeParser.parse("text", true)).isEqualTo(HyperType.varcharUnlimited(true));
    }

    @Test
    void byteaMapsToVarbinary() {
        assertThat(PgCatalogTypeParser.parse("bytea", true)).isEqualTo(HyperType.varbinary(true));
    }

    @Test
    void dateTimeTypes() {
        assertThat(PgCatalogTypeParser.parse("date", true)).isEqualTo(HyperType.date(true));
        assertThat(PgCatalogTypeParser.parse("time", true)).isEqualTo(HyperType.time(true));
        assertThat(PgCatalogTypeParser.parse("time without time zone", true)).isEqualTo(HyperType.time(true));
        assertThat(PgCatalogTypeParser.parse("time with time zone", true)).isEqualTo(HyperType.timeTz(true));
        assertThat(PgCatalogTypeParser.parse("timetz", true)).isEqualTo(HyperType.timeTz(true));
        assertThat(PgCatalogTypeParser.parse("timestamp", true)).isEqualTo(HyperType.timestamp(true));
        assertThat(PgCatalogTypeParser.parse("timestamp without time zone", true))
                .isEqualTo(HyperType.timestamp(true));
        assertThat(PgCatalogTypeParser.parse("timestamp with time zone", true)).isEqualTo(HyperType.timestampTz(true));
        assertThat(PgCatalogTypeParser.parse("timestamptz", true)).isEqualTo(HyperType.timestampTz(true));
    }

    @Test
    void arrayOfInteger() {
        assertThat(PgCatalogTypeParser.parse("array(integer)", true))
                .isEqualTo(HyperType.array(HyperType.int32(true), true));
    }

    @Test
    void arrayOfText() {
        assertThat(PgCatalogTypeParser.parse("array(text)", true))
                .isEqualTo(HyperType.array(HyperType.varcharUnlimited(true), true));
    }

    @Test
    void arrayOfNumeric() {
        assertThat(PgCatalogTypeParser.parse("array(numeric(18,2))", true))
                .isEqualTo(HyperType.array(HyperType.decimal(18, 2, true), true));
    }

    @Test
    void intervalMapsToFirstClassKind() {
        assertThat(PgCatalogTypeParser.parse("interval", true)).isEqualTo(HyperType.interval(true));
    }

    @Test
    void jsonMapsToFirstClassKind() {
        assertThat(PgCatalogTypeParser.parse("json", true)).isEqualTo(HyperType.json(true));
    }

    @Test
    void unknownTypeThrows() {
        assertThatThrownBy(() -> PgCatalogTypeParser.parse("geography", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("geography");
    }

    @Test
    void nullInputThrows() {
        assertThatThrownBy(() -> PgCatalogTypeParser.parse(null, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyInputThrows() {
        assertThatThrownBy(() -> PgCatalogTypeParser.parse("", true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void caseInsensitiveMatching() {
        assertThat(PgCatalogTypeParser.parse("INTEGER", true)).isEqualTo(HyperType.int32(true));
        assertThat(PgCatalogTypeParser.parse("Character Varying(10)", true)).isEqualTo(HyperType.varchar(10, true));
    }
}
