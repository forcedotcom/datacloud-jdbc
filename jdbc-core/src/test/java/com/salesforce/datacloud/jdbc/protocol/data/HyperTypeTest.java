/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class HyperTypeTest {

    @Test
    void boolFactoryProducesBool() {
        HyperType t = HyperType.bool(true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.BOOL);
        assertThat(t.isNullable()).isTrue();
        assertThat(t.getPrecision()).isEqualTo(0);
        assertThat(t.getScale()).isEqualTo(0);
        assertThat(t.getElement()).isNull();
    }

    @Test
    void int32FactoryProducesInt32() {
        HyperType t = HyperType.int32(false);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.INT32);
        assertThat(t.isNullable()).isFalse();
    }

    @Test
    void varcharWithLengthProducesBoundedVarchar() {
        HyperType t = HyperType.varchar(255, true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.VARCHAR);
        assertThat(t.getPrecision()).isEqualTo(255);
    }

    @Test
    void varcharUnlimitedUsesSentinel() {
        HyperType t = HyperType.varcharUnlimited(true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.VARCHAR);
        assertThat(t.getPrecision()).isEqualTo(HyperType.UNLIMITED_LENGTH);
    }

    @Test
    void decimalStoresPrecisionAndScale() {
        HyperType t = HyperType.decimal(18, 2, true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.DECIMAL);
        assertThat(t.getPrecision()).isEqualTo(18);
        assertThat(t.getScale()).isEqualTo(2);
    }

    @Test
    void arrayStoresElementType() {
        HyperType element = HyperType.int32(true);
        HyperType t = HyperType.array(element, true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.ARRAY);
        assertThat(t.getElement()).isEqualTo(element);
    }

    @Test
    void arrayWithNullElementRejected() {
        assertThatThrownBy(() -> HyperType.array(null, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void intervalFactoryProducesInterval() {
        HyperType t = HyperType.interval(true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.INTERVAL);
    }

    @Test
    void jsonFactoryProducesJson() {
        HyperType t = HyperType.json(true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.JSON);
    }

    @Test
    void fixedCharStoresLength() {
        HyperType t = HyperType.fixedChar(1, true);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.CHAR);
        assertThat(t.getPrecision()).isEqualTo(1);
    }

    @Test
    void oidFactoryProducesOid() {
        HyperType t = HyperType.oid(false);
        assertThat(t.getKind()).isEqualTo(HyperTypeKind.OID);
    }

    @Test
    void acceptDispatchesOnKind() {
        HyperTypeVisitor<String> visitor = new HyperTypeVisitor<String>() {
            @Override
            public String visitBool(HyperType t) {
                return "bool";
            }

            @Override
            public String visitInt8(HyperType t) {
                return "int8";
            }

            @Override
            public String visitInt16(HyperType t) {
                return "int16";
            }

            @Override
            public String visitInt32(HyperType t) {
                return "int32";
            }

            @Override
            public String visitInt64(HyperType t) {
                return "int64";
            }

            @Override
            public String visitOid(HyperType t) {
                return "oid";
            }

            @Override
            public String visitFloat4(HyperType t) {
                return "float4";
            }

            @Override
            public String visitFloat8(HyperType t) {
                return "float8";
            }

            @Override
            public String visitDecimal(HyperType t) {
                return "decimal";
            }

            @Override
            public String visitChar(HyperType t) {
                return "char";
            }

            @Override
            public String visitVarchar(HyperType t) {
                return "varchar";
            }

            @Override
            public String visitBinary(HyperType t) {
                return "binary";
            }

            @Override
            public String visitVarbinary(HyperType t) {
                return "varbinary";
            }

            @Override
            public String visitDate(HyperType t) {
                return "date";
            }

            @Override
            public String visitTime(HyperType t) {
                return "time";
            }

            @Override
            public String visitTimeTz(HyperType t) {
                return "timetz";
            }

            @Override
            public String visitTimestamp(HyperType t) {
                return "timestamp";
            }

            @Override
            public String visitTimestampTz(HyperType t) {
                return "timestamptz";
            }

            @Override
            public String visitArray(HyperType t) {
                return "array";
            }

            @Override
            public String visitNull(HyperType t) {
                return "null";
            }

            @Override
            public String visitInterval(HyperType t) {
                return "interval";
            }

            @Override
            public String visitJson(HyperType t) {
                return "json";
            }

            @Override
            public String visitUnknown(HyperType t) {
                return "unknown";
            }
        };

        assertThat(HyperType.bool(true).accept(visitor)).isEqualTo("bool");
        assertThat(HyperType.int32(true).accept(visitor)).isEqualTo("int32");
        assertThat(HyperType.decimal(10, 2, true).accept(visitor)).isEqualTo("decimal");
        assertThat(HyperType.varcharUnlimited(true).accept(visitor)).isEqualTo("varchar");
        assertThat(HyperType.array(HyperType.int32(true), true).accept(visitor)).isEqualTo("array");
        assertThat(HyperType.interval(true).accept(visitor)).isEqualTo("interval");
        assertThat(HyperType.json(true).accept(visitor)).isEqualTo("json");
        assertThat(HyperType.unknown("aclitem", true).accept(visitor)).isEqualTo("unknown");
    }

    @Test
    void equalityIsValueBased() {
        assertThat(HyperType.int32(true)).isEqualTo(HyperType.int32(true));
        assertThat(HyperType.int32(true)).isNotEqualTo(HyperType.int32(false));
        assertThat(HyperType.decimal(10, 2, true)).isEqualTo(HyperType.decimal(10, 2, true));
        assertThat(HyperType.decimal(10, 2, true)).isNotEqualTo(HyperType.decimal(10, 3, true));
    }

    @Test
    void toHyperTypeNamePreservesNativeForm() {
        assertThat(HyperType.bool(true).toHyperTypeName()).isEqualTo("boolean");
        assertThat(HyperType.int16(true).toHyperTypeName()).isEqualTo("smallint");
        assertThat(HyperType.int64(true).toHyperTypeName()).isEqualTo("bigint");
        assertThat(HyperType.oid(true).toHyperTypeName()).isEqualTo("oid");
        assertThat(HyperType.float8(true).toHyperTypeName()).isEqualTo("double precision");
        assertThat(HyperType.decimal(18, 2, true).toHyperTypeName()).isEqualTo("numeric(18,2)");
        assertThat(HyperType.decimal(0, 0, true).toHyperTypeName()).isEqualTo("numeric");
        assertThat(HyperType.fixedChar(1, true).toHyperTypeName()).isEqualTo("character(1)");
        assertThat(HyperType.varchar(255, true).toHyperTypeName()).isEqualTo("character varying(255)");
        assertThat(HyperType.varcharUnlimited(true).toHyperTypeName()).isEqualTo("character varying");
        assertThat(HyperType.varbinary(true).toHyperTypeName()).isEqualTo("bytea");
        assertThat(HyperType.array(HyperType.int32(true), true).toHyperTypeName())
                .isEqualTo("array(integer)");
        assertThat(HyperType.interval(true).toHyperTypeName()).isEqualTo("interval");
        assertThat(HyperType.json(true).toHyperTypeName()).isEqualTo("json");
        assertThat(HyperType.unknown("aclitem", true).toHyperTypeName()).isEqualTo("aclitem");
    }
}
