/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.types;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.PgCatalogTypeParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Pins the Data 360 SQL data type to JDBC type mapping documented in the README
 * ("Data type mapping") against the types listed in the Data 360 SQL reference.
 */
class SqlDatatypeMappingTest {

    @ParameterizedTest(name = "{0} -> {1} ({2})")
    @CsvSource(
            delimiter = '|',
            value = {
                "bigint | BIGINT | java.lang.Long",
                "int8 | BIGINT | java.lang.Long",
                "boolean | BOOLEAN | java.lang.Boolean",
                "bool | BOOLEAN | java.lang.Boolean",
                "bytea | VARBINARY | [B",
                "character(10) | CHAR | java.lang.String",
                "char(10) | CHAR | java.lang.String",
                "character varying(255) | VARCHAR | java.lang.String",
                "varchar(255) | VARCHAR | java.lang.String",
                "date | DATE | java.sql.Date",
                "real | REAL | java.lang.Float",
                "float4 | REAL | java.lang.Float",
                "double precision | DOUBLE | java.lang.Double",
                "float8 | DOUBLE | java.lang.Double",
                "float | DOUBLE | java.lang.Double",
                "integer | INTEGER | java.lang.Integer",
                "int | INTEGER | java.lang.Integer",
                "int4 | INTEGER | java.lang.Integer",
                "numeric(18,2) | DECIMAL | java.math.BigDecimal",
                "smallint | SMALLINT | java.lang.Integer",
                "int2 | SMALLINT | java.lang.Integer",
                "text | VARCHAR | java.lang.String",
                "time | TIME | java.sql.Time",
                "time without time zone | TIME | java.sql.Time",
                "timestamp | TIMESTAMP | java.sql.Timestamp",
                "timestamp without time zone | TIMESTAMP | java.sql.Timestamp",
                "timestamp with time zone | TIMESTAMP_WITH_TIMEZONE | java.sql.Timestamp",
                "timestamptz | TIMESTAMP_WITH_TIMEZONE | java.sql.Timestamp",
                "array(integer) | ARRAY | java.sql.Array",
            })
    void documentedSqlTypeMapsToJdbcType(String sqlType, String jdbcType, String javaClass) {
        HyperType type = PgCatalogTypeParser.parse(sqlType.trim(), true);

        assertThat(HyperTypes.toJdbcType(type).getName()).isEqualTo(jdbcType);
        assertThat(HyperTypes.toJavaClass(type).getName()).isEqualTo(javaClass);
    }
}
