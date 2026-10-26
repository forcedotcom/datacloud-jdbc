/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs each Data 360 SQL data type through a real query and checks the JDBC type, type name and
 * Java class reported by {@link ResultSetMetaData} as well as the value returned by
 * {@link ResultSet#getObject(int)}. Mirrors the "Data type mapping" table in the README.
 *
 * <p>Requires test.connection.orgDomainUrl, clientId and clientSecret (client credentials flow).
 */
@Tag("integration")
class DataTypeMappingIntegrationTest {

    private static Connection conn;

    @BeforeAll
    static void connect() throws Exception {
        String orgDomainUrl = System.getProperty("test.connection.orgDomainUrl", "");
        String clientId = System.getProperty("test.connection.clientId", "");
        String clientSecret = System.getProperty("test.connection.clientSecret", "");
        if (orgDomainUrl.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new AssertionError("Requires test.connection.orgDomainUrl, clientId, clientSecret");
        }
        Properties props = new Properties();
        props.setProperty("dataspace", "default");
        props.setProperty("clientId", clientId);
        props.setProperty("clientSecret", clientSecret);
        String url = orgDomainUrl.startsWith("jdbc:")
                ? orgDomainUrl
                : "jdbc:salesforce-datacloud://" + orgDomainUrl.replaceFirst("^https?://", "");
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
        conn = DriverManager.getConnection(url, props);
    }

    @AfterAll
    static void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    static Stream<Arguments> types() {
        // expression, JDBC type code name, JDBC type name, Java class name, expected getObject().toString()
        return Stream.of(
                Arguments.of("TRUE::boolean", "BOOLEAN", "BOOLEAN", "java.lang.Boolean", "true"),
                Arguments.of("1::smallint", "SMALLINT", "SMALLINT", "java.lang.Integer", "1"),
                Arguments.of("2::integer", "INTEGER", "INTEGER", "java.lang.Integer", "2"),
                Arguments.of("3::bigint", "BIGINT", "BIGINT", "java.lang.Long", "3"),
                Arguments.of("1.5::real", "REAL", "REAL", "java.lang.Float", "1.5"),
                Arguments.of("2.5::double precision", "DOUBLE", "DOUBLE", "java.lang.Double", "2.5"),
                Arguments.of("12.34::numeric(18,2)", "DECIMAL", "DECIMAL", "java.math.BigDecimal", "12.34"),
                Arguments.of("'ab'::char(5)", "CHAR", "CHAR", "java.lang.String", "ab   "),
                Arguments.of("'ab'::varchar(10)", "VARCHAR", "VARCHAR", "java.lang.String", "ab"),
                Arguments.of("'ab'::text", "VARCHAR", "VARCHAR", "java.lang.String", "ab"),
                Arguments.of("'\\x0102'::bytea", "VARBINARY", "VARBINARY", "[B", null),
                Arguments.of("DATE '2024-01-02'", "DATE", "DATE", "java.sql.Date", "2024-01-02"),
                Arguments.of("TIME '01:02:03.123456'", "TIME", "TIME", "java.sql.Time", null),
                Arguments.of(
                        "TIMESTAMP '2024-01-02 03:04:05.123456'", "TIMESTAMP", "TIMESTAMP", "java.sql.Timestamp", null),
                Arguments.of(
                        "TIMESTAMPTZ '2024-01-02 03:04:05+00'",
                        "TIMESTAMP_WITH_TIMEZONE",
                        "TIMESTAMP_WITH_TIMEZONE",
                        "java.sql.Timestamp",
                        null),
                Arguments.of("ARRAY[1,2,3]", "ARRAY", "ARRAY", "java.sql.Array", null),
                Arguments.of("'{\"a\":1}'::json", "VARCHAR", "VARCHAR", "java.lang.String", "{\"a\":1}"));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("types")
    void sqlTypeMapsToJdbcType(
            String expression, String jdbcType, String typeName, String javaClass, String expectedValue)
            throws Exception {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT " + expression + " AS c")) {
            ResultSetMetaData md = rs.getMetaData();
            assertThat(java.sql.JDBCType.valueOf(md.getColumnType(1)).getName()).isEqualTo(jdbcType);
            assertThat(md.getColumnTypeName(1)).isEqualTo(typeName);
            assertThat(md.getColumnClassName(1)).isEqualTo(javaClass);

            assertThat(rs.next()).isTrue();
            Object value = rs.getObject(1);
            assertThat(value).isNotNull();
            if (expectedValue != null) {
                assertThat(value.toString()).isEqualTo(expectedValue);
            }
        }
    }
}
