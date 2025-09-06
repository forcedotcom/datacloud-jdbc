/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class JdbcURLTest {

    @Test
    void testValidUrlWithoutParameters() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://localhost:8080");

        assertThat(jdbcUrl.getHost()).isEqualTo("localhost");
        assertThat(jdbcUrl.getPort()).isEqualTo(8080);
        assertThat(jdbcUrl.getUrlWithoutQuery()).isEqualTo("jdbc:salesforce-datacloud://localhost:8080");
    }

    @Test
    void testValidUrlWithDefaultPort() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://localhost");

        assertThat(jdbcUrl.getHost()).isEqualTo("localhost");
        assertThat(jdbcUrl.getPort()).isEqualTo(-1);
        assertThat(jdbcUrl.getUrlWithoutQuery()).isEqualTo("jdbc:salesforce-datacloud://localhost");
    }

    @Test
    void testValidUrlWithParameters() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://example.com:9090?user=test&password=secret");

        assertThat(jdbcUrl.getHost()).isEqualTo("example.com");
        assertThat(jdbcUrl.getPort()).isEqualTo(9090);
        assertThat(jdbcUrl.getUrlWithoutQuery()).isEqualTo("jdbc:salesforce-datacloud://example.com:9090");
        assertThat(jdbcUrl.getParameters())
                .containsEntry("user", "test")
                .containsEntry("password", "secret")
                .hasSize(2);
    }

    @Test
    void testUrlWithEmptyParameters() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getUrlWithoutQuery()).isEqualTo("jdbc:salesforce-datacloud://host:1234");
    }

    @Test
    void testUrlWithUrlEncodedParameters() throws DataCloudJDBCException {
        JdbcURL jdbcUrl =
                JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=test%40example.com&password=secret%21");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getParameters())
                .containsEntry("user", "test@example.com")
                .containsEntry("password", "secret!")
                .hasSize(2);
    }

    @Test
    void testUrlWithEmptyParameterValue() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=&password=secret");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getParameters())
                .containsEntry("user", "")
                .containsEntry("password", "secret")
                .hasSize(2);
    }

    @Test
    void testUrlWithParameterWithoutValue() {
        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class,
                () -> JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user&password=secret"));

        assertThat(exception.getMessage()).contains("Parameter without value in JDBC URL: user");
        assertThat(exception.getSQLState()).isEqualTo("HY000");
    }

    @Test
    void testUrlWithEmptyPairs() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=test&&password=secret");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getParameters())
                .containsEntry("user", "test")
                .containsEntry("password", "secret")
                .hasSize(2);
    }

    @Test
    void testUrlWithMultipleEqualsInValue() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?config=key=value&other=test");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getParameters())
                .containsEntry("config", "key=value")
                .containsEntry("other", "test")
                .hasSize(2);
    }

    @Test
    void testAddParametersToProperties() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=admin&password=secret&timeout=30");
        Properties properties = new Properties();

        jdbcUrl.addParametersToProperties(properties);

        assertThat(properties.getProperty("user")).isEqualTo("admin");
        assertThat(properties.getProperty("password")).isEqualTo("secret");
        assertThat(properties.getProperty("timeout")).isEqualTo("30");
    }

    @Test
    void testAddParametersToPropertiesWithExistingProperties() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=admin&password=secret");
        Properties properties = new Properties();
        properties.setProperty("timeout", "60");

        jdbcUrl.addParametersToProperties(properties);

        assertThat(properties.getProperty("user")).isEqualTo("admin");
        assertThat(properties.getProperty("password")).isEqualTo("secret");
        assertThat(properties.getProperty("timeout")).isEqualTo("60");
    }

    @Test
    void testAddParametersToPropertiesWithConflict() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=admin&password=secret");
        Properties properties = new Properties();
        properties.setProperty("user", "existing");

        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> jdbcUrl.addParametersToProperties(properties));

        assertThat(exception.getMessage()).contains("Parameter `user` is set both in the URL and the properties");
        assertThat(exception.getSQLState()).isEqualTo("HY000");
    }

    @Test
    void testInvalidUrlMissingJdbcPrefix() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> JdbcURL.of("salesforce-datacloud://host:1234"));

        assertThat(exception.getMessage()).contains("All JDBC URLs must start with 'jdbc:'");
    }

    @Test
    void testInvalidUrlWithPath() {
        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://host:1234/path"));

        assertThat(exception.getMessage()).contains("JDBC URLs must not contain a path");
    }

    @Test
    void testInvalidUrlWithEmptyPath() {
        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://host:1234/"));

        assertThat(exception.getMessage()).contains("JDBC URLs must not contain a path");
    }

    @Test
    void testInvalidUrlWithFragment() {
        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://host:1234#fragment"));

        assertThat(exception.getMessage()).contains("JDBC URLs must not contain a fragment");
    }

    @Test
    void testInvalidUrlWithUserInfo() {
        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://user:pass@host:1234"));

        assertThat(exception.getMessage()).contains("JDBC URLs must not contain a user info");
    }

    @Test
    void testInvalidUrlWithMalformedUri() {
        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://[invalid"));

        assertThat(exception.getMessage()).contains("Invalid URI syntax");
    }

    @Test
    void testDuplicateParameterKey() {
        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class,
                () -> JdbcURL.of("jdbc:salesforce-datacloud://host:1234?user=admin&user=guest"));

        assertThat(exception.getMessage()).contains("Duplicate parameter key in JDBC URL: user");
        assertThat(exception.getSQLState()).isEqualTo("HY000");
    }

    @Test
    void testUrlWithSpecialCharactersInParameters() throws DataCloudJDBCException {
        JdbcURL jdbcUrl =
                JdbcURL.of("jdbc:salesforce-datacloud://host:1234?query=SELECT%20*%20FROM%20table&name=test%2Buser");

        assertThat(jdbcUrl.getHost()).isEqualTo("host");
        assertThat(jdbcUrl.getPort()).isEqualTo(1234);
        assertThat(jdbcUrl.getParameters())
                .containsEntry("query", "SELECT * FROM table")
                .containsEntry("name", "test user")
                .hasSize(2);
    }

    @Test
    void testUrlWithEmptyHost() {
        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> JdbcURL.of("jdbc:salesforce-datacloud://:8080"));

        assertThat(exception.getMessage()).contains("JDBC URLs must contain a host");
    }

    @Test
    void testUrlWithIpv6Host() throws DataCloudJDBCException {
        JdbcURL jdbcUrl = JdbcURL.of("jdbc:salesforce-datacloud://[::1]:8080");

        assertThat(jdbcUrl.getHost()).isEqualTo("[::1]");
        assertThat(jdbcUrl.getPort()).isEqualTo(8080);
    }
}
