/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.SQLException;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class PropertiesTest extends InterceptedHyperTestBase {
    @Test
    public void testQuerySettingPropagationToServer() throws SQLException {
        // This test case verifies that the query setting properties are propagated to the server.
        // We test two different values to verify that we don't accidentally hit the default value.
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        for (val lcTime : ImmutableList.of("en_us", "de_de")) {
            val properties = new Properties();
            properties.setProperty("querySetting.lc_time", lcTime);
            try (val connection = getHyperQueryConnection(server, properties)) {
                try (val statement = connection.createStatement()) {
                    val result = statement.executeQuery("SHOW lc_time");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo(lcTime);
                }
            }
        }
    }

    @Test
    public void testPropertiesErrorReportingFromServer() throws SQLException {
        // This test case verifies that the user will get an actionable error message if they submitted an invalid query
        // setting
        // This section tests behavior on invalid setting key
        val invalidSettingKeyProperty = new Properties();
        invalidSettingKeyProperty.setProperty("querySetting.invalid_setting", "invalid");
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val connection = getHyperQueryConnection(server, invalidSettingKeyProperty)) {
            val exception = assertThrows(
                    DataCloudJDBCException.class,
                    () -> connection.createStatement().executeQuery("SELECT 1"));
            assertThat(exception.getMessage()).contains("unrecognized configuration parameter 'invalid_setting'");
        }

        // This section tests behavior on valid setting key but invalid setting value
        val invalidSettingValueProperty = new Properties();
        invalidSettingValueProperty.setProperty("externalClientContext", "{invalid: json}");
        try (val connection = getHyperQueryConnection(server, invalidSettingValueProperty)) {
            val exception = assertThrows(
                    DataCloudJDBCException.class,
                    () -> connection.createStatement().executeQuery("SELECT 1"));
            assertThat(exception.getMessage()).contains("invalid JSON in `x-hyperdb-external-client-context` header");
        }
    }

    @Test
    void testQuerySettingsParsing() throws DataCloudJDBCException {
        Properties properties = new Properties();
        properties.setProperty("querySetting.lc_time", "en_us");
        // This is a normal interpreted setting and should not land in query settings
        properties.setProperty("workload", "test-workload");
        ConnectionProperties connectionProperties = ConnectionProperties.ofDestructive(properties);
        // Verify that user name is an interpeted property
        assertThat(connectionProperties.getWorkload()).isEqualTo("test-workload");
        // Verify that query settings contains `en_us`
        assertThat(connectionProperties.getStatementProperties().getQuerySettings())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("lc_time", "en_us"));
    }

    @Test
    void testInvalidSettingValue() throws DataCloudJDBCException {
        // This test case verifies that we raise the right exception when the user provides an invalid setting value
        Properties properties = new Properties();
        properties.setProperty("queryTimeout", "invalid");
        val exception = assertThrows(IllegalArgumentException.class, () -> getHyperQueryConnection(properties));
        assertThat(exception.getMessage())
                .contains("Failed to parse `queryTimeout` property as an integer: For input string: \"invalid\"");
    }

    @Test
    void testUnprefixedTimezoneRaisesUserError() {
        // Users sometimes set time_zone directly without the required querySetting. prefix
        // Only time_zone should trigger the targeted hint here.
        Properties properties = new Properties();
        properties.setProperty("time_zone", "Asia/Tokyo");
        val exception = assertThrows(DataCloudJDBCException.class, () -> getHyperQueryConnection(properties));
        assertThat(exception.getMessage()).contains("Use 'querySetting.time_zone'");
    }

    @Test
    void testUnprefixedLcTimeRaisesUserError() {
        Properties properties = new Properties();
        properties.setProperty("lc_time", "en_us");
        val exception = assertThrows(DataCloudJDBCException.class, () -> getHyperQueryConnection(properties));
        assertThat(exception.getMessage()).contains("Use 'querySetting.lc_time'");
    }

    @Test
    void testUnknownTopLevelPropertyRaisesUserError() {
        Properties properties = new Properties();
        properties.setProperty("TIMEZONE", "UTC");
        // This will be caught by StatementProperties but also ensure a random key errors at connect-time
        val exception = assertThrows(DataCloudJDBCException.class, () -> getHyperQueryConnection(properties));
        // With no aliasing for TIMEZONE, this is treated as an unknown property
        assertThat(exception.getMessage()).contains("Unknown JDBC properties: TIMEZONE");
    }
}
