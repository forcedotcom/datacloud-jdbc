/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.time.Duration;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;

class StatementPropertiesTest {
    @Test
    void parsesAllProperties() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("queryTimeout", "10");
        props.setProperty("queryTimeoutLocalEnforcementDelay", "7");
        props.setProperty("querySetting.setting1", "value1");
        props.setProperty("querySetting.setting2", "value2");
        props.setProperty("querySetting.setting3", "value3");
        Properties originalProps = (Properties) props.clone();
        val sp = StatementProperties.ofDestructive(props);

        // Check the parsed properties
        assertThat(sp.getQueryTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(sp.getQueryTimeoutLocalEnforcementDelay()).isEqualTo(Duration.ofSeconds(7));
        assertThat(sp.getQuerySettings()).hasSize(3);
        assertThat(sp.getQuerySettings()).containsEntry("setting1", "value1");
        assertThat(sp.getQuerySettings()).containsEntry("setting2", "value2");
        assertThat(sp.getQuerySettings()).containsEntry("setting3", "value3");

        // Check the roundtrip
        assertThat(sp.toProperties()).isEqualTo(originalProps);
    }

    @Test
    void querySetting_query_timeout_isRejected() {
        Properties props = new Properties();
        props.setProperty("querySetting.query_timeout", "5s");
        assertThatThrownBy(() -> StatementProperties.ofDestructive(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("`query_timeout` is not an allowed `querySetting` subkey")
                .hasMessageContaining("use the `queryTimeout` property instead");
    }

    @Test
    void parses_queryTimeoutLocalEnforcementDelay_seconds() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("queryTimeoutLocalEnforcementDelay", "7");
        val sp = StatementProperties.ofDestructive(props);
        assertThat(sp.getQueryTimeoutLocalEnforcementDelay()).isEqualTo(Duration.ofSeconds(7));
    }
}
