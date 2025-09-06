/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.PropertyValidator;
import java.time.Duration;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;

class StatementPropertiesTest {

    @Test
    void querySetting_query_timeout_isRejected() {
        Properties props = new Properties();
        props.setProperty("querySetting.query_timeout", "5s");
        assertThatThrownBy(() -> StatementProperties.of(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("`query_timeout` is not an allowed `querySetting` subkey")
                .hasMessageContaining("use the `queryTimeout` property instead");
    }

    @Test
    void unprefixed_time_zone_raisesUserError() {
        Properties props = new Properties();
        props.setProperty("time_zone", "UTC");
        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void unprefixed_lc_time_raisesUserError() {
        Properties props = new Properties();
        props.setProperty("lc_time", "en_us");
        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.lc_time'");
    }

    @Test
    void parses_queryTimeoutLocalEnforcementDelay_seconds() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("queryTimeoutLocalEnforcementDelay", "7");
        val sp = StatementProperties.of(props);
        assertThat(sp.getQueryTimeoutLocalEnforcementDelay()).isEqualTo(Duration.ofSeconds(7));
    }
}
