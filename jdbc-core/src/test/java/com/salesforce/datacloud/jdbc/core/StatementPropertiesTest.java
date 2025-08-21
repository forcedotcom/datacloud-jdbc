/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        assertThatThrownBy(() -> StatementProperties.of(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void unprefixed_lc_time_raisesUserError() {
        Properties props = new Properties();
        props.setProperty("lc_time", "en_us");
        assertThatThrownBy(() -> StatementProperties.of(props))
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
