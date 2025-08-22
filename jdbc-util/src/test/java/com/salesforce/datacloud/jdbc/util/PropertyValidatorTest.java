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
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertyValidatorTest {

    @Test
    void validate_allowsKnownKeys() {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("userName", "alice");
        props.setProperty("workload", "jdbcv3");
        props.setProperty("queryTimeout", "30");

        assertThatCode(() -> PropertyValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void validate_allowsKnownPrefixes() {
        Properties props = new Properties();
        props.setProperty("querySetting.lc_time", "en_us");
        props.setProperty("grpc.keepalive_time_ms", "120000");

        assertThatCode(() -> PropertyValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void validate_aggregatesUnknownKeys() {
        Properties props = new Properties();
        props.setProperty("foo", "1");
        props.setProperty("bar", "2");

        assertThatThrownBy(() -> PropertyValidator.validate(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Unknown JDBC properties")
                .hasMessageContaining("foo")
                .hasMessageContaining("bar");
    }

    @Test
    void validate_nullOrEmpty_noop() {
        assertThatCode(() -> PropertyValidator.validate(null)).doesNotThrowAnyException();

        Properties empty = new Properties();
        assertThatCode(() -> PropertyValidator.validate(empty)).doesNotThrowAnyException();
    }

    @Test
    void validateQuerySettings_unprefixed_time_zone_raises() {
        Properties props = new Properties();
        props.setProperty("time_zone", "UTC");

        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void validateQuerySettings_unprefixed_timezone_alias_raises() {
        Properties props = new Properties();
        props.setProperty("timezone", "UTC");

        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void validateQuerySettings_unprefixed_time_dash_zone_raises() {
        Properties props = new Properties();
        props.setProperty("time-zone", "UTC");

        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void validateQuerySettings_unprefixed_lc_time_raises() {
        Properties props = new Properties();
        props.setProperty("lc_time", "en_us");

        assertThatThrownBy(() -> PropertyValidator.validateCommonHyperSettings(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.lc_time'");
    }

    @Test
    void validateQuerySettings_prefixed_ok() {
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "UTC");
        props.setProperty("querySetting.lc_time", "en_us");

        assertThatCode(() -> PropertyValidator.validateCommonHyperSettings(props))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_propagates_unprefixed_time_zone_error() {
        Properties props = new Properties();
        props.setProperty("time_zone", "UTC");

        assertThatThrownBy(() -> PropertyValidator.validate(props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Use 'querySetting.time_zone'");
    }

    @Test
    void canonicalize_null_returnsEmptyString() throws Exception {
        // Use reflection to cover the null branch in canonicalizeSettingName
        java.lang.reflect.Method m = PropertyValidator.class.getDeclaredMethod("canonicalizeSettingName", String.class);
        m.setAccessible(true);
        String result = (String) m.invoke(null, new Object[] {null});
        org.assertj.core.api.Assertions.assertThat(result).isEqualTo("");
    }
}
