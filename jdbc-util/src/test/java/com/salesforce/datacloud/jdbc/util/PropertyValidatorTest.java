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
}
