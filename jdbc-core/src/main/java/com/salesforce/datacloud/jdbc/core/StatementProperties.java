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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;
import lombok.With;

/**
 * Statement properties that control query execution.
 */
@Getter
@Builder
public class StatementProperties {
    /**
     * The query timeout, a zero duration is interpreted as infinite timeout
     */
    @With
    @Builder.Default
    private final Duration queryTimeout = Duration.ZERO;
    // The amount of time that the local driver waits in additional to the query timeout for the server-side
    // cancellation. This is used to account for network latency and ensure the server has time to produce and transmit
    // the (more helpful server-side) error message. The 5 seconds were chosen based off a guess to also accommodate
    // very slow public internet connections. A zero duration is interpreted as zero.
    @With
    @Builder.Default
    private final Duration queryTimeoutLocalEnforcementDelay = Duration.ofSeconds(5);

    /**
     * The query settings to use for the connection
     */
    @Builder.Default
    private final Map<String, String> querySettings = new HashMap<>();

    /**
     * Parses statement properties from a Properties object.
     *
     * @param props The properties to parse
     * @return A StatementProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static StatementProperties of(Properties props) throws DataCloudJDBCException {
        StatementPropertiesBuilder builder = StatementProperties.builder();

        // The query timeout property, zero or negative values are interpreted as infinite timeout.
        // Positive values are interpreted as the number of seconds for the timeout
        String queryTimeoutStr = props.getProperty("queryTimeout");
        if (queryTimeoutStr != null) {
            try {
                Duration timeout = Duration.ofSeconds(Integer.parseInt(queryTimeoutStr));
                if (timeout.isZero() || timeout.isNegative()) {
                    builder.queryTimeout(Duration.ZERO);
                } else {
                    builder.queryTimeout(timeout);
                }
            } catch (NumberFormatException e) {
                throw new DataCloudJDBCException("Failed to parse `queryTimeout` property: " + e.getMessage());
            }
        }

        // The query timeout local enforcement delay property
        String queryTimeoutLocalEnforcementDelayStr = props.getProperty("queryTimeoutLocalEnforcementDelay");
        if (queryTimeoutLocalEnforcementDelayStr != null) {
            try {
                Duration delay = Duration.ofSeconds(Integer.parseInt(queryTimeoutLocalEnforcementDelayStr));
                builder.queryTimeoutLocalEnforcementDelay(delay);
            } catch (NumberFormatException e) {
                throw new DataCloudJDBCException(
                        "Failed to parse `queryTimeoutLocalEnforcementDelay` property: " + e.getMessage());
            }
        }

        // Parse querySetting.* properties
        Map<String, String> querySettings = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("querySetting.")) {
                String settingKey = key.substring("querySetting.".length());
                String settingValue = props.getProperty(key);
                if ("query_timeout".equalsIgnoreCase(settingKey)) {
                    throw new DataCloudJDBCException(
                            "`query_timeout` is not an allowed `querySetting` subkey, use the `queryTimeout` property instead");
                }
                querySettings.put(settingKey, settingValue);
            }
        }
        if (!querySettings.isEmpty()) {
            builder.querySettings(querySettings);
        }

        return builder.build();
    }

    /**
     * Serializes this StatementProperties instance to a Properties object.
     *
     * @return A Properties object containing the statement properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (!queryTimeout.isZero()) {
            props.setProperty("queryTimeout", String.valueOf(queryTimeout.getSeconds()));
        }
        for (Map.Entry<String, String> entry : querySettings.entrySet()) {
            props.setProperty("querySetting." + entry.getKey(), entry.getValue());
        }

        return props;
    }
}
