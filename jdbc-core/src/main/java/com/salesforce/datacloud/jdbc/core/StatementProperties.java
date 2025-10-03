/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalDuration;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

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
    /**
     * The amount of time that the local driver waits in additional to the query timeout for the server-side
     * cancellation. This is used to account for network latency and ensure the server has time to produce and transmit
     * the (more helpful server-side) error message. The 5 seconds were chosen based off a guess to also accommodate
     * very slow public internet connections. A zero duration is interpreted as zero.
     */
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
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A StatementProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static StatementProperties ofDestructive(Properties props) throws DataCloudJDBCException {
        StatementPropertiesBuilder builder = StatementProperties.builder();

        takeOptionalDuration(props, "queryTimeout").ifPresent(builder::queryTimeout);
        takeOptionalDuration(props, "queryTimeoutLocalEnforcementDelay")
                .ifPresent(builder::queryTimeoutLocalEnforcementDelay);

        // Parse querySetting.* properties
        Map<String, String> querySettings = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("querySetting.")) {
                String settingKey = key.substring("querySetting.".length());
                String settingValue = takeRequired(props, key);
                // We want `query_timeout` to be set as a JDBC-side setting, so
                // we can also enforce it on the network level.
                if ("query_timeout".equalsIgnoreCase(settingKey)) {
                    throw new DataCloudJDBCException(
                            "`query_timeout` is not an allowed `querySetting` subkey, use the `queryTimeout` property instead");
                }
                querySettings.put(settingKey, settingValue);
            }
        }
        builder.querySettings(querySettings);

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
        if (!queryTimeoutLocalEnforcementDelay.isZero()) {
            props.setProperty(
                    "queryTimeoutLocalEnforcementDelay",
                    String.valueOf(queryTimeoutLocalEnforcementDelay.getSeconds()));
        }
        for (Map.Entry<String, String> entry : querySettings.entrySet()) {
            props.setProperty("querySetting." + entry.getKey(), entry.getValue());
        }

        return props;
    }
}
