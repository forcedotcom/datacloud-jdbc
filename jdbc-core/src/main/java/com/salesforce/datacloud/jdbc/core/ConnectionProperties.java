/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;

import java.sql.SQLException;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * Connection properties that control the JDBC connection behavior.
 */
@Getter
@Builder
public class ConnectionProperties {
    /**
     * The workload to use for the connection (default: jdbcv3)
     */
    @Builder.Default
    private final String workload = "jdbcv3";

    /**
     * The external client context to use for the connection
     */
    @Builder.Default
    private final String externalClientContext = "";

    /**
     * Statement properties associated with this connection
     */
    @Builder.Default
    private final StatementProperties statementProperties =
            StatementProperties.builder().build();

    public static ConnectionProperties defaultProperties() {
        return builder().build();
    }

    /**
     * Parses connection properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A ConnectionProperties instance
     * @throws SQLException if parsing of property values fails
     */
    public static ConnectionProperties ofDestructive(Properties props) throws SQLException {
        ConnectionPropertiesBuilder builder = ConnectionProperties.builder();

        takeOptional(props, "workload").ifPresent(builder::workload);
        takeOptional(props, "externalClientContext").ifPresent(builder::externalClientContext);
        builder.statementProperties(StatementProperties.ofDestructive(props));

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the connection properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (!workload.isEmpty()) {
            props.setProperty("workload", workload);
        }
        if (!externalClientContext.isEmpty()) {
            props.setProperty("externalClientContext", externalClientContext);
        }
        props.putAll(statementProperties.toProperties());

        return props;
    }
}
