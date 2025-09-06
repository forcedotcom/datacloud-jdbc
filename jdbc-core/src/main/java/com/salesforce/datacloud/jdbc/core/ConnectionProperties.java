/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
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
     * The dataspace to use for the connection
     */
    @Builder.Default
    private final String dataspace = "";

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
     * The user name to use for the connection
     */
    @Builder.Default
    private final String userName = "";

    /**
     * Statement properties associated with this connection
     */
    @Builder.Default
    private final StatementProperties statementProperties =
            StatementProperties.builder().build();

    /**
     * Parses connection properties from a Properties object.
     *
     * @param props The properties to parse
     * @return A ConnectionProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static ConnectionProperties of(Properties props) throws DataCloudJDBCException {
        ConnectionPropertiesBuilder builder = ConnectionProperties.builder();

        String dataspaceValue = props.getProperty("dataspace");
        if (dataspaceValue != null) {
            builder.dataspace(dataspaceValue);
        }
        String workloadValue = props.getProperty("workload");
        if (workloadValue != null) {
            builder.workload(workloadValue);
        }
        String externalClientContextValue = props.getProperty("external-client-context");
        if (externalClientContextValue != null) {
            builder.externalClientContext(externalClientContextValue);
        }
        String userNameValue = props.getProperty("userName");
        if (userNameValue != null) {
            builder.userName(userNameValue);
        }
        builder.statementProperties(StatementProperties.of(props));

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the connection properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (!dataspace.isEmpty()) {
            props.setProperty("dataspace", dataspace);
        }
        if (!workload.isEmpty()) {
            props.setProperty("workload", workload);
        }
        if (!externalClientContext.isEmpty()) {
            props.setProperty("external-client-context", externalClientContext);
        }
        if (!userName.isEmpty()) {
            props.setProperty("userName", userName);
        }
        props.putAll(statementProperties.toProperties());

        return props;
    }
}
