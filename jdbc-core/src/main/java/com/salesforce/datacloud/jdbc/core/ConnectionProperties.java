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
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * Connection properties that control the JDBC connection behavior.
 */
@Getter
@Builder
public class ConnectionProperties {
    // The dataspace to use for the connection
    @Builder.Default
    private final String dataspace = "";

    // The workload to use for the connection (default: jdbcv3)
    @Builder.Default
    private final String workload = "jdbcv3";

    // The external client context to use for the connection
    @Builder.Default
    private final String externalClientContext = "";

    // The user name to use for the connection
    @Builder.Default
    private final String userName = "";

    // Statement properties associated with this connection
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
