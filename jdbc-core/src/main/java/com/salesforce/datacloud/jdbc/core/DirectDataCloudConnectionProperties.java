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
 * Connection properties that control the Direct Data cloud JDBC connection behavior.
 */
@Getter
@Builder
public class DirectDataCloudConnectionProperties {
    // Property key constants
    public static final String direct = "direct";

    // property to disable SSL (for testing only, we might change the implementation in future)
    public static final String sslDisabled = "ssl_disabled";

    // JKS truststore properties - for trust verification
    public static final String truststorePath = "truststore_path";
    public static final String truststorePassword = "truststore_password";
    public static final String truststoreType = "truststore_type";

    // PEM certificate properties - for trust verification and client authentication
    public static final String clientCertPath = "client_cert_path";
    public static final String clientKeyPath = "client_key_path";
    public static final String caCertPath = "ca_cert_path";

    // Instance fields for parsed property values
    @Builder.Default
    private final boolean directConnection = false;

    @Builder.Default
    private final boolean sslDisabledFlag = false;

    @Builder.Default
    private final String truststorePathValue = "";

    @Builder.Default
    private final String truststorePasswordValue = "";

    @Builder.Default
    private final String truststoreTypeValue = "JKS";

    @Builder.Default
    private final String clientCertPathValue = "";

    @Builder.Default
    private final String clientKeyPathValue = "";

    @Builder.Default
    private final String caCertPathValue = "";

    /**
     * Parses direct connection properties from a Properties object.
     *
     * @param props The properties to parse
     * @return A DirectDataCloudConnectionProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static DirectDataCloudConnectionProperties of(Properties props) throws DataCloudJDBCException {
        if (props == null) {
            return DirectDataCloudConnectionProperties.builder().build();
        }

        DirectDataCloudConnectionPropertiesBuilder builder = DirectDataCloudConnectionProperties.builder();

        // Parse direct connection flag
        String directValue = props.getProperty(direct);
        if (directValue != null) {
            builder.directConnection(Boolean.parseBoolean(directValue));
        }

        // Parse SSL disabled flag
        String sslDisabledValue = props.getProperty(sslDisabled);
        if (sslDisabledValue != null) {
            builder.sslDisabledFlag(Boolean.parseBoolean(sslDisabledValue));
        }

        // Parse truststore properties
        String truststorePathVal = props.getProperty(truststorePath);
        if (truststorePathVal != null) {
            builder.truststorePathValue(truststorePathVal);
        }

        String truststorePasswordVal = props.getProperty(truststorePassword);
        if (truststorePasswordVal != null) {
            builder.truststorePasswordValue(truststorePasswordVal);
        }

        String truststoreTypeVal = props.getProperty(truststoreType);
        if (truststoreTypeVal != null) {
            builder.truststoreTypeValue(truststoreTypeVal);
        }

        // Parse client certificate properties
        String clientCertPathVal = props.getProperty(clientCertPath);
        if (clientCertPathVal != null) {
            builder.clientCertPathValue(clientCertPathVal);
        }

        String clientKeyPathVal = props.getProperty(clientKeyPath);
        if (clientKeyPathVal != null) {
            builder.clientKeyPathValue(clientKeyPathVal);
        }

        String caCertPathVal = props.getProperty(caCertPath);
        if (caCertPathVal != null) {
            builder.caCertPathValue(caCertPathVal);
        }

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the direct connection properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (directConnection) {
            props.setProperty(direct, String.valueOf(directConnection));
        }

        if (sslDisabledFlag) {
            props.setProperty(sslDisabled, String.valueOf(sslDisabledFlag));
        }

        if (!truststorePathValue.isEmpty()) {
            props.setProperty(truststorePath, truststorePathValue);
        }

        if (!truststorePasswordValue.isEmpty()) {
            props.setProperty(truststorePassword, truststorePasswordValue);
        }

        if (!truststoreTypeValue.equals("JKS")) {
            props.setProperty(truststoreType, truststoreTypeValue);
        }

        if (!clientCertPathValue.isEmpty()) {
            props.setProperty(clientCertPath, clientCertPathValue);
        }

        if (!clientKeyPathValue.isEmpty()) {
            props.setProperty(clientKeyPath, clientKeyPathValue);
        }

        if (!caCertPathValue.isEmpty()) {
            props.setProperty(caCertPath, caCertPathValue);
        }

        return props;
    }
}
