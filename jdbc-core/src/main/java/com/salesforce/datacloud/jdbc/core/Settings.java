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

import com.salesforce.datacloud.jdbc.core.PropertiesRegistry.JdbcProperty;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.With;

/**
 * This class captures all the different settings that can be configured through properties.
 */
@Getter
public class Settings {
    /**
     * The supported settings that are used by the statement
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatementSettings {
        // The query timeout, a zero duration is interpreted as infinite timeout, can be configured through the
        // `queryTimeout` property.
        @With
        Duration queryTimeout = Duration.ZERO;

        // The query settings to use for the connection, can be configured through the `querySetting.<key> = <value>`
        // property.
        Map<String, String> querySettings = new HashMap<>();
    }

    /**
     * Settings that control the connection
     */
    @Getter
    public static class ConnectionSettings {
        // The dataspace to use for the connection, can be configured through the `dataspace` property.
        String dataspace = "";
        // The workload to use for the connection, can be configured through the `workload` property.
        String workload = "jdbcv3";
        // The external client context to use for the connection, can be configured through the
        // `external-client-context` property.
        String externalClientContext = "";
        // The user name to use for the connection, can be configured through the `userName` property.
        String userName = "";
    }

    // The query settings that will be used to control query execution.
    final StatementSettings statementSettings = new StatementSettings();
    // The connection settings that will be used to control the connection.
    final ConnectionSettings connectionSettings = new ConnectionSettings();

    /**
     * Interpret the generic JDBC properties map and convert it into the interpreted (and default values available) JdbcProperties object.
     * @param properties The properties de
     * @return A Settings object.
     * @throws DataCloudJDBCException throws exception if properties could not properly interpreted
     */
    public static Settings of(Properties properties) throws DataCloudJDBCException {
        Settings settings = new Settings();
        // Iterate over each property and check if it is supported and if so, interpret it.
        for (String key : properties.stringPropertyNames()) {
            try {
                if (PropertiesRegistry.supportedProperties.containsKey(key)) {
                    PropertiesRegistry.supportedProperties
                            .get(key)
                            .interpreter
                            .interpret(key, properties.getProperty(key), settings);
                } else {
                    // Check if the property is a prefixed setting
                    String[] parts = key.split("\\.", 2);
                    if (parts.length == 2 && PropertiesRegistry.supportedProperties.containsKey(parts[0])) {
                        JdbcProperty property = PropertiesRegistry.supportedProperties.get(parts[0]);
                        if (!property.prefixedSetting) {
                            throw new DataCloudJDBCException("Property " + key
                                    + " is a prefixed setting but the property is not a prefixed setting");
                        }
                        property.interpreter.interpret(key, properties.getProperty(key), settings);
                    }
                }
                // Remove key so that we can track which properties were used
                properties.remove(key);
            } catch (Exception e) {
                throw new DataCloudJDBCException("Failed to parse property " + key + ": {}", e.getMessage());
            }
        }
        return settings;
    }

    /**
     * Convert the JdbcProperties object into a Properties object. The properties object will contain the explicitly set
     * properties as well as the default values for the supported properties.
     * @return A Properties object that contains the explicitly set properties as well as the default values for the supported properties.
     */
    public Properties toProperties() {
        Properties properties = new Properties();
        for (Entry<String, JdbcProperty> entry : PropertiesRegistry.supportedProperties.entrySet()) {
            entry.getValue().serializer.append(this, entry.getKey(), properties);
        }
        return properties;
    }
}
