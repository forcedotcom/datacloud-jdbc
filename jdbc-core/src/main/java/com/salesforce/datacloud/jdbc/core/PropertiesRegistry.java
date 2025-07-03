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

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.Builder;

/**
 * This class is a register of properties that are relevant for the JDBC Connection and Statement.
 */
public class PropertiesRegistry {
    /**
     * Handle the property deserializiaton concerns
     */
    static interface PropertyInterpreter {
        /**
         * Interpret the property value and set the interpreted value on the settings object.
         * @param propertyValue The string value of the property.
         * @param settings The interpreter will set the interpreted value on this settings object.
         */
        void interpret(String propertyKey, String propertyValue, Settings settings) throws Exception;
    }

    /**
     * Serializes setting values to properties
     */
    static interface PropertySerializer {
        /**
         * Convert the setting(s) for the handled property to property value string(s) and add it/them to the properties object.
         * @param settings The settings object that contains the current value for the property.
         * @param key The key of the property.
         * @param properties The properties object to append the property value to.
         */
        void append(Settings settings, String key, Properties properties);
    }

    /**
     * A JdbcProperty is a property that can be set on a DataCloudConnection.
     *
     * @param defaultValue The default value of the property.
     * @param interpreter The interpreter that will be used to interpret the property value.
     */
    @Builder
    static class JdbcProperty {
        // The interpreter that will be used to interpret the property value.
        final PropertyInterpreter interpreter;
        // The serializer that will be used to serialize the property value.
        final PropertySerializer serializer;
        // If the key is a prefix, the prefix to suffix separator is '.'
        @Builder.Default
        final boolean prefixedSetting = false;
    }

    static Map<String, JdbcProperty> supportedProperties = ImmutableMap.of(
            // The query timeout property, zero or negative values are interpreted as infinite timeout.
            // Positive values are interpreted as the number of seconds for the timeout
            "queryTimeout",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        Duration value = Duration.ofSeconds(Integer.parseInt(propertyValue));
                        if (value.isZero() || value.isNegative()) {

                            settings.statementSettings.queryTimeout = Duration.ZERO;
                        } else {
                            settings.statementSettings.queryTimeout = value;
                        }
                    })
                    .serializer((settings, key, properties) -> {
                        properties.setProperty(
                                key, String.valueOf(settings.statementSettings.queryTimeout.getSeconds()));
                    })
                    .build(),
            "dataspace",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        settings.connectionSettings.dataspace = propertyValue;
                    })
                    .serializer((settings, key, properties) -> {
                        if (!settings.connectionSettings.dataspace.isEmpty()) {
                            properties.setProperty(key, settings.connectionSettings.dataspace);
                        }
                    })
                    .build(),
            "workload",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        settings.connectionSettings.workload = propertyValue;
                    })
                    .serializer((settings, key, properties) -> {
                        if (!settings.connectionSettings.workload.isEmpty()) {
                            properties.setProperty(key, settings.connectionSettings.workload);
                        }
                    })
                    .build(),
            "external-client-context",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        settings.connectionSettings.externalClientContext = propertyValue;
                    })
                    .serializer((settings, key, properties) -> {
                        if (!settings.connectionSettings.externalClientContext.isEmpty()) {
                            properties.setProperty(key, settings.connectionSettings.externalClientContext);
                        }
                    })
                    .build(),
            "userName",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        settings.connectionSettings.userName = propertyValue;
                    })
                    .serializer((settings, key, properties) -> {
                        if (!settings.connectionSettings.userName.isEmpty()) {
                            properties.setProperty(key, settings.connectionSettings.userName);
                        }
                    })
                    .build(),
            // The query setting property, this is a prefixed setting and the interpreter will split the key and
            // set the value on the query settings map.
            "querySetting",
            JdbcProperty.builder()
                    .interpreter((propertyKey, propertyValue, settings) -> {
                        String key = propertyKey.substring("querySetting.".length());
                        settings.statementSettings.querySettings.put(key, propertyValue);
                    })
                    .serializer((settings, key, properties) -> {
                        for (Map.Entry<String, String> entry : settings.statementSettings.querySettings.entrySet()) {
                            properties.setProperty(key + "." + entry.getKey(), entry.getValue());
                        }
                    })
                    .prefixedSetting(true)
                    .build());
}
