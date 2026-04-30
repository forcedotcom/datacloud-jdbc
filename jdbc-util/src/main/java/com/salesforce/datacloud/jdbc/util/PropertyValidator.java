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

import com.google.common.collect.ImmutableSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

/**
 * @deprecated Use {@link PropertyParsingUtils#validateRemainingProperties(Properties)} instead.
 */
@Deprecated
@UtilityClass
public class PropertyValidator {

    private static final Set<String> KNOWN_KEYS = ImmutableSet.<String>builder()
            .add("loginURL", "user", "userName", "password", "privateKey", "clientId", "clientSecret")
            .add("refreshToken", "coreToken", "cdpToken", "tenantUrl")
            .add("dataspace", "workload", "external-client-context")
            .add("User-Agent", "maxRetries")
            .add("direct")
            .add("queryTimeout", "queryTimeoutLocalEnforcementDelay")
            .add("internalEndpoint", "port", "tenantId", "coreTenantId")
            .build();

    private static final Set<String> KNOWN_PREFIXES = ImmutableSet.of("querySetting.", "grpc.");

    public static void validate(Properties properties) throws SQLException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        final Set<String> unknown = properties.stringPropertyNames().stream()
                .filter(key -> !KNOWN_KEYS.contains(key))
                .filter(key -> KNOWN_PREFIXES.stream().noneMatch(key::startsWith))
                .collect(Collectors.toSet());

        if (!unknown.isEmpty()) {
            throw new SQLException("Unknown JDBC properties: " + String.join(", ", unknown)
                    + ". Review documentation and use 'querySetting.<name>' for session settings if applicable.");
        }
    }
}
