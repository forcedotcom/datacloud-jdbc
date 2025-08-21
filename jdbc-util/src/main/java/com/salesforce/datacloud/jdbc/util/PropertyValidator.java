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
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates incoming JDBC properties and raises user errors for unknown properties.
 *
 * This does not validate presence of required keys (handled elsewhere), and it
 * deliberately allows any subkey under the {@code querySetting.} namespace to
 * defer detailed validation to the server.
 */
public final class PropertyValidator {
    private PropertyValidator() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static final Set<String> KNOWN_KEYS = ImmutableSet.<String>builder()
            // Auth and basics
            .add("loginURL", "user", "userName", "password", "privateKey", "clientId", "clientSecret")
            .add("refreshToken", "coreToken")
            // Connection and metadata
            .add("dataspace", "workload", "external-client-context")
            // Driver/HTTP options
            .add("User-Agent", "maxRetries")
            // Direct connection/testing
            .add("direct")
            // Statement properties
            .add("queryTimeout", "queryTimeoutLocalEnforcementDelay")
            // DataSource-specific extras (passed through)
            .add("internalEndpoint", "port", "tenantId", "coreTenantId")
            .build();

    private static final Set<String> KNOWN_PREFIXES = ImmutableSet.of(
            // Session settings
            "querySetting.",
            // gRPC channel configuration
            "grpc.");

    public static void validate(Properties properties) throws DataCloudJDBCException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        final Set<String> unknown = properties.stringPropertyNames().stream()
                .filter(key -> !KNOWN_KEYS.contains(key))
                .filter(key -> KNOWN_PREFIXES.stream().noneMatch(key::startsWith))
                .collect(Collectors.toSet());

        if (!unknown.isEmpty()) {
            throw new DataCloudJDBCException("Unknown JDBC properties: " + String.join(", ", unknown)
                    + ". Review documentation and use 'querySetting.<name>' for session settings if applicable.");
        }
    }
}
