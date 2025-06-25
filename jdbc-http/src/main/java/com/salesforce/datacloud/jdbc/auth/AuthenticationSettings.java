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
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.copy;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.required;

import com.google.common.collect.ImmutableSet;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.PropertiesExtensions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.val;

@Getter
public abstract class AuthenticationSettings {
    public static AuthenticationSettings of(@NonNull Properties properties) throws DataCloudJDBCException {
        checkNotEmpty(properties);
        checkHasAllRequired(properties);

        if (hasPrivateKey(properties)) {
            return new PrivateKeyAuthenticationSettings(properties);
        } else if (hasPassword(properties)) {
            return new PasswordAuthenticationSettings(properties);
        } else if (hasRefreshToken(properties)) {
            return new RefreshTokenAuthenticationSettings(properties);
        } else {
            throw new DataCloudJDBCException(Messages.PROPERTIES_MISSING, "28000");
        }
    }

    public static boolean hasAll(Properties properties, Set<String> keys) {
        return keys.stream().allMatch(k -> optional(properties, k).isPresent());
    }

    public static boolean hasAny(Properties properties) {
        return hasPrivateKey(properties) || hasPassword(properties) || hasRefreshToken(properties);
    }

    private static boolean hasPrivateKey(Properties properties) {
        return hasAll(properties, Keys.PRIVATE_KEY_KEYS);
    }

    private static boolean hasPassword(Properties properties) {
        return hasAll(properties, Keys.PASSWORD_KEYS);
    }

    private static boolean hasRefreshToken(Properties properties) {
        return hasAll(properties, Keys.REFRESH_TOKEN_KEYS);
    }

    private static void checkNotEmpty(@NonNull Properties properties) throws DataCloudJDBCException {
        if (properties.isEmpty()) {
            throw new DataCloudJDBCException(
                    Messages.PROPERTIES_EMPTY, "28000", new IllegalArgumentException(Messages.PROPERTIES_EMPTY));
        }
    }

    private static void checkHasAllRequired(Properties properties) throws DataCloudJDBCException {
        if (hasAll(properties, Keys.REQUIRED_KEYS)) {
            return;
        }

        val missing = Keys.REQUIRED_KEYS.stream()
                .filter(k -> !optional(properties, k).isPresent())
                .collect(Collectors.joining(", ", Messages.PROPERTIES_REQUIRED, ""));

        throw new DataCloudJDBCException(missing, "28000", new IllegalArgumentException(missing));
    }

    final URI getLoginUri() throws DataCloudJDBCException {
        try {
            return new URI(loginUrl);
        } catch (URISyntaxException ex) {
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    protected AuthenticationSettings(@NonNull Properties properties) throws DataCloudJDBCException {
        checkNotEmpty(properties);

        this.relevantProperties = copy(properties, Keys.ALL);

        this.loginUrl = required(relevantProperties, Keys.LOGIN_URL);
        this.clientId = required(relevantProperties, Keys.CLIENT_ID);
        this.clientSecret = required(relevantProperties, Keys.CLIENT_SECRET);

        this.dataspace = optional(relevantProperties, Keys.DATASPACE).orElse(Defaults.DATASPACE);
        this.userAgent = optional(relevantProperties, Keys.USER_AGENT).orElse(Defaults.USER_AGENT);
        this.maxRetries = optional(relevantProperties, Keys.MAX_RETRIES)
                .map(PropertiesExtensions::toIntegerOrNull)
                .orElse(Defaults.MAX_RETRIES);
    }

    private final Properties relevantProperties;

    private final String loginUrl;
    private final String clientId;
    private final String clientSecret;
    private final String dataspace;
    private final String userAgent;
    private final int maxRetries;

    @UtilityClass
    protected static class Keys {
        static final String LOGIN_URL = "loginURL";
        static final String USER_NAME = "userName";
        static final String PASSWORD = "password";
        static final String PRIVATE_KEY = "privateKey";
        static final String CLIENT_SECRET = "clientSecret";
        static final String CLIENT_ID = "clientId";
        static final String DATASPACE = "dataspace";
        static final String MAX_RETRIES = "maxRetries";
        static final String USER_AGENT = "User-Agent";
        static final String REFRESH_TOKEN = "refreshToken";

        static final Set<String> REQUIRED_KEYS = ImmutableSet.of(LOGIN_URL, CLIENT_ID, CLIENT_SECRET);

        static final Set<String> OPTIONAL_KEYS = ImmutableSet.of(DATASPACE, USER_AGENT, MAX_RETRIES);

        static final Set<String> PASSWORD_KEYS = ImmutableSet.of(USER_NAME, PASSWORD);

        static final Set<String> PRIVATE_KEY_KEYS = ImmutableSet.of(USER_NAME, PRIVATE_KEY);

        static final Set<String> REFRESH_TOKEN_KEYS = ImmutableSet.of(REFRESH_TOKEN);

        static final Set<String> ALL = Stream.of(
                        REQUIRED_KEYS, OPTIONAL_KEYS, PASSWORD_KEYS, PRIVATE_KEY_KEYS, REFRESH_TOKEN_KEYS)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    protected static final class Defaults {
        static final int MAX_RETRIES = 3;
        static final String DATASPACE = null;
        static final String USER_AGENT = formatDriverInfo();

        private Defaults() {
            throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
        }
    }

    protected static final class Messages {
        static final String PROPERTIES_NULL = "properties is marked non-null but is null";
        static final String PROPERTIES_EMPTY = "Properties cannot be empty when creating AuthenticationSettings.";
        static final String PROPERTIES_MISSING =
                "Properties did not contain valid settings for known authentication strategies: password, privateKey, or refreshToken with coreToken";
        static final String PROPERTIES_REQUIRED = "Properties did not contain the following required settings: ";

        private Messages() {
            throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
        }
    }
}

@Getter
class PasswordAuthenticationSettings extends AuthenticationSettings {
    protected PasswordAuthenticationSettings(@NonNull Properties properties) throws DataCloudJDBCException {
        super(properties);

        this.password = required(this.getRelevantProperties(), Keys.PASSWORD);
        this.userName = required(this.getRelevantProperties(), Keys.USER_NAME);
    }

    private final String password;
    private final String userName;
}

@Getter
class PrivateKeyAuthenticationSettings extends AuthenticationSettings {
    protected PrivateKeyAuthenticationSettings(@NonNull Properties properties) throws DataCloudJDBCException {
        super(properties);

        this.privateKey = required(this.getRelevantProperties(), Keys.PRIVATE_KEY);
        this.userName = required(this.getRelevantProperties(), Keys.USER_NAME);
    }

    private final String privateKey;
    private final String userName;
}

@Getter
class RefreshTokenAuthenticationSettings extends AuthenticationSettings {
    protected RefreshTokenAuthenticationSettings(@NonNull Properties properties) throws DataCloudJDBCException {
        super(properties);

        this.refreshToken = required(this.getRelevantProperties(), Keys.REFRESH_TOKEN);
    }

    private final String refreshToken;
}
