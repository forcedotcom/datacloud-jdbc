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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

interface AuthenticationStrategy {
    static AuthenticationStrategy of(@NonNull Properties properties) throws DataCloudJDBCException {
        val settings = AuthenticationSettings.of(properties);
        return of(settings);
    }

    static AuthenticationStrategy of(@NonNull AuthenticationSettings settings) throws DataCloudJDBCException {
        if (settings instanceof PasswordAuthenticationSettings) {
            return new PasswordAuthenticationStrategy((PasswordAuthenticationSettings) settings);
        } else if (settings instanceof PrivateKeyAuthenticationSettings) {
            return new PrivateKeyAuthenticationStrategy((PrivateKeyAuthenticationSettings) settings);
        } else if (settings instanceof RefreshTokenAuthenticationSettings) {
            return new RefreshTokenAuthenticationStrategy((RefreshTokenAuthenticationSettings) settings);
        } else {
            val rootCauseException = new IllegalArgumentException(Messages.UNKNOWN_SETTINGS_TYPE);
            throw new DataCloudJDBCException(Messages.UNKNOWN_SETTINGS_TYPE, "28000", rootCauseException);
        }
    }

    final class Messages {
        static final String UNKNOWN_SETTINGS_TYPE = "Resolved settings were an unknown type of AuthenticationSettings";

        private Messages() {
            throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
        }
    }

    final class Keys {
        static final String GRANT_TYPE = "grant_type";
        static final String CLIENT_ID = "client_id";
        static final String CLIENT_SECRET = "client_secret";
        static final String USER_AGENT = "User-Agent";

        private Keys() {
            throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
        }
    }

    FormCommand buildAuthenticate() throws SQLException;

    AuthenticationSettings getSettings();
}

abstract class SharedAuthenticationStrategy implements AuthenticationStrategy {
    protected final FormCommand.Builder builder(HttpCommandPath path) throws DataCloudJDBCException {
        val settings = getSettings();
        val builder = FormCommand.builder();

        builder.url(settings.getLoginUri());
        builder.suffix(path.getSuffix());

        builder.header(Keys.USER_AGENT, settings.getUserAgent());

        return builder;
    }
}

@Getter
@RequiredArgsConstructor
class PasswordAuthenticationStrategy extends SharedAuthenticationStrategy {
    private static final String GRANT_TYPE = "password";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private final PasswordAuthenticationSettings settings;

    /**
     * <a
     * href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5">username
     * password flow docs</a>
     */
    @Override
    public FormCommand buildAuthenticate() throws DataCloudJDBCException {
        val builder = super.builder(HttpCommandPath.AUTHENTICATE);

        builder.bodyEntry(Keys.GRANT_TYPE, GRANT_TYPE);
        builder.bodyEntry(USERNAME, settings.getUserName());
        builder.bodyEntry(PASSWORD, settings.getPassword());
        builder.bodyEntry(Keys.CLIENT_ID, settings.getClientId());
        builder.bodyEntry(Keys.CLIENT_SECRET, settings.getClientSecret());

        return builder.build();
    }
}

@Getter
@RequiredArgsConstructor
class RefreshTokenAuthenticationStrategy extends SharedAuthenticationStrategy {
    private static final String GRANT_TYPE = "refresh_token";
    private static final String REFRESH_TOKEN = "refresh_token";

    private final RefreshTokenAuthenticationSettings settings;

    /**
     * <a
     * href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm&type=5">refresh
     * token flow docs</a>
     */
    @Override
    public FormCommand buildAuthenticate() throws DataCloudJDBCException {
        val builder = super.builder(HttpCommandPath.AUTHENTICATE);

        builder.bodyEntry(Keys.GRANT_TYPE, GRANT_TYPE);
        builder.bodyEntry(REFRESH_TOKEN, settings.getRefreshToken());
        builder.bodyEntry(Keys.CLIENT_ID, settings.getClientId());
        builder.bodyEntry(Keys.CLIENT_SECRET, settings.getClientSecret());

        return builder.build();
    }
}

@Getter
@RequiredArgsConstructor
class PrivateKeyAuthenticationStrategy extends SharedAuthenticationStrategy {
    private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";
    private static final String ASSERTION = "assertion";

    private final PrivateKeyAuthenticationSettings settings;

    /**
     * <a href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5">private key flow
     * docs</a>
     */
    @Override
    public FormCommand buildAuthenticate() throws DataCloudJDBCException {
        val builder = super.builder(HttpCommandPath.AUTHENTICATE);

        builder.bodyEntry(Keys.GRANT_TYPE, GRANT_TYPE);
        builder.bodyEntry(ASSERTION, JwtParts.buildJwt(settings));

        return builder.build();
    }
}

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class ExchangeTokenAuthenticationStrategy {
    private static final String GRANT_TYPE = "urn:salesforce:grant-type:external:cdp";
    private static final String ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token";
    static final String SUBJECT_TOKEN_TYPE = "subject_token_type";
    static final String SUBJECT_TOKEN_KEY = "subject_token";
    static final String DATASPACE = "dataspace";

    static ExchangeTokenAuthenticationStrategy of(@NonNull AuthenticationSettings settings, @NonNull OAuthToken token) {
        return new ExchangeTokenAuthenticationStrategy(settings, token);
    }

    @Getter
    private final AuthenticationSettings settings;

    private final OAuthToken token;

    /**
     * <a
     * href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_getting_started_with_cdp.htm">exchange
     * token flow docs</a>
     */
    public FormCommand toCommand() {
        val builder = FormCommand.builder();

        builder.url(token.getInstanceUrl());
        builder.suffix(HttpCommandPath.EXCHANGE.getSuffix());

        builder.header(AuthenticationStrategy.Keys.USER_AGENT, settings.getUserAgent());

        builder.bodyEntry(AuthenticationStrategy.Keys.GRANT_TYPE, GRANT_TYPE);
        builder.bodyEntry(SUBJECT_TOKEN_TYPE, ACCESS_TOKEN);
        builder.bodyEntry(SUBJECT_TOKEN_KEY, token.getToken());

        if (StringUtils.isNotBlank(settings.getDataspace())) {
            builder.bodyEntry(DATASPACE, settings.getDataspace());
        }

        return builder.build();
    }
}

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class RevokeTokenAuthenticationStrategy {
    static final String REVOKE_TOKEN_KEY = "token";

    static RevokeTokenAuthenticationStrategy of(@NonNull AuthenticationSettings settings, @NonNull OAuthToken token) {
        return new RevokeTokenAuthenticationStrategy(settings, token);
    }

    @Getter
    private final AuthenticationSettings settings;

    private final OAuthToken token;

    public FormCommand toCommand() {
        val builder = FormCommand.builder();

        builder.url(token.getInstanceUrl());
        builder.suffix(HttpCommandPath.REVOKE.getSuffix());

        builder.header(AuthenticationStrategy.Keys.USER_AGENT, settings.getUserAgent());

        builder.bodyEntry(REVOKE_TOKEN_KEY, token.getToken());

        return builder.build();
    }
}

@Getter
enum HttpCommandPath {
    AUTHENTICATE("services/oauth2/token"),
    EXCHANGE("services/a360/token"),
    REVOKE("services/oauth2/revoke");

    private final URI suffix;

    @SneakyThrows
    HttpCommandPath(String suffix) {
        this.suffix = new URI(suffix);
    }
}
