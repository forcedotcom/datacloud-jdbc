/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.StringCompatibility.isNotEmpty;

import com.google.common.base.Strings;
import com.salesforce.datacloud.jdbc.auth.errors.AuthorizationException;
import com.salesforce.datacloud.jdbc.auth.model.AuthenticationResponseWithError;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import com.salesforce.datacloud.jdbc.http.HttpClientProperties;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.jsonwebtoken.Jwts;
import java.net.URI;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class DataCloudTokenProvider {
    private static final URI AUTHENTICATE_URL = URI.create("services/oauth2/token");
    private static final URI EXCHANGE_TOKEN_URL = URI.create("services/a360/token");

    @Getter
    private SalesforceAuthProperties settings;

    private OkHttpClient client;
    private DataCloudToken cachedDataCloudToken;
    private RetryPolicy<AuthenticationResponseWithError> exponentialBackOffPolicy;

    public static DataCloudTokenProvider of(
            HttpClientProperties clientProperties, SalesforceAuthProperties authProperties) throws SQLException {
        val settings = authProperties;
        val client = clientProperties.buildOkHttpClient();
        val exponentialBackOffPolicy = buildExponentialBackoffRetryPolicy(clientProperties.getMaxRetries());

        return DataCloudTokenProvider.builder()
                .client(client)
                .exponentialBackOffPolicy(exponentialBackOffPolicy)
                .settings(settings)
                .build();
    }

    private OAuthToken fetchOAuthToken() throws SQLException {
        val command = buildAuthenticate();
        val model = getWithRetry(() -> {
            val response = FormCommand.post(this.client, command, OAuthTokenResponse.class);
            return throwExceptionOnError(response, "Received an error when acquiring oauth access token");
        });
        return OAuthToken.of(model);
    }

    private DataCloudToken exchangeOauthForDataCloudToken() throws SQLException {
        val oauthToken = getOAuthToken();

        val model = getWithRetry(() -> {
            // Setup token exchange command
            // https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_getting_started_with_cdp.htm
            val builder = FormCommand.builder();
            builder.url(oauthToken.getInstanceUrl());
            builder.suffix(EXCHANGE_TOKEN_URL);
            builder.bodyEntry("grant_type", "urn:salesforce:grant-type:external:cdp");
            builder.bodyEntry("subject_token_type", "urn:ietf:params:oauth:token-type:access_token");
            builder.bodyEntry("subject_token", oauthToken.getToken());
            if (StringUtils.isNotEmpty(settings.getDataspace())) {
                builder.bodyEntry("dataspace", settings.getDataspace());
            }

            // Execute the token exchange command
            val command = builder.build();
            val response = FormCommand.post(this.client, command, DataCloudTokenResponse.class);
            return throwExceptionOnError(
                    response, "Received an error when exchanging oauth access token for data cloud token");
        });
        return DataCloudToken.of(model);
    }

    public OAuthToken getOAuthToken() throws SQLException {
        return fetchOAuthToken();
    }

    public DataCloudToken getDataCloudToken() throws SQLException {
        if (cachedDataCloudToken != null && cachedDataCloudToken.isAlive()) {
            if (cachedDataCloudToken.isAlive()) {
                return cachedDataCloudToken;
            }
            cachedDataCloudToken = null;
        }

        cachedDataCloudToken = exchangeOauthForDataCloudToken();
        return cachedDataCloudToken;
    }

    public String getLakehouseName() throws SQLException {
        val tenantId = getDataCloudToken().getTenantId();
        val dataspace = getSettings().getDataspace();
        val response =
                "lakehouse:" + tenantId + ";" + Optional.ofNullable(dataspace).orElse("");
        log.info("Lakehouse: {}", response);
        return response;
    }

    /**
     * Builds the authentication command.
     */
    private FormCommand buildAuthenticate() throws SQLException {
        val builder = FormCommand.builder();

        builder.url(settings.getLoginUrl());
        builder.suffix(AUTHENTICATE_URL);
        builder.bodyEntry("client_id", settings.getClientId());
        builder.bodyEntry("client_secret", settings.getClientSecret());

        switch (settings.getAuthenticationMode()) {
            case PASSWORD:
                // https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm
                builder.bodyEntry("grant_type", "password");
                builder.bodyEntry("username", settings.getUserName());
                builder.bodyEntry("password", settings.getPassword());
                break;
            case PRIVATE_KEY:
                // https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm
                builder.bodyEntry("grant_type", "urn:salesforce:grant-type:external:cdp");
                builder.bodyEntry("assertion", buildJwt());
                break;
            case REFRESH_TOKEN:
                // https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm
                builder.bodyEntry("grant_type", "refresh_token");
                builder.bodyEntry("refresh_token", settings.getRefreshToken());
                break;
        }

        return builder.build();
    }

    /**
     * Build a JWT assertion for the private key authentication mode.
     */
    private String buildJwt() throws SQLException {
        assert settings.getAuthenticationMode() == SalesforceAuthProperties.AuthenticationMode.PRIVATE_KEY;
        try {
            val now = Instant.now();
            return Jwts.builder()
                    .issuer(settings.getClientId())
                    .subject(settings.getUserName())
                    .audience()
                    .add(settings.getLoginUrl().getHost())
                    .and()
                    .issuedAt(Date.from(now))
                    .expiration(Date.from(now.plus(2L, ChronoUnit.MINUTES)))
                    .signWith(settings.getPrivateKey(), Jwts.SIG.RS256)
                    .compact();
        } catch (Exception ex) {
            throw new SQLException(
                    "JWT assertion creation failed. Please check Username, Client Id and Private key.", "28000", ex);
        }
    }

    private static <T extends AuthenticationResponseWithError> T throwExceptionOnError(T response, String message)
            throws SQLException, AuthorizationException {
        val token = response.getToken();
        val code = response.getErrorCode();
        val description = response.getErrorDescription();

        if (isNotEmpty(token) && isNotEmpty(code) && isNotEmpty(description)) {
            log.warn("{} but got error code {} : {}", message, code, description);
        } else if (isNotEmpty(code) || isNotEmpty(description)) {
            throw AuthorizationException.builder()
                    .message(message + ". " + code + ": " + description)
                    .errorCode(code)
                    .errorDescription(description)
                    .build();
        } else if (Strings.isNullOrEmpty(token)) {
            throw new SQLException(message + ", no token in response.", "28000");
        }

        return response;
    }

    private <T extends AuthenticationResponseWithError> T getWithRetry(CheckedSupplier<T> response)
            throws SQLException {
        try {
            return Failsafe.with(this.exponentialBackOffPolicy).get(response);
        } catch (FailsafeException ex) {
            if (ex.getCause() != null) {
                throw new SQLException(ex.getCause().getMessage(), "28000", ex);
            }
            throw new SQLException(ex.getMessage(), "28000", ex);
        }
    }

    private static RetryPolicy<AuthenticationResponseWithError> buildExponentialBackoffRetryPolicy(int maxRetries) {
        return RetryPolicy.<AuthenticationResponseWithError>builder()
                .withMaxRetries(maxRetries)
                .withBackoff(1, 30, ChronoUnit.SECONDS)
                .handleIf(e -> !(e instanceof AuthorizationException))
                .build();
    }
}
