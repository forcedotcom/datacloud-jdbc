/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getIntegerOrDefault;
import static com.salesforce.datacloud.jdbc.util.StringCompatibility.isNotEmpty;

import com.google.common.base.Strings;
import com.salesforce.datacloud.jdbc.auth.errors.AuthorizationException;
import com.salesforce.datacloud.jdbc.auth.model.AuthenticationResponseWithError;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.ClientBuilder;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class DataCloudTokenProcessor implements TokenProcessor {
    static final String MAX_RETRIES_KEY = "maxRetries";
    static final int DEFAULT_MAX_RETRIES = 3;

    private static final String CORE_ERROR_RESPONSE = "Received an error when acquiring oauth access token";

    private static final String OFF_CORE_ERROR_RESPONSE =
            "Received an error when exchanging oauth access token for data cloud token";

    @Getter
    private AuthenticationSettings settings;

    private AuthenticationStrategy strategy;
    private OkHttpClient client;
    private TokenCache cache;
    private RetryPolicy<AuthenticationResponseWithError> policy;
    private RetryPolicy<AuthenticationResponseWithError> exponentialBackOffPolicy;

    private AuthenticationResponseWithError getTokenWithRetry(CheckedSupplier<AuthenticationResponseWithError> response)
            throws SQLException {
        try {
            return Failsafe.with(this.policy).get(response);
        } catch (FailsafeException ex) {
            if (ex.getCause() != null) {
                throw new DataCloudJDBCException(ex.getCause().getMessage(), "28000", ex);
            }
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    private AuthenticationResponseWithError getDataCloudTokenWithRetry(
            CheckedSupplier<AuthenticationResponseWithError> response) throws SQLException {
        try {
            return Failsafe.with(this.exponentialBackOffPolicy).get(response);
        } catch (FailsafeException ex) {
            if (ex.getCause() != null) {
                throw new DataCloudJDBCException(ex.getCause().getMessage(), "28000", ex);
            }
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    private OAuthToken fetchOAuthToken() throws SQLException {
        val command = strategy.buildAuthenticate();
        val model = (OAuthTokenResponse) getTokenWithRetry(() -> {
            val response = FormCommand.post(this.client, command, OAuthTokenResponse.class);
            return throwExceptionOnError(response, CORE_ERROR_RESPONSE);
        });
        return OAuthToken.of(model);
    }

    private DataCloudToken fetchDataCloudToken() throws SQLException {
        val model = (DataCloudTokenResponse) getDataCloudTokenWithRetry(() -> {
            val oauthToken = getOAuthToken();
            val command =
                    ExchangeTokenAuthenticationStrategy.of(settings, oauthToken).toCommand();
            val response = FormCommand.post(this.client, command, DataCloudTokenResponse.class);
            return throwExceptionOnError(response, OFF_CORE_ERROR_RESPONSE);
        });
        return DataCloudToken.of(model);
    }

    @Override
    public OAuthToken getOAuthToken() throws SQLException {
        try {
            return fetchOAuthToken();
        } catch (Exception ex) {
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    @Override
    public DataCloudToken getDataCloudToken() throws SQLException {
        val cachedDataCloudToken = cache.getDataCloudToken();
        if (cachedDataCloudToken != null && cachedDataCloudToken.isAlive()) {
            return cachedDataCloudToken;
        }

        try {
            return retrieveAndCacheDataCloudToken();
        } catch (Exception ex) {
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    @Override
    public String getLakehouse() throws SQLException {
        val tenantId = getDataCloudToken().getTenantId();
        val dataspace = getSettings().getDataspace();
        val response =
                "lakehouse:" + tenantId + ";" + Optional.ofNullable(dataspace).orElse("");
        log.info("Lakehouse: {}", response);
        return response;
    }

    private DataCloudToken retrieveAndCacheDataCloudToken() throws SQLException {
        try {
            val dataCloudToken = fetchDataCloudToken();
            cache.setDataCloudToken(dataCloudToken);
            return dataCloudToken;
        } catch (Exception ex) {
            cache.clearDataCloudToken();
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    private static AuthenticationResponseWithError throwExceptionOnError(
            AuthenticationResponseWithError response, String message) throws SQLException {
        val token = response.getToken();
        val code = response.getErrorCode();
        val description = response.getErrorDescription();

        if (isNotEmpty(token) && isNotEmpty(code) && isNotEmpty(description)) {
            log.warn("{} but got error code {} : {}", message, code, description);
        } else if (isNotEmpty(code) || isNotEmpty(description)) {
            val authorizationException = AuthorizationException.builder()
                    .message(message + ". " + code + ": " + description)
                    .errorCode(code)
                    .errorDescription(description)
                    .build();
            throw new DataCloudJDBCException(authorizationException.getMessage(), "28000", authorizationException);
        } else if (Strings.isNullOrEmpty(token)) {
            throw new DataCloudJDBCException(message + ", no token in response.", "28000");
        }

        return response;
    }

    public static DataCloudTokenProcessor of(Properties properties) throws DataCloudJDBCException {
        val settings = AuthenticationSettings.of(properties);
        val strategy = AuthenticationStrategy.of(settings);
        val client = ClientBuilder.buildOkHttpClient(properties);
        val policy = buildRetryPolicy(properties);
        val exponentialBackOffPolicy = buildExponentialBackoffRetryPolicy(properties);
        val cache = new TokenCacheImpl();

        return DataCloudTokenProcessor.builder()
                .client(client)
                .policy(policy)
                .exponentialBackOffPolicy(exponentialBackOffPolicy)
                .cache(cache)
                .strategy(strategy)
                .settings(settings)
                .build();
    }

    static RetryPolicy<AuthenticationResponseWithError> buildRetryPolicy(Properties properties) {
        val maxRetries = getIntegerOrDefault(properties, MAX_RETRIES_KEY, DEFAULT_MAX_RETRIES);
        return RetryPolicy.<AuthenticationResponseWithError>builder()
                .withMaxRetries(maxRetries)
                .handleIf(e -> !(e instanceof AuthorizationException))
                .build();
    }

    static RetryPolicy<AuthenticationResponseWithError> buildExponentialBackoffRetryPolicy(Properties properties) {
        val maxRetries = getIntegerOrDefault(properties, MAX_RETRIES_KEY, DEFAULT_MAX_RETRIES);
        return RetryPolicy.<AuthenticationResponseWithError>builder()
                .withMaxRetries(maxRetries)
                .withBackoff(1, 30, ChronoUnit.SECONDS)
                .handleIf(e -> !(e instanceof AuthorizationException))
                .build();
    }
}
