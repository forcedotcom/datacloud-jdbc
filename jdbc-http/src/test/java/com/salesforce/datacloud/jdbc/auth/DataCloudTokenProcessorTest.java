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

import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeTenantId;
import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeToken;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPassword;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.errors.AuthorizationException;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.val;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class DataCloudTokenProcessorTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    static final Function<Properties, RetryPolicy> buildRetry = DataCloudTokenProcessor::buildRetryPolicy;

    Random random = new Random(10);

    @SneakyThrows
    @Test
    void createsRetryPolicyWithAndWithoutDefault() {
        val properties = new Properties();
        val none = buildRetry.apply(properties);

        softly.assertThat(none.getConfig().getMaxRetries()).isEqualTo(DataCloudTokenProcessor.DEFAULT_MAX_RETRIES);

        val retries = random.nextInt(12345);
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, Integer.toString(retries));

        val some = buildRetry.apply(properties);
        softly.assertThat(some.getConfig().getMaxRetries()).isEqualTo(retries);
    }

    @SneakyThrows
    @Test
    void retryPolicyDoesntHandleAuthorizationException() {
        val retry = buildRetry.apply(new Properties());

        val ex =
                assertThrows(FailsafeException.class, () -> Failsafe.with(retry).get(() -> {
                    throw AuthorizationException.builder().build();
                }));

        AssertionsForClassTypes.assertThat(ex).hasRootCauseInstanceOf(AuthorizationException.class);
    }

    @SneakyThrows
    @Test
    void retryPolicyOnlyHandlesTokenException() {
        val retry = buildRetry.apply(new Properties());
        val expected = UUID.randomUUID().toString();

        assertThrows(IllegalArgumentException.class, () -> Failsafe.with(retry).get(() -> {
            throw new IllegalArgumentException();
        }));

        val counter = new AtomicInteger(0);

        val actual = Failsafe.with(retry).get(() -> {
            if (counter.getAndIncrement() < DataCloudTokenProcessor.DEFAULT_MAX_RETRIES) {
                throw new SQLException("hi");
            }

            return expected;
        });

        assertThat(actual).isEqualTo(expected);
    }

    @SneakyThrows
    @Test
    void retryPolicyRetriesExpectedNumberOfTimesThenGivesUp() {
        val properties = propertiesForPassword("un", "pw");
        val expectedTriesCount = DataCloudTokenProcessor.DEFAULT_MAX_RETRIES + 1;
        try (val server = new MockWebServer()) {
            server.start();
            for (int x = 0; x < expectedTriesCount; x++) {
                server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
            }
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());
            assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getOAuthToken());
            assertThat(server.getRequestCount()).isEqualTo(expectedTriesCount);
            server.shutdown();
        }
    }

    @SneakyThrows
    @Test
    void exponentialBackoffPolicyRetriesExpectedNumberOfTimesThenGivesUp() {
        val mapper = new ObjectMapper();
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        oAuthTokenResponse.setToken(accessToken);
        val properties = propertiesForPassword("un", "pw");
        properties.put("metadataCacheTtlMs", "0");
        val expectedTriesCount = 2 * DataCloudTokenProcessor.DEFAULT_MAX_RETRIES + 1;
        try (val server = new MockWebServer()) {
            server.start();
            for (int x = 0; x < expectedTriesCount; x++) {
                server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
                server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
            }
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());
            assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());
            assertThat(server.getRequestCount()).isEqualTo(expectedTriesCount);
            server.shutdown();
        }
    }

    @SneakyThrows
    @Test
    void oauthTokenRetrieved() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        oAuthTokenResponse.setToken(accessToken);

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));

            val actual = DataCloudTokenProcessor.of(properties).getOAuthToken();
            assertThat(actual.getToken()).as("access token").isEqualTo(accessToken);
            assertThat(actual.getInstanceUrl().toString())
                    .as("instance url")
                    .isEqualTo(server.url("").toString());
        }
    }

    @SneakyThrows
    @Test
    void bothTokensRetrievedWithLakehouse() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.remove(AuthenticationSettings.Keys.DATASPACE);
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setExpiresIn(60000);
            dataCloudTokenResponse.setToken(fakeToken);
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            val expected = DataCloudToken.of(dataCloudTokenResponse);
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val processor = DataCloudTokenProcessor.of(properties);
            assertThat(processor.getLakehouse()).as("lakehouse").isEqualTo("lakehouse:" + fakeTenantId + ";");

            val actual = processor.getDataCloudToken();
            assertThat(actual.getAccessToken()).as("access token").isEqualTo(expected.getAccessToken());
            assertThat(actual.getTenantUrl()).as("tenant url").isEqualTo(expected.getTenantUrl());
            assertThat(actual.getTenantId()).as("tenant id").isEqualTo(fakeTenantId);
        }
    }

    @SneakyThrows
    @Test
    void bothTokensRetrievedWithLakehouseAndDataspace() {
        val mapper = new ObjectMapper();
        val dataspace = UUID.randomUUID().toString();
        val properties = propertiesForPassword("un", "pw");
        properties.put(AuthenticationSettings.Keys.DATASPACE, dataspace);
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setToken(fakeToken);
            dataCloudTokenResponse.setExpiresIn(60000);
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            val expected = DataCloudToken.of(dataCloudTokenResponse);
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val processor = DataCloudTokenProcessor.of(properties);
            assertThat(processor.getLakehouse())
                    .as("lakehouse")
                    .isEqualTo("lakehouse:" + fakeTenantId + ";" + dataspace);

            val actual = processor.getDataCloudToken();
            assertThat(actual.getAccessToken()).as("access token").isEqualTo(expected.getAccessToken());
            assertThat(actual.getTenantUrl()).as("tenant url").isEqualTo(expected.getTenantUrl());
            assertThat(actual.getTenantId()).as("tenant id").isEqualTo(fakeTenantId);
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseContainsErrorDescription() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            val errorDescription = UUID.randomUUID().toString();
            val errorCode = UUID.randomUUID().toString();
            dataCloudTokenResponse.setErrorDescription(errorDescription);
            dataCloudTokenResponse.setErrorCode(errorCode);
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());

            assertAuthorizationException(
                    ex,
                    "Received an error when exchanging oauth access token for data cloud token.",
                    errorCode + ": " + errorDescription);
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenOauthTokenResponseIsMissingAccessToken() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());

            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));

            val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());

            assertSQLException(ex, "Received an error when acquiring oauth access token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseIsMissingAccessToken() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());

            assertSQLException(
                    ex,
                    "Received an error when exchanging oauth access token for data cloud token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenOauthTokenResponseIsNull() {
        val properties = propertiesForPassword("un", "pw");
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody("{}"));

            val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());

            assertSQLException(ex, "Received an error when acquiring oauth access token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseIsNull() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(DataCloudTokenProcessor.MAX_RETRIES_KEY, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            properties.setProperty(
                    AuthenticationSettings.Keys.LOGIN_URL, server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody("{}"));

            val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudTokenProcessor.of(properties)
                    .getDataCloudToken());

            assertSQLException(
                    ex,
                    "Received an error when exchanging oauth access token for data cloud token, no token in response.");
        }
    }

    private static void assertAuthorizationException(Throwable actual, CharSequence... messages) {
        AssertionsForClassTypes.assertThat(actual)
                .hasMessageContainingAll(messages)
                .hasCauseInstanceOf(DataCloudJDBCException.class)
                .hasRootCauseInstanceOf(AuthorizationException.class);
    }

    private static void assertSQLException(Throwable actual, CharSequence... messages) {
        AssertionsForClassTypes.assertThat(actual)
                .hasMessageContainingAll(messages)
                .hasCauseInstanceOf(DataCloudJDBCException.class)
                .hasRootCauseInstanceOf(SQLException.class);
    }
}
