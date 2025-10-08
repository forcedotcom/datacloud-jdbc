/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.auth.DataCloudTokenTest.FAKE_TENANT_ID;
import static com.salesforce.datacloud.jdbc.auth.DataCloudTokenTest.FAKE_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.errors.AuthorizationException;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.http.HttpClientProperties;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
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
class DataCloudTokenProviderTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    static Properties propertiesForPassword(String userName, String password) {
        val properties = new Properties();
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_ID, "clientId");
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_SECRET, "clientSecret");
        properties.setProperty(SalesforceAuthProperties.AUTH_USER_NAME, userName);
        properties.setProperty(SalesforceAuthProperties.AUTH_PASSWORD, password);
        return properties;
    }

    static Properties propertiesForPrivateKey(String userName, String privateKey) {
        val properties = new Properties();
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_ID, "clientId");
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_SECRET, "clientSecret");
        properties.setProperty(SalesforceAuthProperties.AUTH_USER_NAME, userName);
        properties.setProperty(SalesforceAuthProperties.AUTH_PRIVATE_KEY, privateKey);
        return properties;
    }

    static Properties propertiesForRefreshToken(String refreshToken) {
        val properties = new Properties();
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_ID, "clientId");
        properties.setProperty(SalesforceAuthProperties.AUTH_CLIENT_SECRET, "clientSecret");
        properties.setProperty(SalesforceAuthProperties.AUTH_REFRESH_TOKEN, refreshToken);
        return properties;
    }

    // Valid RSA private key in PEM format for testing
    private static final String FAKE_PRIVATE_KEY = SalesforceAuthPropertiesTest.FAKE_PRIVATE_KEY;

    @SneakyThrows
    @Test
    void retryPolicyRetriesExpectedNumberOfTimesThenGivesUp() {
        val properties = propertiesForPassword("un", "pw");
        properties.setProperty(HttpClientProperties.HTTP_MAX_RETRIES, "3");
        val expectedTriesCount = 4;
        try (val server = new MockWebServer()) {
            server.start();
            for (int x = 0; x < expectedTriesCount; x++) {
                server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
            }
            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
                    .getOAuthToken());
            assertThat(server.getRequestCount()).isEqualTo(expectedTriesCount);
            server.shutdown();
        }
    }

    @SneakyThrows
    @Test
    void retryPolicyDoesntRetryOnAuthorizationException() {
        val properties = propertiesForPassword("un", "pw");
        try (val server = new MockWebServer()) {
            server.start();
            // Simulate an HTTP 401 Unauthorized response to trigger an AuthorizationException
            for (int i = 0; i < 3; i++) {
                server.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"invalid_client\"}"));
            }
            server.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"invalid_client\"}"));
            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
                    .getOAuthToken());
            assertThat(server.getRequestCount()).isEqualTo(1);
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
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val actual =
                    DataCloudTokenProvider.of(clientProperties, authProperties).getOAuthToken();
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
        properties.remove(SalesforceAuthProperties.AUTH_DATASPACE);
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setExpiresIn(60000);
            dataCloudTokenResponse.setToken(FAKE_TOKEN);
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            val expected = DataCloudToken.of(dataCloudTokenResponse);

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);

            val processor = DataCloudTokenProvider.of(clientProperties, authProperties);
            assertThat(processor.getLakehouseName()).as("lakehouse").isEqualTo("lakehouse:" + FAKE_TENANT_ID + ";");

            val actual = processor.getDataCloudToken();
            assertThat(actual.getAccessToken()).as("access token").isEqualTo(expected.getAccessToken());
            assertThat(actual.getTenantUrl()).as("tenant url").isEqualTo(expected.getTenantUrl());
            assertThat(actual.getTenantId()).as("tenant id").isEqualTo(FAKE_TENANT_ID);
        }
    }

    @SneakyThrows
    @Test
    void bothTokensRetrievedWithLakehouseAndDataspace() {
        val mapper = new ObjectMapper();
        val dataspace = UUID.randomUUID().toString();
        val properties = propertiesForPassword("un", "pw");
        properties.put(SalesforceAuthProperties.AUTH_DATASPACE, dataspace);
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setToken(FAKE_TOKEN);
            dataCloudTokenResponse.setExpiresIn(60000);
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            val expected = DataCloudToken.of(dataCloudTokenResponse);

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val processor = DataCloudTokenProvider.of(clientProperties, authProperties);
            assertThat(processor.getLakehouseName())
                    .as("lakehouse")
                    .isEqualTo("lakehouse:" + FAKE_TENANT_ID + ";" + dataspace);

            val actual = processor.getDataCloudToken();
            assertThat(actual.getAccessToken()).as("access token").isEqualTo(expected.getAccessToken());
            assertThat(actual.getTenantUrl()).as("tenant url").isEqualTo(expected.getTenantUrl());
            assertThat(actual.getTenantId()).as("tenant id").isEqualTo(FAKE_TENANT_ID);
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseContainsErrorDescription() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(HttpClientProperties.HTTP_MAX_RETRIES, "0");
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

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val ex = assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
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
        properties.put(HttpClientProperties.HTTP_MAX_RETRIES, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val ex = assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
                    .getDataCloudToken());

            assertSQLException(ex, "Received an error when acquiring oauth access token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseIsMissingAccessToken() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(HttpClientProperties.HTTP_MAX_RETRIES, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val ex = assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
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
        properties.put(HttpClientProperties.HTTP_MAX_RETRIES, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());

            server.enqueue(new MockResponse().setBody("{}"));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val ex = assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
                    .getDataCloudToken());

            assertSQLException(ex, "Received an error when acquiring oauth access token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void throwsExceptionWhenDataCloudTokenResponseIsNull() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        properties.put(HttpClientProperties.HTTP_MAX_RETRIES, "0");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody("{}"));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val ex = assertThrows(SQLException.class, () -> DataCloudTokenProvider.of(clientProperties, authProperties)
                    .getDataCloudToken());

            assertSQLException(
                    ex,
                    "Received an error when exchanging oauth access token for data cloud token, no token in response.");
        }
    }

    @SneakyThrows
    @Test
    void testPrivateKeyAuthenticationSuccess() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPrivateKey("testuser", FAKE_PRIVATE_KEY);
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        oAuthTokenResponse.setToken(accessToken);

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val actual =
                    DataCloudTokenProvider.of(clientProperties, authProperties).getOAuthToken();

            assertThat(actual.getToken()).as("access token").isEqualTo(accessToken);
            assertThat(actual.getInstanceUrl().toString())
                    .as("instance url")
                    .isEqualTo(server.url("").toString());
        }
    }

    @SneakyThrows
    @Test
    void testRefreshTokenAuthenticationSuccess() {
        val mapper = new ObjectMapper();
        val refreshToken = UUID.randomUUID().toString();
        val properties = propertiesForRefreshToken(refreshToken);
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        oAuthTokenResponse.setToken(accessToken);

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            val actual =
                    DataCloudTokenProvider.of(clientProperties, authProperties).getOAuthToken();

            assertThat(actual.getToken()).as("access token").isEqualTo(accessToken);
            assertThat(actual.getInstanceUrl().toString())
                    .as("instance url")
                    .isEqualTo(server.url("").toString());
        }
    }

    // Also includes the token exchange flow
    @SneakyThrows
    @Test
    void testPasswordBasedDataCloudTokenFlow() {
        val mapper = new ObjectMapper();
        val properties = propertiesForPassword("un", "pw");
        val oAuthTokenResponse = new OAuthTokenResponse();
        oAuthTokenResponse.setToken(UUID.randomUUID().toString());

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            val dataCloudTokenResponse = new DataCloudTokenResponse();
            dataCloudTokenResponse.setTokenType(UUID.randomUUID().toString());
            dataCloudTokenResponse.setExpiresIn(60000);
            dataCloudTokenResponse.setToken(FAKE_TOKEN);
            dataCloudTokenResponse.setInstanceUrl(server.url("").toString());
            val expected = DataCloudToken.of(dataCloudTokenResponse);

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(oAuthTokenResponse)));
            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataCloudTokenResponse)));

            val loginUrl = server.url("").uri();
            HttpClientProperties clientProperties = HttpClientProperties.ofDestructive(properties);
            SalesforceAuthProperties authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);

            val processor = DataCloudTokenProvider.of(clientProperties, authProperties);
            val actual = processor.getDataCloudToken();

            assertThat(actual.getAccessToken()).as("access token").isEqualTo(expected.getAccessToken());
            assertThat(actual.getTenantUrl()).as("tenant url").isEqualTo(expected.getTenantUrl());
            assertThat(actual.getTenantId()).as("tenant id").isEqualTo(FAKE_TENANT_ID);
        }
    }

    private static void assertAuthorizationException(Throwable actual, CharSequence... messages) {
        AssertionsForClassTypes.assertThat(actual)
                .isInstanceOf(SQLException.class)
                .hasMessageContainingAll(messages)
                .hasRootCauseInstanceOf(AuthorizationException.class);
    }

    private static void assertSQLException(Throwable actual, CharSequence... messages) {
        AssertionsForClassTypes.assertThat(actual)
                .isInstanceOf(SQLException.class)
                .hasMessageContainingAll(messages)
                .hasRootCauseInstanceOf(SQLException.class);
    }
}
