/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.soql;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.HttpClientProperties;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataspaceClientTest {
    @Mock
    DataCloudTokenProvider tokenProvider;

    private String randomString() {
        return UUID.randomUUID().toString();
    }

    @SneakyThrows
    @Test
    public void testGetDataspaces() {
        val mapper = new ObjectMapper();
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        val dataspaceAttributeName = randomString();
        oAuthTokenResponse.setToken(accessToken);
        val dataspaceResponse = new DataspaceResponse();
        val dataspaceAttributes = new DataspaceResponse.DataSpaceAttributes();
        dataspaceAttributes.setName(dataspaceAttributeName);
        dataspaceResponse.setRecords(ImmutableList.of(dataspaceAttributes));

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            Mockito.when(tokenProvider.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));

            val client = new DataspaceClient(HttpClientProperties.defaultProperties(), tokenProvider);

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataspaceResponse)));
            val actual = client.get();
            List<String> expected = ImmutableList.of(dataspaceAttributeName);
            assertThat(actual).isEqualTo(expected);

            val actualRequest = server.takeRequest();
            val query = "SELECT+name+from+Dataspace";
            assertThat(actualRequest.getMethod()).isEqualTo("GET");
            assertThat(actualRequest.getRequestUrl()).isEqualTo(server.url("services/data/v61.0/query/?q=" + query));
            assertThat(actualRequest.getBody().readUtf8()).isBlank();
            assertThat(actualRequest.getHeader("Authorization")).isEqualTo("Bearer " + accessToken);
            assertThat(actualRequest.getHeader("Content-Type")).isEqualTo("application/json");
            assertThat(actualRequest.getHeader("User-Agent")).startsWith("salesforce-datacloud-jdbc/");
            assertThat(actualRequest.getHeader("enable-stream-flow")).isEqualTo("false");
        }
    }

    @SneakyThrows
    @Test
    public void testGetDataspacesThrowsExceptionWhenCallFails() {
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        val dataspaceAttributeName = randomString();
        oAuthTokenResponse.setToken(accessToken);
        val dataspaceResponse = new DataspaceResponse();
        val dataspaceAttributes = new DataspaceResponse.DataSpaceAttributes();
        dataspaceAttributes.setName(dataspaceAttributeName);
        dataspaceResponse.setRecords(ImmutableList.of(dataspaceAttributes));

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            Mockito.when(tokenProvider.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));
            val client = new DataspaceClient(HttpClientProperties.defaultProperties(), tokenProvider);

            server.enqueue(new MockResponse().setResponseCode(500));
            Assertions.assertThrows(DataCloudJDBCException.class, client::get);
        }
    }
}
