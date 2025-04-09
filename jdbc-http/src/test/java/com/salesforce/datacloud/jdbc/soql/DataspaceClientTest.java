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
package com.salesforce.datacloud.jdbc.soql;

import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.List;
import java.util.Properties;
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
    TokenProcessor tokenProcessor;

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
            Mockito.when(tokenProcessor.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));

            val client = new DataspaceClient(new Properties(), tokenProcessor);

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
            assertThat(actualRequest.getHeader("User-Agent")).isEqualTo("cdp/jdbc");
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
            Mockito.when(tokenProcessor.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));
            val client = new DataspaceClient(new Properties(), tokenProcessor);

            server.enqueue(new MockResponse().setResponseCode(500));
            Assertions.assertThrows(DataCloudJDBCException.class, client::get);
        }
    }
}
