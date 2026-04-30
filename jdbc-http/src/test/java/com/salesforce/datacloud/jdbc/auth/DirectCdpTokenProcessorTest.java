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
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForCdpToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

class DirectCdpTokenProcessorTest {

    private static final String TENANT_URL = "https://test.c360a.salesforce.com";

    @SneakyThrows
    @Test
    void getDataCloudTokenReturnsValidToken() {
        val processor = DirectCdpTokenProcessor.of(propertiesForCdpToken(fakeToken, TENANT_URL));
        val token = processor.getDataCloudToken();

        assertThat(token.getAccessToken()).isEqualTo("Bearer " + fakeToken);
        assertThat(token.getTenantUrl()).isEqualTo(TENANT_URL);
        assertThat(token.getTenantId()).isEqualTo(fakeTenantId);
        assertThat(token.isAlive()).isTrue();
    }

    @SneakyThrows
    @Test
    void getDataCloudTokenReturnsCachedToken() {
        val processor = DirectCdpTokenProcessor.of(propertiesForCdpToken(fakeToken, TENANT_URL));
        val first = processor.getDataCloudToken();
        val second = processor.getDataCloudToken();

        assertThat(first.getAccessToken()).isEqualTo(second.getAccessToken());
    }

    @Test
    void getOAuthTokenThrows() {
        val processor = assertDoesNotThrowOnCreate();
        val ex = assertThrows(DataCloudJDBCException.class, processor::getOAuthToken);

        assertThat(ex.getMessage())
                .contains("OAuth token is not available when using direct CDP token authentication");
    }

    @SneakyThrows
    @Test
    void getLakehouseWithoutDataspace() {
        val processor = DirectCdpTokenProcessor.of(propertiesForCdpToken(fakeToken, TENANT_URL));
        val lakehouse = processor.getLakehouse();

        assertThat(lakehouse).isEqualTo("lakehouse:" + fakeTenantId + ";");
    }

    @SneakyThrows
    @Test
    void getLakehouseWithDataspace() {
        val dataspace = UUID.randomUUID().toString();
        val props = propertiesForCdpToken(fakeToken, TENANT_URL);
        props.setProperty("dataspace", dataspace);

        val processor = DirectCdpTokenProcessor.of(props);
        val lakehouse = processor.getLakehouse();

        assertThat(lakehouse).isEqualTo("lakehouse:" + fakeTenantId + ";" + dataspace);
    }

    @Test
    void ofThrowsWhenCdpTokenMissing() {
        val props = new Properties();
        props.setProperty("tenantUrl", TENANT_URL);

        val ex = assertThrows(DataCloudJDBCException.class, () -> DirectCdpTokenProcessor.of(props));
        assertThat(ex.getMessage()).contains("cdpToken");
    }

    @Test
    void ofThrowsWhenTenantUrlMissing() {
        val props = new Properties();
        props.setProperty("cdpToken", fakeToken);

        val ex = assertThrows(DataCloudJDBCException.class, () -> DirectCdpTokenProcessor.of(props));
        assertThat(ex.getMessage()).contains("tenantUrl");
    }

    @Test
    void hasCdpTokenReturnsTrueWhenBothPresent() {
        val props = propertiesForCdpToken(fakeToken, TENANT_URL);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(props)).isTrue();
    }

    @Test
    void hasCdpTokenReturnsFalseWhenMissing() {
        assertThat(DirectCdpTokenProcessor.hasCdpToken(new Properties())).isFalse();

        val onlyCdpToken = new Properties();
        onlyCdpToken.setProperty("cdpToken", fakeToken);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyCdpToken)).isFalse();

        val onlyTenantUrl = new Properties();
        onlyTenantUrl.setProperty("tenantUrl", TENANT_URL);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyTenantUrl)).isFalse();
    }

    @Test
    void ofThrowsWhenCdpTokenIsInvalidJwt() {
        val props = propertiesForCdpToken("not-a-valid-jwt", TENANT_URL);

        val ex = assertThrows(DataCloudJDBCException.class, () -> DirectCdpTokenProcessor.of(props));
        assertThat(ex.getMessage()).contains("Invalid CDP token");
    }

    @Test
    void getSettingsReturnsNull() {
        val processor = assertDoesNotThrowOnCreate();
        assertThat(processor.getSettings()).isNull();
    }

    @SneakyThrows
    private DirectCdpTokenProcessor assertDoesNotThrowOnCreate() {
        return DirectCdpTokenProcessor.of(propertiesForCdpToken(fakeToken, TENANT_URL));
    }
}
