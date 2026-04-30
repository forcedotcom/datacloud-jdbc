/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DirectCdpTokenProcessorTest {

    private static final String TENANT_URL = "https://test.c360a.salesforce.com";
    static final String FAKE_TOKEN =
            "eyJraWQiOiJDT1JFLjAwRE9LMDAwMDAwOVp6ci4xNzE4MDUyMTU0NDIyIiwidHlwIjoiSldUIiwiYWxnIjoiRVMyNTYifQ.eyJzdWIiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS9pZC8wMERPSzAwMDAwMDlaenIyQUUvMDA1T0swMDAwMDBVeTkxWUFDIiwic2NwIjoiY2RwX3Byb2ZpbGVfYXBpIGNkcF9pbmdlc3RfYXBpIGNkcF9pZGVudGl0eXJlc29sdXRpb25fYXBpIGNkcF9zZWdtZW50X2FwaSBjZHBfcXVlcnlfYXBpIGNkcF9hcGkiLCJpc3MiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS8iLCJvcmdJZCI6IjAwRE9LMDAwMDAwOVp6ciIsImlzc3VlclRlbmFudElkIjoiY29yZS9mYWxjb250ZXN0MS1jb3JlNG9yYTE1LzAwRE9LMDAwMDAwOVp6cjJBRSIsInNmYXBwaWQiOiIzTVZHOVhOVDlUbEI3VmtZY0tIVm5sUUZzWEd6cUJuMGszUC5zNHJBU0I5V09oRU1OdkgyNzNpM1NFRzF2bWl3WF9YY2NXOUFZbHA3VnJnQ3BGb0ZXIiwiYXVkaWVuY2VUZW5hbnRJZCI6ImEzNjAvZmFsY29uZGV2L2E2ZDcyNmE3M2Y1MzQzMjdhNmE4ZTJlMGYzY2MzODQwIiwiY3VzdG9tX2F0dHJpYnV0ZXMiOnsiZGF0YXNwYWNlIjoiZGVmYXVsdCJ9LCJhdWQiOiJhcGkuYTM2MC5zYWxlc2ZvcmNlLmNvbSIsIm5iZiI6MTcyMDczMTAyMSwic2ZvaWQiOiIwMERPSzAwMDAwMDlaenIiLCJzZnVpZCI6IjAwNU9LMDAwMDAwVXk5MSIsImV4cCI6MTcyMDczODI4MCwiaWF0IjoxNzIwNzMxMDgxLCJqdGkiOiIwYjYwMzc4OS1jMGI2LTQwZTMtYmIzNi03NDQ3MzA2MzAxMzEifQ.lXgeAhJIiGoxgNpBi0W5oBWyn2_auB2bFxxajGuK6DMHlkqDhHJAlFN_uf6QPSjGSJCh5j42Ow5SrEptUDJwmQ";
    static final String FAKE_TENANT_ID = "a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840";

    private static Properties propertiesForCdpToken(String cdpToken, String tenantUrl) {
        Properties properties = new Properties();
        properties.setProperty("cdpToken", cdpToken);
        properties.setProperty("tenantUrl", tenantUrl);
        return properties;
    }

    @Test
    void getDataCloudTokenReturnsValidToken() throws SQLException {
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(FAKE_TOKEN, TENANT_URL));
        DataCloudToken token = processor.getDataCloudToken();

        assertThat(token.getAccessToken()).isEqualTo("Bearer " + FAKE_TOKEN);
        assertThat(token.getTenantUrl()).isEqualTo(TENANT_URL);
        assertThat(token.getTenantId()).isEqualTo(FAKE_TENANT_ID);
        assertThat(token.isAlive()).isTrue();
    }

    @Test
    void getDataCloudTokenReturnsCachedToken() throws SQLException {
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(FAKE_TOKEN, TENANT_URL));
        DataCloudToken first = processor.getDataCloudToken();
        DataCloudToken second = processor.getDataCloudToken();

        assertThat(first.getAccessToken()).isEqualTo(second.getAccessToken());
    }

    @Test
    void getLakehouseWithoutDataspace() throws SQLException {
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(FAKE_TOKEN, TENANT_URL));
        String lakehouse = processor.getLakehouse();

        assertThat(lakehouse).isEqualTo("lakehouse:" + FAKE_TENANT_ID + ";");
    }

    @Test
    void getLakehouseWithDataspace() throws SQLException {
        String dataspace = UUID.randomUUID().toString();
        Properties props = propertiesForCdpToken(FAKE_TOKEN, TENANT_URL);
        props.setProperty("dataspace", dataspace);

        DirectCdpTokenProcessor processor = DirectCdpTokenProcessor.ofDestructive(props);
        String lakehouse = processor.getLakehouse();

        assertThat(lakehouse).isEqualTo("lakehouse:" + FAKE_TENANT_ID + ";" + dataspace);
    }

    @Test
    void ofDestructiveThrowsWhenCdpTokenMissing() {
        Properties props = new Properties();
        props.setProperty("tenantUrl", TENANT_URL);

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("cdpToken");
    }

    @Test
    void ofDestructiveThrowsWhenTenantUrlMissing() {
        Properties props = new Properties();
        props.setProperty("cdpToken", FAKE_TOKEN);

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("tenantUrl");
    }

    @Test
    void ofDestructiveThrowsWhenCdpTokenIsInvalidJwt() {
        Properties props = propertiesForCdpToken("not-a-valid-jwt", TENANT_URL);

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("Invalid CDP token");
    }

    @Test
    void ofDestructiveRemovesPropertiesFromInput() throws SQLException {
        Properties props = propertiesForCdpToken(FAKE_TOKEN, TENANT_URL);
        props.setProperty("dataspace", "myspace");

        DirectCdpTokenProcessor.ofDestructive(props);

        assertThat(props.containsKey("cdpToken")).isFalse();
        assertThat(props.containsKey("tenantUrl")).isFalse();
        assertThat(props.containsKey("dataspace")).isFalse();
    }

    @Test
    void hasCdpTokenReturnsTrueWhenBothPresent() {
        Properties props = propertiesForCdpToken(FAKE_TOKEN, TENANT_URL);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(props)).isTrue();
    }

    @Test
    void hasCdpTokenReturnsFalseWhenMissing() {
        assertThat(DirectCdpTokenProcessor.hasCdpToken(new Properties())).isFalse();
        assertThat(DirectCdpTokenProcessor.hasCdpToken(null)).isFalse();

        Properties onlyCdpToken = new Properties();
        onlyCdpToken.setProperty("cdpToken", FAKE_TOKEN);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyCdpToken)).isFalse();

        Properties onlyTenantUrl = new Properties();
        onlyTenantUrl.setProperty("tenantUrl", TENANT_URL);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyTenantUrl)).isFalse();
    }
}
