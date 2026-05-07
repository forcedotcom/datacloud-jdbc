/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.datacloud.jdbc.auth.DataCloudToken;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import com.salesforce.datacloud.jdbc.auth.DirectCdpTokenProcessor;
import java.sql.SQLException;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;

class TokenProcessorSupplierTest {

    private static final String FAKE_TOKEN =
            "eyJraWQiOiJDT1JFLjAwRE9LMDAwMDAwOVp6ci4xNzE4MDUyMTU0NDIyIiwidHlwIjoiSldUIiwiYWxnIjoiRVMyNTYifQ.eyJzdWIiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS9pZC8wMERPSzAwMDAwMDlaenIyQUUvMDA1T0swMDAwMDBVeTkxWUFDIiwic2NwIjoiY2RwX3Byb2ZpbGVfYXBpIGNkcF9pbmdlc3RfYXBpIGNkcF9pZGVudGl0eXJlc29sdXRpb25fYXBpIGNkcF9zZWdtZW50X2FwaSBjZHBfcXVlcnlfYXBpIGNkcF9hcGkiLCJpc3MiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS8iLCJvcmdJZCI6IjAwRE9LMDAwMDAwOVp6ciIsImlzc3VlclRlbmFudElkIjoiY29yZS9mYWxjb250ZXN0MS1jb3JlNG9yYTE1LzAwRE9LMDAwMDAwOVp6cjJBRSIsInNmYXBwaWQiOiIzTVZHOVhOVDlUbEI3VmtZY0tIVm5sUUZzWEd6cUJuMGszUC5zNHJBU0I5V09oRU1OdkgyNzNpM1NFRzF2bWl3WF9YY2NXOUFZbHA3VnJnQ3BGb0ZXIiwiYXVkaWVuY2VUZW5hbnRJZCI6ImEzNjAvZmFsY29uZGV2L2E2ZDcyNmE3M2Y1MzQzMjdhNmE4ZTJlMGYzY2MzODQwIiwiY3VzdG9tX2F0dHJpYnV0ZXMiOnsiZGF0YXNwYWNlIjoiZGVmYXVsdCJ9LCJhdWQiOiJhcGkuYTM2MC5zYWxlc2ZvcmNlLmNvbSIsIm5iZiI6MTcyMDczMTAyMSwic2ZvaWQiOiIwMERPSzAwMDAwMDlaenIiLCJzZnVpZCI6IjAwNU9LMDAwMDAwVXk5MSIsImV4cCI6MTcyMDczODI4MCwiaWF0IjoxNzIwNzMxMDgxLCJqdGkiOiIwYjYwMzc4OS1jMGI2LTQwZTMtYmIzNi03NDQ3MzA2MzAxMzEifQ.lXgeAhJIiGoxgNpBi0W5oBWyn2_auB2bFxxajGuK6DMHlkqDhHJAlFN_uf6QPSjGSJCh5j42Ow5SrEptUDJwmQ";
    private static final String FAKE_TENANT_ID = "a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840";
    private static final String TENANT_URL = "https://test.c360a.salesforce.com";

    @Test
    void delegatesToDirectCdpTokenProcessor() throws Exception {
        val props = new Properties();
        props.setProperty("cdpToken", FAKE_TOKEN);
        props.setProperty("tenantUrl", TENANT_URL);
        val processor = DirectCdpTokenProcessor.ofDestructive(props);

        val supplier = new TokenProcessorSupplier(processor);

        assertThat(supplier.getToken()).isEqualTo("Bearer " + FAKE_TOKEN);
        assertThat(supplier.getAudience()).isEqualTo(FAKE_TENANT_ID);
    }

    @Test
    void delegatesToDataCloudTokenProvider() throws Exception {
        val token = mock(DataCloudToken.class);
        when(token.getAccessToken()).thenReturn("Bearer " + FAKE_TOKEN);
        when(token.getTenantId()).thenReturn(FAKE_TENANT_ID);

        val provider = mock(DataCloudTokenProvider.class);
        when(provider.getDataCloudToken()).thenReturn(token);

        val supplier = new TokenProcessorSupplier(provider);

        assertThat(supplier.getToken()).isEqualTo("Bearer " + FAKE_TOKEN);
        assertThat(supplier.getAudience()).isEqualTo(FAKE_TENANT_ID);
    }

    @Test
    void delegatesToCustomSupplier() throws Exception {
        val token = mock(DataCloudToken.class);
        when(token.getAccessToken()).thenReturn("Bearer custom");
        when(token.getTenantId()).thenReturn("custom-tenant");

        val supplier = new TokenProcessorSupplier(() -> token);

        assertThat(supplier.getToken()).isEqualTo("Bearer custom");
        assertThat(supplier.getAudience()).isEqualTo("custom-tenant");
    }

    @Test
    void getTokenPropagatesSupplierException() {
        val supplier = new TokenProcessorSupplier(() -> {
            throw new SQLException("supplier failed");
        });

        assertThatThrownBy(supplier::getToken).isInstanceOf(SQLException.class).hasMessage("supplier failed");
    }

    @Test
    void getAudiencePropagatesSupplierException() {
        val supplier = new TokenProcessorSupplier(() -> {
            throw new SQLException("supplier failed");
        });

        assertThatThrownBy(supplier::getAudience)
                .isInstanceOf(SQLException.class)
                .hasMessage("supplier failed");
    }
}
