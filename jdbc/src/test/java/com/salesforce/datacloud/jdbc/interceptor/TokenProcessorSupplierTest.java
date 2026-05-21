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
import lombok.val;
import org.junit.jupiter.api.Test;

class TokenProcessorSupplierTest {

    private static final String ACCESS_TOKEN = "Bearer fake-access-token";
    private static final String TENANT_ID = "fake-tenant-id";

    private static DataCloudToken stubToken() throws SQLException {
        val token = mock(DataCloudToken.class);
        when(token.getAccessToken()).thenReturn(ACCESS_TOKEN);
        when(token.getTenantId()).thenReturn(TENANT_ID);
        return token;
    }

    @Test
    void delegatesToDirectCdpTokenProcessor() throws Exception {
        val token = stubToken();
        val processor = mock(DirectCdpTokenProcessor.class);
        when(processor.getDataCloudToken()).thenReturn(token);

        val supplier = new TokenProcessorSupplier(processor);

        assertThat(supplier.getToken()).isEqualTo(ACCESS_TOKEN);
        assertThat(supplier.getAudience()).isEqualTo(TENANT_ID);
    }

    @Test
    void delegatesToDataCloudTokenProvider() throws Exception {
        val token = stubToken();
        val provider = mock(DataCloudTokenProvider.class);
        when(provider.getDataCloudToken()).thenReturn(token);

        val supplier = new TokenProcessorSupplier(provider);

        assertThat(supplier.getToken()).isEqualTo(ACCESS_TOKEN);
        assertThat(supplier.getAudience()).isEqualTo(TENANT_ID);
    }

    @Test
    void delegatesToCustomSupplier() throws Exception {
        val token = stubToken();
        val supplier = new TokenProcessorSupplier(() -> token);

        assertThat(supplier.getToken()).isEqualTo(ACCESS_TOKEN);
        assertThat(supplier.getAudience()).isEqualTo(TENANT_ID);
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
