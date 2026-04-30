/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import com.salesforce.datacloud.jdbc.auth.DataCloudToken;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import com.salesforce.datacloud.jdbc.auth.DirectCdpTokenProcessor;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@AllArgsConstructor
public class TokenProcessorSupplier implements AuthorizationHeaderInterceptor.TokenProvider {
    private final ThrowingJdbcSupplier<DataCloudToken> tokenSupplier;

    public TokenProcessorSupplier(DataCloudTokenProvider tokenProvider) {
        this(tokenProvider::getDataCloudToken);
    }

    public TokenProcessorSupplier(DirectCdpTokenProcessor cdpTokenProcessor) {
        this(cdpTokenProcessor::getDataCloudToken);
    }

    @SneakyThrows
    @Override
    public String getToken() {
        val token = tokenSupplier.get();
        return token.getAccessToken();
    }

    @SneakyThrows
    @Override
    public String getAudience() {
        val token = tokenSupplier.get();
        return token.getTenantId();
    }
}
