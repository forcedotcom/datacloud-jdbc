/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@AllArgsConstructor
public class TokenProcessorSupplier implements AuthorizationHeaderInterceptor.TokenProvider {
    private final DataCloudTokenProvider tokenProvider;

    @SneakyThrows
    @Override
    public String getToken() {
        val token = tokenProvider.getDataCloudToken();
        return token.getAccessToken();
    }

    @SneakyThrows
    @Override
    public String getAudience() {
        val token = tokenProvider.getDataCloudToken();
        return token.getTenantId();
    }
}
