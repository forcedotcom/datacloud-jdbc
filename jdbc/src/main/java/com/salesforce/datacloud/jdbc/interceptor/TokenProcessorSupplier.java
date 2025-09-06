/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TokenProcessorSupplier implements AuthorizationHeaderInterceptor.TokenSupplier {
    public static AuthorizationHeaderInterceptor of(TokenProcessor tokenProcessor) {
        val supplier = new TokenProcessorSupplier(tokenProcessor);
        return new AuthorizationHeaderInterceptor(supplier, "oauth");
    }

    private final TokenProcessor tokenProcessor;

    @SneakyThrows
    @Override
    public String getToken() {
        val token = tokenProcessor.getDataCloudToken();
        return token.getAccessToken();
    }

    @SneakyThrows
    @Override
    public String getAudience() {
        val token = tokenProcessor.getDataCloudToken();
        return token.getTenantId();
    }
}
