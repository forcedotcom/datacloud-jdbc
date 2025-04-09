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
