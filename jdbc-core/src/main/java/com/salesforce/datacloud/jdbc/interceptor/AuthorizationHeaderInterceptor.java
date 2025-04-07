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

import static com.salesforce.datacloud.jdbc.interceptor.MetadataUtilities.keyOf;

import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import io.grpc.Metadata;
import java.sql.SQLException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AuthorizationHeaderInterceptor implements HeaderMutatingClientInterceptor {

    @FunctionalInterface
    public interface TokenSupplier {
        String getToken() throws SQLException;

        default String getAudience() {
            return null;
        }
    }

    public static AuthorizationHeaderInterceptor of(TokenProcessor tokenProcessor) {
        val supplier = new TokenProcessorSupplier(tokenProcessor);
        return new AuthorizationHeaderInterceptor(supplier, "oauth");
    }

    public static AuthorizationHeaderInterceptor of(TokenSupplier supplier) {
        return new AuthorizationHeaderInterceptor(supplier, "custom");
    }

    private static final String AUTH = "Authorization";
    private static final String AUD = "audience";

    private static final Metadata.Key<String> AUTH_KEY = keyOf(AUTH);
    private static final Metadata.Key<String> AUD_KEY = keyOf(AUD);

    @ToString.Exclude
    private final TokenSupplier tokenSupplier;

    private final String name;

    @SneakyThrows
    @Override
    public void mutate(final Metadata headers) {
        val token = tokenSupplier.getToken();
        headers.put(AUTH_KEY, token);

        val audience = tokenSupplier.getAudience();
        if (audience != null) {
            headers.put(AUD_KEY, audience);
        }
    }

    @AllArgsConstructor
    static class TokenProcessorSupplier implements TokenSupplier {
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
}
