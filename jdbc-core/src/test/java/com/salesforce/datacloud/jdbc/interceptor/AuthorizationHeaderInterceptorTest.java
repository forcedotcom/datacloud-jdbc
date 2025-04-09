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

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import java.sql.SQLException;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthorizationHeaderInterceptorTest {
    private static final String AUTH = "Authorization";
    private static final String AUD = "audience";

    private static final Metadata.Key<String> AUTH_KEY = Metadata.Key.of(AUTH, ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUD_KEY = Metadata.Key.of(AUD, ASCII_STRING_MARSHALLER);

    @SneakyThrows
    @Test
    void interceptorCallsGetDataCloudTokenTwice() {
        val token = UUID.randomUUID().toString();
        val aud = UUID.randomUUID().toString();

        val sut = sut(token, aud);
        val metadata = new Metadata();

        sut.mutate(metadata);

        assertThat(metadata.get(AUTH_KEY)).isEqualTo(token);
        assertThat(metadata.get(AUD_KEY)).isEqualTo(aud);
    }

    @SneakyThrows
    @Test
    void interceptorIgnoresNullAudience() {
        val sut = sut("", null);
        val metadata = new Metadata();

        sut.mutate(metadata);

        assertThat(metadata.get(AUD_KEY)).isNull();
    }

    private AuthorizationHeaderInterceptor sut(String token, String aud) {
        val supplier = new AuthorizationHeaderInterceptor.TokenSupplier() {

            @Override
            public String getToken() throws SQLException {
                return token;
            }

            @Override
            public String getAudience() {
                return aud;
            }
        };
        return AuthorizationHeaderInterceptor.of(supplier);
    }
}
