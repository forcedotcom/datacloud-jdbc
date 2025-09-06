/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
