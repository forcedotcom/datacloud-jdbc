/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static com.salesforce.datacloud.jdbc.interceptor.MetadataUtilities.keyOf;

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
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class AuthorizationHeaderInterceptor implements HeaderMutatingClientInterceptor {

    @FunctionalInterface
    public interface TokenSupplier {
        String getToken() throws SQLException;

        default String getAudience() {
            return null;
        }
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
}
