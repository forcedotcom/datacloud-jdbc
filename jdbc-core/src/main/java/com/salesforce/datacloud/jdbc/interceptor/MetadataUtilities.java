/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;

public final class MetadataUtilities {
    private MetadataUtilities() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static Metadata.Key<String> keyOf(String key) {
        return Metadata.Key.of(key, ASCII_STRING_MARSHALLER);
    }
}
