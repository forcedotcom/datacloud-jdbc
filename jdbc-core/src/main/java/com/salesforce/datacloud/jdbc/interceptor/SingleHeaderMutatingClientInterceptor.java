/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import io.grpc.Metadata;
import lombok.NonNull;

public interface SingleHeaderMutatingClientInterceptor extends HeaderMutatingClientInterceptor {
    @NonNull Metadata.Key<String> getKey();

    @NonNull String getValue();

    default void mutate(final Metadata headers) {
        headers.put(getKey(), getValue());
    }
}
