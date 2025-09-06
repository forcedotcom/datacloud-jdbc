/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static com.salesforce.datacloud.jdbc.interceptor.MetadataUtilities.keyOf;

import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import io.grpc.Metadata;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class QueryIdHeaderInterceptor implements SingleHeaderMutatingClientInterceptor {
    @ToString.Exclude
    public final Metadata.Key<String> key = keyOf("x-hyperdb-query-id");

    @NonNull private final String value;

    @Override
    public void mutate(final Metadata headers) {
        if (StringCompatibility.isBlank(value)) {
            return;
        }

        headers.put(key, value);
    }
}
