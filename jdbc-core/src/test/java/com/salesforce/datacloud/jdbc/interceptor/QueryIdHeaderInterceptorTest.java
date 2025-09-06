/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.grpc.Metadata;
import java.util.UUID;
import lombok.val;
import org.junit.jupiter.api.Test;

class QueryIdHeaderInterceptorTest {
    @Test
    void appliesQueryIdToHeaders() {
        val key = Metadata.Key.of("x-hyperdb-query-id", ASCII_STRING_MARSHALLER);
        val id = UUID.randomUUID().toString();
        val interceptor = new QueryIdHeaderInterceptor(id);
        val headers = new Metadata();
        interceptor.mutate(headers);
        assertThat(headers.get(key)).isEqualTo(id);
    }
}
