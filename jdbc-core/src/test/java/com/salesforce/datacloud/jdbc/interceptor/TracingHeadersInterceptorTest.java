/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.grpc.Metadata;
import lombok.val;
import org.junit.jupiter.api.Test;

class TracingHeadersInterceptorTest {
    private static final TracingHeadersInterceptor sut = TracingHeadersInterceptor.of();

    private static final Metadata.Key<String> trace = Metadata.Key.of("x-b3-traceid", ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> span = Metadata.Key.of("x-b3-spanid", ASCII_STRING_MARSHALLER);

    @Test
    void itAppliesIdsFromTracerToHeaders() {
        val metadata = new Metadata();

        sut.mutate(metadata);

        val traceA = metadata.get(trace);
        val spanA = metadata.get(span);

        sut.mutate(metadata);

        val traceB = metadata.get(trace);
        val spanB = metadata.get(span);

        assertThat(traceA).isNotBlank();
        assertThat(traceB).isNotBlank();
        assertThat(traceA).isEqualTo(traceB);

        assertThat(spanA).isNotBlank();
        assertThat(spanB).isNotBlank();
        assertThat(spanA).isNotEqualTo(spanB);
    }
}
