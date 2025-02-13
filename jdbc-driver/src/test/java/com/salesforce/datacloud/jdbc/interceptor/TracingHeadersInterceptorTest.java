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
