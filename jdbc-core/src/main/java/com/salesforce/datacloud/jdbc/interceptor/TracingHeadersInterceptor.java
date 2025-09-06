/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import static com.salesforce.datacloud.jdbc.interceptor.MetadataUtilities.keyOf;

import com.salesforce.datacloud.jdbc.tracing.Tracer;
import io.grpc.Metadata;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class TracingHeadersInterceptor implements HeaderMutatingClientInterceptor {
    public static TracingHeadersInterceptor of() {
        val tracer = Tracer.get();
        val traceId = tracer.nextId();
        log.info("new tracing interceptor created. traceId={}", traceId);
        return TracingHeadersInterceptor.builder()
                .getTraceId(() -> traceId)
                .getSpanId(tracer::nextSpanId)
                .build();
    }

    private static final String TRACE_ID = "x-b3-traceid";
    private static final String SPAN_ID = "x-b3-spanid";

    private static final Metadata.Key<String> TRACE_ID_KEY = keyOf(TRACE_ID);
    private static final Metadata.Key<String> SPAN_ID_KEY = keyOf(SPAN_ID);

    private final Supplier<String> getTraceId;
    private final Supplier<String> getSpanId;

    @Override
    public void mutate(Metadata headers) {
        headers.put(TRACE_ID_KEY, getTraceId.get());
        headers.put(SPAN_ID_KEY, getSpanId.get());
    }
}
