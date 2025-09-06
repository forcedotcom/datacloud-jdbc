/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/** https://grpc.github.io/grpc-java/javadoc/io/grpc/auth/ClientAuthInterceptor.html */
@Getter
@Slf4j
public class RequestRecordingInterceptor implements ClientInterceptor {

    private final List<String> queries = new ArrayList<>();

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        val name = method.getFullMethodName();

        queries.add(name);
        log.info("Executing grpc endpoint: " + name);

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                headers.put(Metadata.Key.of("FOO", Metadata.ASCII_STRING_MARSHALLER), "BAR");
                super.start(responseListener, headers);
            }
        };
    }
}
