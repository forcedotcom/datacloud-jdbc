/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.SneakyThrows;

@FunctionalInterface
public interface HeaderMutatingClientInterceptor extends ClientInterceptor {
    void mutate(final Metadata headers) throws DataCloudJDBCException;

    @Override
    default <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @SneakyThrows
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                try {
                    mutate(headers);
                } catch (Exception ex) {
                    throw new DataCloudJDBCException(
                            "Caught exception when mutating headers in client interceptor", ex);
                }

                super.start(responseListener, headers);
            }
        };
    }
}
