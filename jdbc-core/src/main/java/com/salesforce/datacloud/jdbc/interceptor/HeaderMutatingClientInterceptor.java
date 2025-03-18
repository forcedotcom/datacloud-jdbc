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
