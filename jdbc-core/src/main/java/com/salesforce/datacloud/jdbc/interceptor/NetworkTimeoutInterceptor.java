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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

/**
 * A gRPC client interceptor that sets a deadline for each RPC call.
 * This allows for per-call timeout control rather than setting a deadline on the entire stub.
 */
@RequiredArgsConstructor
public class NetworkTimeoutInterceptor implements ClientInterceptor {
    private final Duration networkTimeout;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        long networkTimeoutRemaining = networkTimeout.toMillis();
        // If a deadline is already set we'll only override with network timeout if it is shorter than the current
        // deadline
        Deadline currentDeadline = callOptions.getDeadline();
        if (currentDeadline != null) {
            long currentRemaining = currentDeadline.timeRemaining(TimeUnit.MILLISECONDS);
            // The current deadline already guarantees a stricter timeout than the network timeout
            if (currentRemaining <= networkTimeoutRemaining) {
                return next.newCall(method, callOptions);
            }
            // Fall through to set the network timeout
        }
        // Inject the network timeout into the call options
        return next.newCall(method, callOptions.withDeadlineAfter(networkTimeoutRemaining, TimeUnit.MILLISECONDS));
    }
}
