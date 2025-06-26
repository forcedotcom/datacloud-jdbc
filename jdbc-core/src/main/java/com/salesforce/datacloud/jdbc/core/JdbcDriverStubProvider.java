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
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.interceptor.DataspaceHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.HyperExternalClientContextHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.HyperWorkloadHeaderInterceptor;
import io.grpc.ClientInterceptor;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@Slf4j
/**
 * This class is used to provide a stub for the Hyper gRPC client used by the JDBC Connection.
 * It interprets the connection properties to configure the stub with the appropriate interceptors and query timeout.
 */
public class JdbcDriverStubProvider implements HyperGrpcStubProvider {

    private final DataCloudJdbcManagedChannel channel;
    private final boolean shouldCloseChannelWithStub;

    /**
     * Initializes a new JdbcDriverStubProvider with the given channel and a flag indicating whether the channel should
     * be closed when the stub is closed (when the channel is shared across multiple stub providers this should be false).
     *
     * @param channel the channel to use for the stub
     * @param shouldCloseChannelWithStub a flag indicating whether the channel should be closed when the stub is closed
     */
    public JdbcDriverStubProvider(DataCloudJdbcManagedChannel channel, boolean shouldCloseChannelWithStub) {
        this.channel = channel;
        this.shouldCloseChannelWithStub = shouldCloseChannelWithStub;
    }

    /**
     * Returns a new HyperServiceGrpc.HyperServiceBlockingStub configured interceptors that provide the behavior
     * configured through the connection properties and the query timeout.
     *
     * @param properties the properties used to initialize the JDBC connection which should be used to configure the
     *                   stub
     * @param queryTimeout the query timeout to use for the stub
     * @return a new HyperServiceGrpc.HyperServiceBlockingStub configured using the Properties
     */
    @Override
    public HyperServiceGrpc.HyperServiceBlockingStub getStub(Properties properties, Duration queryTimeout) {
        ClientInterceptor[] interceptors =
                configurePropertyDerivedClientInterceptors(properties).toArray(new ClientInterceptor[0]);

        HyperServiceGrpc.HyperServiceBlockingStub stub =
                HyperServiceGrpc.newBlockingStub(channel.getChannel()).withInterceptors(interceptors);

        if (!queryTimeout.isZero() && !queryTimeout.isNegative()) {
            log.info("Built stub with queryTimeout={}, interceptors={}", queryTimeout, interceptors.length);
            stub = stub.withDeadlineAfter(queryTimeout.getSeconds(), TimeUnit.SECONDS);
        } else {
            log.info("Built stub with queryTimeout=none, interceptors={}", interceptors.length);
        }

        return stub;
    }

    /**
     * Initializes a list of interceptors that handle request level concerns that can be defined through properties
     *
     * @param properties - The connection properties
     * @return a list of client interceptors
     */
    public static List<ClientInterceptor> configurePropertyDerivedClientInterceptors(Properties properties) {
        return Stream.of(
                        HyperExternalClientContextHeaderInterceptor.of(properties),
                        HyperWorkloadHeaderInterceptor.of(properties),
                        DataspaceHeaderInterceptor.of(properties))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        if (shouldCloseChannelWithStub) {
            channel.close();
        }
    }
}
