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

import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@Slf4j
/**
 * This class is used to provide a stub for the Hyper gRPC client used by the JDBC Connection.
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
     * Returns a new HyperServiceGrpc.HyperServiceBlockingStub using the configured channel.
     *
     * @return a new HyperServiceGrpc.HyperServiceBlockingStub configured using the Properties
     */
    @Override
    public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
        return HyperServiceGrpc.newBlockingStub(channel.getChannel());
    }

    @Override
    public void close() throws Exception {
        if (shouldCloseChannelWithStub) {
            channel.close();
        }
    }
}
