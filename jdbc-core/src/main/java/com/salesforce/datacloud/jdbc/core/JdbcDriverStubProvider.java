/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

/**
 * This class is used to provide a stub for the Hyper gRPC client used by the JDBC Connection.
 */
@Slf4j
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
