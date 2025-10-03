/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

/**
 * This class is used to provide a stub for the Hyper gRPC client used by the JDBC Connection.
 *
 * It sets up the provided  {@link io.grpc.ManagedChannel} using the default settings
 * for the JDBC driver. Alternatively, you can implement your own {@link HyperGrpcStubProvider}
 * to customize the stub creation.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class JdbcDriverStubProvider implements HyperGrpcStubProvider {
    private final ManagedChannel channel;

    /**
     * Configure required settings (inbound message size and user agent) in addition to optional keep alive and retry settings based on the provided properties.
     */
    public static JdbcDriverStubProvider of(ManagedChannelBuilder<?> builder, GrpcChannelProperties properties) {
        properties.applyToChannel(builder);
        return new JdbcDriverStubProvider(builder.build());
    }

    /**
     * Configure only required settings ({@link ManagedChannelBuilder#maxInboundMessageSize(int)} and {@link ManagedChannelBuilder#userAgent(String)}) and immediately builds and stores a {@link ManagedChannel}.
     */
    public static JdbcDriverStubProvider of(ManagedChannelBuilder<?> builder) {
        return of(builder, GrpcChannelProperties.defaultProperties());
    }

    /**
     * Returns a new HyperServiceGrpc.HyperServiceBlockingStub using the configured channel.
     */
    @Override
    public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
        return HyperServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            return;
        }

        channel.shutdown();

        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Failed to shutdown channel within 5 seconds", e);
        } finally {
            if (!channel.isTerminated()) {
                channel.shutdownNow();
            }
        }
    }
}
