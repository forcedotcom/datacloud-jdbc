/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc;

import io.grpc.ManagedChannelBuilder;

/**
 * The Hyper protocol in parts needs channel settings that differ from the gRPC defaults. This class ensures that
 * the default are overwritten with values that work with the protocol.
 * - The inbound message size default is 4MB. Our protocol doesn't split rows across multiple messages and thus the
 *   message size needs to be large enough to hold a complete row.
 */
public class HyperGrpcChannelSettings {
    private static final int GRPC_INBOUND_MESSAGE_MAX_SIZE = 64 * 1024 * 1024;
    private static final int GRPC_INBOUND_METADATA_MAX_SIZE = 1024 * 1024;

    /**
     * This applies the settings that are required for full hyper protocol support to the managed channel.
     * @param builder The builder to apply the settings to
     */
    public static void applyToBuilder(ManagedChannelBuilder<?> builder) {
        builder.maxInboundMessageSize(GRPC_INBOUND_MESSAGE_MAX_SIZE);
        builder.maxInboundMetadataSize(GRPC_INBOUND_METADATA_MAX_SIZE);
    }
}
