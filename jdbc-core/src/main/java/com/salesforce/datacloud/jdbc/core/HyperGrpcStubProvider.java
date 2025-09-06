/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

/**
 * Interface used to generate a stub for the DataCloudConnection.
 *
 * Implement this interface to customize the stub creation, e.g., using custom
 * interceptors or using a custom pool of grpc channels.
 *
 * To allow proper cleanup, the interface extends AutoCloseable and the driver will
 * call close() on the provider when the DataCloudConnection is closed.
 */
public interface HyperGrpcStubProvider extends AutoCloseable {
    /**
     * Returns a new HyperServiceGrpc.HyperServiceBlockingStub
     *
     * @return the stub
     */
    HyperServiceGrpc.HyperServiceBlockingStub getStub();
}
