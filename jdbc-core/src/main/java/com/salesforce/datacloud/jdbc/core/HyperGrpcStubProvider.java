/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

/**
 * This interface allows to provide a custom initialized stub for the Hyper gRPC client used by the JDBC Connection.
 * This is useful for example to provide a stub that uses additional custom interceptors or a custom channel. To allow
 * implementations to do proper cleanup, the interface extends AutoCloseable and the driver will call close() on the
 * provider when DataCloudConnection is closed.
 */
public interface HyperGrpcStubProvider extends AutoCloseable {

    /**
     * Returns a new HyperServiceGrpc.HyperServiceBlockingStub
     *
     * @return the stub
     */
    HyperServiceGrpc.HyperServiceBlockingStub getStub();
}
