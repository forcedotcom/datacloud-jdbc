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
    public HyperServiceGrpc.HyperServiceBlockingStub getStub();
}
