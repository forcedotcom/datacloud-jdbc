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
package com.salesforce.datacloud.jdbc.hyper;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.hyper.HyperServerConfig;
import com.salesforce.datacloud.hyper.HyperServerProcess;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.core.DataCloudJdbcManagedChannel;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@AllArgsConstructor
@Slf4j
public class HyperConnectionAdapter implements AutoCloseable {
    public HyperConnectionAdapter(HyperServerConfig config) {
        this.process = config.start();
    }

    private final HyperServerProcess process;

    public DataCloudConnection getConnection() {
        return getConnection(ImmutableMap.of());
    }

    @SneakyThrows
    public HyperGrpcClientExecutor getRawClient() {
        val channel = DataCloudJdbcManagedChannel.of(
                ManagedChannelBuilder.forAddress("127.0.0.1", process.getPort()).usePlaintext());
        val stub = channel.getStub(new Properties(), Duration.ZERO);
        return HyperGrpcClientExecutor.of(stub, new Properties());
    }

    @SneakyThrows
    public DataCloudConnection getConnection(Map<String, String> connectionSettings) {
        val properties = new Properties();
        properties.put(DirectDataCloudConnection.DIRECT, "true");
        properties.putAll(connectionSettings);
        val url = DataCloudConnectionString.CONNECTION_PROTOCOL + "//127.0.0.1:" + process.getPort();
        return DirectDataCloudConnection.of(url, properties);
    }

    @Override
    public void close() throws Exception {
        process.close();
    }
}
