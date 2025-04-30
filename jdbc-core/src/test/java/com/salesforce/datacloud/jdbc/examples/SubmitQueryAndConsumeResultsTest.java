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
package com.salesforce.datacloud.jdbc.examples;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This example uses a locally spawned Hyper instance to demonstrate best practices around connecting to Hyper.
 * This consciously only uses the JDBC API in the core and no helpers (outside of this class) to provide self-contained
 * examples.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
public class SubmitQueryAndConsumeResultsTest {
    /**
     * This example shows how to create a Data Cloud Connection while still having full control over concerns like
     * authorization and tracing.
     */
    @Test
    public void testBareBonesExecuteQuery() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels, setup in the way you like (mTLS / Plaintext / ...) and your own
        // interceptors as well as executors.
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.fromChannel(channel, properties)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,10) s");
                while (rs.next()) {
                    System.out.println("Retrieved value:" + rs.getLong(1));
                }
            }
        }
    }
}
