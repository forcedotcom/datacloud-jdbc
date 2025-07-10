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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.HyperLogScope;
import java.sql.SQLException;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class DataCloudConnectionFunctionalTest {
    @Test
    public void testNetworkTimeoutDefault() throws SQLException {
        // Verify that by default no deadline is set
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = HyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            assertThat(connection.getNetworkTimeout()).isZero();
            // Make a call to capture the gRPC deadline
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // If the caller has no deadline set the server side will report a super high deadline
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION)from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isGreaterThan(Duration.ofDays(5).toMillis() / 1000.0);
        }
        hyperLogScope.close();
    }

    @Test
    public void testNetworkTimeoutPropagatesToServer() throws SQLException {
        // Verify that when network timeout is set a corresponding deadline is set on the gRPC call level
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = HyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            // Set the network timeout to 5 seconds
            connection.setNetworkTimeout(null, 5000);
            assertThat(connection.getNetworkTimeout()).isEqualTo(5000);
            // Make a call to capture the gRPC deadline
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // Verify the deadline propagated to HyperServerConfig, and that it's less than the default (which is test
            // in
            // other test case)
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION)from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
        }
        hyperLogScope.close();
    }

    @Test
    @SneakyThrows
    public void testNetworkTimeoutIsPerGrpcCall() {
        // This is a regression test as we previously had set the deadline on the stub which results in a deadline
        // across all calls made on that stub. While the desired network timeout behavior is that it should
        // independently apply on each call. Thus in this test we first create a stub and then check that after a sleep
        // there still is approximately the full network timeout duration for the next call.
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = HyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            // Set the network timeout to 5 seconds
            connection.setNetworkTimeout(null, 5000);
            assertThat(connection.getNetworkTimeout()).isEqualTo(5000);

            // Do an initial call to ensure that the stub deadline would get definitely started (and that we don't
            // accidentally rely on some internal behavior)
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }
            // Sleep for 1.1 second after which a shared deadline would not be able to be >4s anymore (at most 3.9s)
            Thread.sleep(1100);
            // Do a second call and we'll verify that the deadline is still approximately 5 seconds from now
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // Verify that all the deadlines are still >4s
            // We allow up to 1 second of slack to account for busy local testing machine
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION) from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
            assertThat(rs.getDouble(1)).isGreaterThan(4);
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
            assertThat(rs.getDouble(1)).isGreaterThan(4);
            assertThat(rs.next()).isFalse();
        }
        hyperLogScope.close();
    }
}
