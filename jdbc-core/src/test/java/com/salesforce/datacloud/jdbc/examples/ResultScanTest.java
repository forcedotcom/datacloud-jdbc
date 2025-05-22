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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class ResultScanTest {
    @SneakyThrows
    @Test
    void testResultScanWithWait() {
        int size = 10;
        Properties properties = new Properties();
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        final String queryId;

        try (val conn = DataCloudConnection.of(channel, properties);
                val stmt = conn.prepareStatement("SELECT a from generate_series(1,?) a")
                        .unwrap(DataCloudPreparedStatement.class)) {
            stmt.setInt(1, size);
            stmt.execute();
            queryId = stmt.getQueryId();
        }

        val results = new ArrayList<Integer>();

        try (val conn = DataCloudConnection.of(channel, properties);
                val stmt = conn.createStatement()) {
            conn.waitForQueryStatus(queryId, Duration.ofDays(1), DataCloudQueryStatus::isExecutionFinished);

            val rs = stmt.executeQuery(String.format("SELECT * from result_scan('%s')", queryId));

            while (rs.next()) {
                results.add(rs.getInt(1));
            }

            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.rangeClosed(1, size).boxed().collect(Collectors.toList()));
        }
    }
}
