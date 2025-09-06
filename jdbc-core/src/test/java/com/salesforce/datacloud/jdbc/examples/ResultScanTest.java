/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
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
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        final String queryId;

        try (val conn = DataCloudConnection.of(channelBuilder, properties);
                val stmt = conn.prepareStatement("SELECT a from generate_series(1,?) a")
                        .unwrap(DataCloudPreparedStatement.class)) {
            stmt.setInt(1, size);
            stmt.execute();
            queryId = stmt.getQueryId();
        }

        val results = new ArrayList<Integer>();

        try (val conn = DataCloudConnection.of(channelBuilder, properties);
                val stmt = conn.createStatement()) {
            // We don't have any separate query timeouts here, as Spark already has a global job timeout, anyway.
            // TODO (W-18851398): Set the timeout to infinite, as soon as `waitForQueryStatus` accepts it.abstract
            conn.waitFor(queryId, Duration.ofDays(1), QueryStatus::isExecutionFinished);

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
