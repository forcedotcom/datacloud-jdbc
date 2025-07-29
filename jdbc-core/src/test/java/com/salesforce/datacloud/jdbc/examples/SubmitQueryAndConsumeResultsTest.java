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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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

    private static String formatMemory(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static void logMemoryUsage(String context, long rowNumber) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();
        double usagePercent = (usedMemory * 100.0) / maxMemory;

        log.warn(
                "context={}, row={}, used={}, free={}, total={}, max={}, usage={}%",
                context,
                rowNumber,
                formatMemory(usedMemory),
                formatMemory(freeMemory),
                formatMemory(totalMemory),
                formatMemory(maxMemory),
                String.format("%.1f", usagePercent));
    }

    /**
     * This example shows how to create a Data Cloud Connection while still having full control over concerns like
     * authorization and tracing.
     */
    @Test
    public void testBareBonesExecuteQuery() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels that are set up in the way you like (mTLS / Plaintext / ...) and your
        // own interceptors as well as executors.
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        logMemoryUsage("Before query execution", 0);

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
            try (Statement stmt = conn.createStatement()) {
                logMemoryUsage("After statement creation", 0);

                ResultSet rs = stmt.executeQuery("SELECT s, lpad('A', 1024*1024, 'X') FROM generate_series(1, 2048) s");

                logMemoryUsage("After query execution", 0);

                long maxUsedMemory = 0;
                long previousLogRow = 0;

                while (rs.next()) {
                    assertThat(rs.getLong(1)).isEqualTo(rs.getRow());

                    long currentRow = rs.getRow();
                    Runtime runtime = Runtime.getRuntime();
                    long currentUsedMemory = runtime.totalMemory() - runtime.freeMemory();
                    maxUsedMemory = Math.max(maxUsedMemory, currentUsedMemory);

                    if ((currentRow & (currentRow - 1)) == 0) {
                        logMemoryUsage("Processing rows", currentRow);
                        previousLogRow = currentRow;
                    }

                    if (currentRow % 100000 == 0 && currentRow != previousLogRow) {
                        logMemoryUsage("Checkpoint", currentRow);
                        System.gc();
                        Thread.sleep(100);
                        logMemoryUsage("After GC", currentRow);
                    }
                }

                logMemoryUsage("After ResultSet consumption", rs.getRow());
                log.warn("Peak memory usage during test: {}", formatMemory(maxUsedMemory));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
