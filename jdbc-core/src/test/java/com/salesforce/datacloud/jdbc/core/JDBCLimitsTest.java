/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithConnection;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStatement;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import io.grpc.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class JDBCLimitsTest {
    @Test
    @SneakyThrows
    public void testLargeQuery() {
        String query = "SELECT 'a', /*" + repeat('x', 62 * 1024 * 1024) + "*/ 'b'";
        // Verify that the full SQL string is submitted by checking that the value before and after the large
        // comment are returned
        assertWithStatement(statement -> {
            val result = statement.executeQuery(query);
            result.next();
            assertThat(result.getString(1)).isEqualTo("a");
            assertThat(result.getString(2)).isEqualTo("b");
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeQuery() {
        String query = "SELECT 'a', /*" + repeat('x', 65 * 1024 * 1024) + "*/ 'b'";
        assertWithStatement(statement -> {
            assertThatExceptionOfType(SQLException.class)
                    .isThrownBy(() -> statement.executeQuery(query))
                    // Also verify that we don't explode exception sizes by keeping the full query
                    .withMessageEndingWith("<truncated>")
                    .satisfies(t -> assertThat(t.getMessage()).hasSizeLessThan(16500));
        });
    }

    @Test
    @SneakyThrows
    public void testLargeRowResponse() {
        // 31 MB is the expected max row size configured in Hyper
        String value = repeat('x', 31 * 1024 * 1024);
        String query = "SELECT rpad('', 31*1024*1024, 'x')";
        // Verify that large responses are supported
        assertWithStatement(statement -> {
            val result = statement.executeQuery(query);
            result.next();
            assertThat(result.getString(1)).isEqualTo(value);
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeRowResponse() {
        // 31 MB is the expected max row size configured in Hyper, thus 33 MB should be too large
        String query = "SELECT rpad('', 33*1024*1024, 'x')";
        assertWithStatement(statement -> assertThatThrownBy(
                        () -> statement.executeQuery(query).next())
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("tuple size limit exceeded")
                .satisfies(t -> {
                    val m = t.getMessage();
                    assertThat(m).isNotBlank();
                }));
    }

    @Test
    @SneakyThrows
    public void testLargeParameterRoundtrip() {
        // 31 MB is the expected max row size configured in Hyper
        String value = repeat('x', 31 * 1024 * 1024);
        // Verify that large responses are supported
        assertWithConnection(connection -> {
            val stmt = connection.prepareStatement("SELECT ?");
            stmt.setString(1, value);
            val result = stmt.executeQuery();
            result.next();
            assertThat(result.getString(1)).isEqualTo(value);
        });
    }

    @Test
    @SneakyThrows
    public void testLargeParameter() {
        // We can send requests of up to 64MB so this parameter should still be accepted
        String value = repeat('x', 63 * 1024 * 1024);
        // Verify that large responses are supported
        assertWithConnection(connection -> {
            val stmt = connection.prepareStatement("SELECT length(?)");
            stmt.setString(1, value);
            val result = stmt.executeQuery();
            result.next();
            assertThat(result.getInt(1)).isEqualTo(value.length());
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeParameter() {
        // We can send requests of up to 64MB so this parameter should fail
        String value = repeat('x', 64 * 1024 * 1024);
        assertWithConnection(connection -> {
            try (val stmt = connection.prepareStatement("SELECT length(?)")) {
                assertThatExceptionOfType(SQLException.class).isThrownBy(() -> {
                    stmt.setString(1, value);
                    stmt.executeQuery();
                });
            }
        });
    }

    @Test
    @SneakyThrows
    public void testLargeHeaders() {
        // We expect that under 1 MB total header size should be fine, we use workload as it'll get injected into the
        // header
        val properties = new Properties();
        properties.put("workload", repeat('x', 1000 * 1024));
        try (val connection = getHyperQueryConnection(properties);
                val statement = connection.createStatement()) {
            val result = statement.executeQuery("SELECT 'A'");
            result.next();
            assertThat(result.getString(1)).isEqualTo("A");
        }
    }

    @Test
    @SneakyThrows
    public void testTooLargeHeaders() {
        // We expect that due to 1 MB total header size limit, setting such a large workload should fail
        val properties = new Properties();
        properties.put("workload", repeat('x', 1024 * 1024));
        try (val connection = getHyperQueryConnection(properties);
                val statement = connection.createStatement()) {
            assertThatExceptionOfType(SQLException.class).isThrownBy(() -> {
                statement.executeQuery("SELECT 'A'");
            });
        }
    }

    /**
     * A stub provider that injects a configureable trace id and allows to toggle whether the jdbc channel configuration
     * should be applied or not. Used to allow testing the difference between default gRPC and JDBC behavior for headers.
     */
    public static class TraceIdChannelConfigStubProvider implements HyperGrpcStubProvider {
        final ManagedChannel channel;

        TraceIdChannelConfigStubProvider(int port, String traceId, boolean useJDBCChannelConfig) {
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress("localhost", port);
            if (useJDBCChannelConfig) {
                val jdbcChannelProperties = GrpcChannelProperties.defaultProperties();
                jdbcChannelProperties.applyToChannel(builder);
            }
            builder = builder.intercept(new ClientInterceptor() {
                        @Override
                        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                                    next.newCall(method, callOptions)) {
                                @Override
                                public void start(Listener<RespT> responseListener, Metadata headers) {
                                    // Add B3 trace ID header
                                    headers.put(
                                            Metadata.Key.of("x-b3-traceid", Metadata.ASCII_STRING_MARSHALLER), traceId);
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    })
                    .usePlaintext();
            channel = builder.build();
        }

        @Override
        public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
            return HyperServiceGrpc.newBlockingStub(channel);
        }

        @Override
        public void close() {
            channel.shutdown();
        }
    }

    @Test
    @SneakyThrows
    public void testLargeResponseHeaders() {
        // We are not allowed to close as this instance is cached
        val server = HyperServerManager.get(HyperServerManager.ConfigFile.SMALL_CHUNKS);
        // The Hyper service sends back the trace id as response header. With the JDBC drivers channel config we should
        // support close to 1 MB response header as large headers can get injected by side cars in mesh scenarios.
        try (val connection = DataCloudConnection.of(
                new TraceIdChannelConfigStubProvider(server.getPort(), StringUtils.leftPad(" ", 100 * 1024), true),
                ConnectionProperties.defaultProperties(),
                "dataspace/unused",
                null)) {
            try (val stmt = connection.createStatement()) {
                // We just verify that we are able to get a full response
                val result = stmt.executeQuery("SELECT 'A'");
                result.next();
                assertThat(result.getString(1)).isEqualTo("A");
            }
        }
        // Verify that the trace id is indeed sent back by checking that the stmt fails with gRPC defaults for the
        // channel.
        try (val connection = DataCloudConnection.of(
                new TraceIdChannelConfigStubProvider(server.getPort(), StringUtils.leftPad(" ", 100 * 1024), false),
                ConnectionProperties.defaultProperties(),
                "dataspace/unused",
                null)) {
            try (val stmt = connection.createStatement()) {
                assertThatExceptionOfType(DataCloudJDBCException.class).isThrownBy(() -> {
                    stmt.executeQuery("SELECT 'A'");
                });
            }
        }
    }

    @Test
    public void testOutOfMemoryRegression() {
        assertWithStatement(stmt -> {
            logMemoryUsage("After statement creation", 0);

            ResultSet rs = stmt.executeQuery("SELECT s, lpad('A', 1024*1024, 'X') FROM generate_series(1, 2048) s");

            logMemoryUsage("After query execution", 0);

            long maxUsedMemory = 0;
            long previousLogRow = 0;

            while (rs.next()) {
                AssertionsForClassTypes.assertThat(rs.getLong(1)).isEqualTo(rs.getRow());

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
        });
    }

    private static String repeat(char c, int length) {
        val str = String.format("%c", c);
        return Stream.generate(() -> str).limit(length).collect(Collectors.joining());
    }

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

        assertThat(usedMemory / (1024.0 * 1024.0 * 1024.0))
                .isLessThan(128); // TODO: we can probably do some work to get a tighter number for this

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
}
