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
import static org.assertj.core.data.Offset.offset;

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
                    .satisfies(t -> assertThat(t.getMessage()).hasSizeLessThan(16600));
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

    @Test
    @SneakyThrows
    public void testLargeColumnCount() {
        int columnCount = 100000;

        // Verify that queries with many columns are supported
        assertWithConnection(connection -> {
            // Create a SELECT statement with columnCount columns
            StringBuilder sb = new StringBuilder("SELECT ");
            // Generate column expressions: 1 as c1, 2 as c2, ..., columnCount as cN
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append(i).append(" as c").append(i);
            }
            sb.append(" FROM (VALUES(1)) AS t");

            val stmt = connection.prepareStatement(sb.toString());
            val result = stmt.executeQuery();

            // Verify the result has the expected number of columns
            val metaData = result.getMetaData();
            assertThat(metaData.getColumnCount()).isEqualTo(columnCount);

            // Verify we can read the first row
            assertThat(result.next()).isTrue();

            // Verify values in the first row
            // With HashMap-based findColumn, this should be fast even with 100k columns
            for (int i = 1; i <= columnCount; i++) {
                // With only this it's super fast
                assertThat(result.getInt(i)).isEqualTo(i);
                // With this its suuuuuper slow (before optimization), but should be fast now
                assertThat(result.getInt("c" + Integer.toString(i))).isEqualTo(i);
            }

            // Verify there's only one row
            assertThat(result.next()).isFalse();
        });
    }

    @Test
    @SneakyThrows
    public void testColumnLookupExactMatchPreference() {
        // Test the exact-match-first behavior with case-sensitive columns
        // This verifies that exact matches are preferred over case-insensitive matches
        assertWithConnection(connection -> {
            // Aaa -> 1 (ordinal 0), aaa -> 2 (ordinal 1), AaA -> 3 (ordinal 2)
            String sql = "SELECT 1 as \"Aaa\", 2 as \"aaa\", 3 as \"AaA\" FROM (VALUES(1)) AS t";

            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Exact matches should work (case-sensitive)
            assertThat(result.getInt("Aaa")).isEqualTo(1); // exact match -> ordinal 0 -> index 1
            assertThat(result.getInt("aaa")).isEqualTo(2); // exact match -> ordinal 1 -> index 2
            assertThat(result.getInt("AaA")).isEqualTo(3); // exact match -> ordinal 2 -> index 3

            // Case-insensitive matches should fall back to first lowercase occurrence
            // With putIfAbsent, the first column processed (lowest ordinal) wins
            // "Aaa" (ordinal 0) is processed first, so it wins for lowercase "aaa"
            // "AAA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
            assertThat(result.getInt("AAA")).isEqualTo(1); // lowercase match -> "Aaa" -> ordinal 0 -> index 1
            // "aaA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
            assertThat(result.getInt("aaA")).isEqualTo(1); // lowercase match -> "Aaa" -> ordinal 0 -> index 1

            // Verify exact matches still work after case-insensitive lookups
            assertThat(result.getInt("Aaa")).isEqualTo(1);
            assertThat(result.getInt("aaa")).isEqualTo(2);
            assertThat(result.getInt("AaA")).isEqualTo(3);
        });
    }

    @Test
    @SneakyThrows
    public void testColumnLookupAllGetterMethods() {
        // Test all getter methods with both exact and case-insensitive matches
        assertWithConnection(connection -> {
            String sql = "SELECT "
                    + "123::bigint as \"LongCol\", "
                    + "true as \"BoolCol\", "
                    + "42::smallint as \"ShortCol\", "
                    + "17::smallint as \"ByteCol\", "
                    + "3.14::real as \"FloatCol\", "
                    + "2.718::double precision as \"DoubleCol\" "
                    + "FROM (VALUES(1)) AS t";

            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Test exact matches for all getter types
            assertThat(result.getLong("LongCol")).isEqualTo(123L);
            assertThat(result.getBoolean("BoolCol")).isTrue();
            assertThat(result.getShort("ShortCol")).isEqualTo((short) 42);
            assertThat(result.getByte("ByteCol")).isEqualTo((byte) 17);
            assertThat(result.getFloat("FloatCol")).isEqualTo(3.14f, offset(0.001f));
            assertThat(result.getDouble("DoubleCol")).isEqualTo(2.718, offset(0.001));

            // Test case-insensitive matches for all getter types
            assertThat(result.getLong("longcol")).isEqualTo(123L);
            assertThat(result.getBoolean("BOOLCOL")).isTrue();
            assertThat(result.getShort("ShortCol")).isEqualTo((short) 42);
            assertThat(result.getByte("bytecol")).isEqualTo((byte) 17);
            assertThat(result.getFloat("FLOATCOL")).isEqualTo(3.14f, offset(0.001f));
            assertThat(result.getDouble("doublecol")).isEqualTo(2.718, offset(0.001));
        });
    }

    @Test
    @SneakyThrows
    public void testColumnLookupNotFound() {
        // Test that column not found throws appropriate exception
        assertWithConnection(connection -> {
            String sql = "SELECT 1 as \"col1\", 2 as \"col2\" FROM (VALUES(1)) AS t";
            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Test that findColumn throws exception for non-existent column
            assertThatThrownBy(() -> result.findColumn("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            // Test that all getter methods throw exception for non-existent column
            assertThatThrownBy(() -> result.getString("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getInt("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getLong("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getBoolean("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getByte("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getShort("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getFloat("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");

            assertThatThrownBy(() -> result.getDouble("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found");
        });
    }

    @Test
    @SneakyThrows
    public void testMetadataResultSetColumnLookup() {
        // Test MetadataResultSet column lookup through DatabaseMetaData
        assertWithConnection(connection -> {
            val metaData = connection.getMetaData();
            // getTables returns a MetadataResultSet
            try (val tables = metaData.getTables(null, null, null, null)) {
                // Test exact match
                if (tables.next()) {
                    // These columns exist in the tables metadata
                    String tableName = tables.getString("TABLE_NAME");
                    assertThat(tableName).isNotNull();

                    // Test case-insensitive match
                    String tableName2 = tables.getString("table_name");
                    assertThat(tableName2).isEqualTo(tableName);

                    // Test other getter methods
                    tables.getString("TABLE_SCHEM");
                    tables.getString("TABLE_CAT");
                }
            }

            // getColumns also returns a MetadataResultSet
            try (val columns = metaData.getColumns(null, null, null, null)) {
                if (columns.next()) {
                    // Test exact and case-insensitive matches
                    columns.getString("COLUMN_NAME");
                    columns.getString("column_name");
                    columns.getInt("DATA_TYPE");
                    columns.getInt("data_type");
                }
            }
        });
    }

    /**
     * The intent of this stub provider is to inject a large header that the Hyper server will also mirror back
     * and that thus we get a large header in the _response_ from the Hyper server which we can use to test
     * compatibility with potentially large headers coming through the response.
     * We use the trace id header as it is one of the few headers that Hyper will send back unchanged to the caller.
     */
    public static class LargeHeaderChannelConfigStubProvider implements HyperGrpcStubProvider {
        final ManagedChannel channel;

        LargeHeaderChannelConfigStubProvider(int port, String traceId, boolean useJDBCChannelConfig) {
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
        // For this test case, we want Hyper to respond with a large header. The easiest way to do so, is by sending a
        // large x-b3-traceid as part of the request. Hyper will copy over the trace id into its response headers.
        // With the JDBC drivers channel config we should support close to 1 MB response header as large headers can
        // get injected by side cars in mesh scenarios.
        try (val connection = DataCloudConnection.of(
                new LargeHeaderChannelConfigStubProvider(server.getPort(), StringUtils.leftPad(" ", 100 * 1024), true),
                ConnectionProperties.defaultProperties(),
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
                new LargeHeaderChannelConfigStubProvider(server.getPort(), StringUtils.leftPad(" ", 100 * 1024), false),
                ConnectionProperties.defaultProperties(),
                null)) {
            try (val stmt = connection.createStatement()) {
                assertThatExceptionOfType(SQLException.class).isThrownBy(() -> {
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
