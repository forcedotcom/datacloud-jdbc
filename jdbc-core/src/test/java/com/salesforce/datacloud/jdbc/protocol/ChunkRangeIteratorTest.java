/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.*;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.OutputFormat;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
class ChunkRangeIteratorTest {
    @SneakyThrows
    private List<Integer> sut(String queryId, long chunkId, long limit) {
        try (val connection = getHyperQueryConnection()) {
            val rs = limit == 1
                    ? connection.getChunkBasedResultSet(queryId, chunkId)
                    : connection.getChunkBasedResultSet(queryId, chunkId, limit);
            return RowRangeIteratorTest.toStream(rs).collect(Collectors.toList());
        }
    }

    private static final int smallSize = 4;
    private static final int largeSize = smallSize * 16;
    private static String singleChunk;
    private static String multipleChunks;

    @SneakyThrows
    @BeforeAll
    static void setupQueries() {
        singleChunk = getQueryId(smallSize);
        multipleChunks = getQueryId(largeSize);

        try (val conn = getHyperQueryConnection()) {
            conn.waitFor(singleChunk, QueryStatus::allResultsProduced);
            conn.waitFor(multipleChunks, QueryStatus::allResultsProduced);
        }
    }

    @SneakyThrows
    @Test
    void canGetSimpleChunk() {
        val actual = sut(singleChunk, 0, 1);
        assertThat(actual).containsExactly(1, 2, 3, 4);
    }

    @SneakyThrows
    @Test
    void canGetPartialRange() {
        val actual = sut(multipleChunks, 1, 2);
        assertThat(actual).containsExactly(5, 6, 7, 8, 9, 10, 11, 12);
    }

    @SneakyThrows
    @Test
    void canHandleEmptyFirstChunk() {
        val process = HyperServerManager.get(
                HyperServerConfig.builder().grpcAdaptiveTimeoutSeconds("2"),
                HyperServerManager.ConfigFile.SMALL_CHUNKS);
        try (val conn = getHyperQueryConnection(process);
                val stmt = (DataCloudStatement) conn.createStatement()) {
            // The adaptive timeout will lead to an empty first chunk
            stmt.execute("SELECT 1, pg_sleep(3)");
            val status = conn.waitFor(stmt.getQueryId(), QueryStatus::allResultsProduced);
            assertThat(status.getChunkCount() == 2);

            ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress("127.0.0.1", process.getPort())
                    .usePlaintext();
            val stubProvider = JdbcDriverStubProvider.of(channel);
            val iterator = ChunkRangeIterator.of(
                    QueryAccessGrpcClient.of(stmt.getQueryId(), stubProvider.getStub()),
                    0,
                    2,
                    true,
                    OutputFormat.JSON_ARRAY);
            assertThat(iterator.next().getStringPart().getData()).isEqualTo("[[1,true]]");
        }
    }

    @SneakyThrows
    @Test
    void failsOnChunkOverrun() {
        assertThatThrownBy(() -> sut(singleChunk, 0, 2))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("The requested chunk id '1' is out of range");
    }

    @SneakyThrows
    @Test
    void consecutiveChunksIncludeAllData() {

        val last = new AtomicLong(0);
        try (val connection = getHyperQueryConnection()) {
            val status = connection.waitFor(multipleChunks, QueryStatus::allResultsProduced);
            val rs = connection.getChunkBasedResultSet(multipleChunks, 0, status.getChunkCount());

            while (rs.next()) {
                assertThat(rs.getLong(1)).isEqualTo(last.incrementAndGet());
            }
        }

        assertThat(last.get()).isEqualTo(largeSize);
    }

    @SneakyThrows
    private static String getQueryId(int max) {
        val query = String.format(
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, %d) as s(a) order by a asc",
                max);

        try (val client = getHyperQueryConnection();
                val statement = client.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeQuery(query);
            return statement.getQueryId();
        }
    }
}
