/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStubProvider;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.query.v3.QueryStatus;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class QueryResultArrowStreamTest {

    @SneakyThrows
    @Test
    void testArrowStreamWithSimpleSelectQuery() {
        // Execute the query which produces multiple chunks and get the query ID
        val queryId = executeQuery("SELECT g from generate_series(1,10) g");

        // Use assertWithStubProvider to access the stub
        assertWithStubProvider(stubProvider -> {
            // Create QueryAccessGrpcClient and ChunkRangeIterator
            val queryClient = QueryAccessGrpcClient.of(queryId, stubProvider.getStub());
            val chunkIterator = ChunkRangeIterator.of(queryClient, 0, 3, false, QueryResultArrowStream.OUTPUT_FORMAT);

            // Create ArrowStreamReader from the iterator
            try (val reader = QueryResultArrowStream.toArrowStreamReader(chunkIterator)) {
                int rowCount = 0;

                // Count all rows in the arrow stream
                while (reader.loadNextBatch()) {
                    rowCount += reader.getVectorSchemaRoot().getRowCount();
                }

                // Assert that we got exactly 10 rows
                assertThat(rowCount).isEqualTo(10);

                // Verify schema has one column
                assertThat(reader.getVectorSchemaRoot().getFieldVectors()).hasSize(1);
            }
        });
    }

    @SneakyThrows
    @Test
    void testArrowStreamWithNoColumnsQuery() {
        // Execute a query with no columns (SELECT without column list)
        val queryId = executeQuery("SELECT FROM generate_series(1,10)");

        // Use assertWithStubProvider to access the stub
        assertWithStubProvider(stubProvider -> {
            // Create QueryAccessGrpcClient and ChunkRangeIterator
            val queryClient = QueryAccessGrpcClient.of(queryId, stubProvider.getStub());
            val chunkIterator = ChunkRangeIterator.of(queryClient, 0, 3, false, QueryResultArrowStream.OUTPUT_FORMAT);

            // Create ArrowStreamReader from the iterator
            try (val reader = QueryResultArrowStream.toArrowStreamReader(chunkIterator)) {
                int rowCount = 0;

                // Count all rows in the arrow stream
                while (reader.loadNextBatch()) {
                    rowCount += reader.getVectorSchemaRoot().getRowCount();
                }

                // Assert that we got exactly 10 rows
                assertThat(rowCount).isEqualTo(10);

                // Verify schema has zero columns (no columns in SELECT)
                assertThat(reader.getVectorSchemaRoot().getFieldVectors()).isEmpty();
            }
        });
    }

    @SneakyThrows
    private String executeQuery(String query) {
        try (val connection = getHyperQueryConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeQuery(query);
            connection.waitFor(statement.getQueryId(), QueryStatus::allResultsProduced);
            return statement.getQueryId();
        }
    }
}
