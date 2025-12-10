/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

class QuerySchemaAccessorTest extends InterceptedHyperTestBase {
    @Test
    @SneakyThrows
    void getArrowSchema_shouldReturnSchema() {
        String queryId;
        try (val connection = getInterceptedClientConnection();
                val stmt = connection.createStatement()) {
            // Submit a query to get query id
            val result = (DataCloudResultSet) stmt.executeQuery("SELECT 1 as a");
            queryId = result.getQueryId();
        }

        // Fetch schema
        val stub = getInterceptedStub();
        val schema = QuerySchemaAccessor.getArrowSchema(QueryAccessGrpcClient.of(queryId, stub));
        assertThat(schema).isNotNull();
        assertThat(schema.getFields()).hasSize(1);
        assertThat(schema.getFields().get(0).getName()).isEqualTo("a");
    }

    @Test
    void getArrowSchema_shouldThrowExceptionWhenIteratorExhaustedWithoutSchema() {
        val stub = getInterceptedStub();
        String queryId = "TEST-1234";

        // Setup a query info call that doesn't return a schema
        setupGetQueryInfo(queryId, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);

        // Execute & Verify - should throw exception
        val client = QueryAccessGrpcClient.of(queryId, stub);
        assertThatThrownBy(() -> QuerySchemaAccessor.getArrowSchema(client))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("No schema data available")
                .hasMessageContaining("queryId=" + queryId)
                .matches(ex -> ((StatusRuntimeException) ex).getStatus().getCode() == Status.Code.INTERNAL);
    }
}
