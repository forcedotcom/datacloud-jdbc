/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class QuerySchemaAccessorTest {
    @Test
    @SneakyThrows
    void getArrowSchema_shouldReturnSchema() {
        LocalHyperTestBase.assertWithStubProvider(provider -> {
            try (val connection = DataCloudConnection.of(provider, ConnectionProperties.defaultProperties(), null);
                    val stmt = connection.createStatement()) {
                // Submit a query to get query id
                val result = (DataCloudResultSet) stmt.executeQuery("SELECT 1 as a");
                val queryId = result.getQueryId();

                // Fetch schema
                val stub = provider.getStub();
                val schema = QuerySchemaAccessor.getArrowSchema(QueryAccessGrpcClient.of(queryId, stub));
                assertThat(schema).isNotNull();
                assertThat(schema.getFields()).hasSize(1);
                assertThat(schema.getFields().get(0).getName()).isEqualTo("a");
            }
        });
    }

    /*
    @Test
    void getArrowSchema_shouldThrowExceptionWhenIteratorExhaustedWithoutSchema() {
        LocalHyperTestBase.assertWithStubProvider(provider -> {
            String queryId = "TEST-1234";

            // Setup spy to intercept getQueryInfo and return empty iterator
            val stub = provider.getStub();
            val spyStub = spy(stub);
            val client = QueryAccessGrpcClient.of(queryId, spyStub);
            val spyClient = spy(client);
            when(spyClient.getStub()).thenReturn(spyStub);

            // Create QueryInfo messages without binary schema
            val queryInfoWithoutSchema = QueryInfo.newBuilder().build();
            // Intercept getQueryInfo calls for our specific queryId
            when(spyStub.getQueryInfo(argThat(param -> param.getQueryId().equals(queryId))))
                    .thenReturn(Collections.singleton(queryInfoWithoutSchema).iterator());

            // Execute & Verify - should throw exception
            assertThatThrownBy(() -> QuerySchemaAccessor.getArrowSchema(spyClient))
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("No schema data available")
                    .hasMessageContaining("queryId=" + queryId)
                    .matches(ex -> ((StatusRuntimeException) ex).getStatus().getCode() == Status.Code.INTERNAL);
        });
    }
     */
}
