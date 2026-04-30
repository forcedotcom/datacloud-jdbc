/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryExecutionStatistics;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;

@ExtendWith(LocalHyperTestBase.class)
public class AsyncQueryAccessHandleTest {

    @Test
    public void testAsyncQueryAccessHandle_withLongRunningQuery() {
        LocalHyperTestBase.assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();
            val param = QueryParam.newBuilder()
                    // Use a huge sleep to ensure that the test is not blocked by query execution
                    .setSql("SELECT pg_sleep(10)")
                    .setTransferMode(QueryParam.TransferMode.ASYNC)
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .build();

            val handle = AsyncQueryAccessHandle.of(stub, param);
            assertThat(handle.getQueryStatus().getQueryId()).isNotEmpty();
        });
    }

    @Test
    public void testAsyncQueryAccessHandle_withLongRunningJsonQuery() {
        LocalHyperTestBase.assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();
            val param = QueryParam.newBuilder()
                    .setSql("SELECT pg_sleep(10)")
                    .setTransferMode(QueryParam.TransferMode.ASYNC)
                    // Also works with JSON as this layer is agnostic of output format
                    .setOutputFormat(OutputFormat.JSON_ARRAY)
                    .build();

            val handle = AsyncQueryAccessHandle.of(stub, param);
            assertThat(handle.getQueryStatus().getQueryId()).isNotEmpty();
        });
    }

    @Test
    public void testAsyncQueryAccessHandle_withFailingQuery() {
        LocalHyperTestBase.assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();
            val param = QueryParam.newBuilder()
                    .setSql("SELECT a")
                    .setTransferMode(QueryParam.TransferMode.ASYNC)
                    // Also works with JSON to cover that path
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .build();
            StatusRuntimeException exception =
                    assertThrows(StatusRuntimeException.class, () -> AsyncQueryAccessHandle.of(stub, param));
            assertThat(exception.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(exception.getStatus().getDescription()).contains("unknown column 'a'");
        });
    }

    @Test
    public void testObserveQueryStatus_replacesLatestWrapperStatus() {
        LocalHyperTestBase.assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();
            val param = QueryParam.newBuilder()
                    .setQuery("SELECT pg_sleep(10)")
                    .setTransferMode(QueryParam.TransferMode.ASYNC)
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .build();

            val handle = AsyncQueryAccessHandle.of(stub, param);
            val initial = handle.getLatestWrapperStatus();
            assertThat(initial).isNotNull();
            assertThat(initial.getExecutionStatistics()).isNull();

            val externallyObserved = new QueryStatus(
                    initial.getQueryId(),
                    7L,
                    42L,
                    1.0,
                    QueryStatus.CompletionStatus.FINISHED,
                    new QueryExecutionStatistics(Duration.ofMillis(500), 42L));

            handle.observeQueryStatus(externallyObserved);

            val after = handle.getLatestWrapperStatus();
            assertThat(after).isSameAs(externallyObserved);
            assertThat(after.allResultsProduced()).isTrue();
            assertThat(after.getExecutionStatistics().getRowsProcessed()).isEqualTo(42L);
        });
    }

    @Test
    public void testObserveQueryStatus_nullIsIgnored() {
        LocalHyperTestBase.assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();
            val param = QueryParam.newBuilder()
                    .setQuery("SELECT pg_sleep(10)")
                    .setTransferMode(QueryParam.TransferMode.ASYNC)
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .build();

            val handle = AsyncQueryAccessHandle.of(stub, param);
            val initial = handle.getLatestWrapperStatus();
            handle.observeQueryStatus(null);
            assertThat(handle.getLatestWrapperStatus()).isSameAs(initial);
        });
    }
}
