/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.grpcmock.GrpcMock.*;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.verifyThat;

import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;

public class QueryResultIteratorFunctionalTest extends InterceptedHyperTestBase {
    @SneakyThrows
    @Test
    void getQueryInfoRetriesOnTimeout() {
        val size = 10000;
        val results = new ArrayList<Integer>();
        val configWithSleep =
                HyperServerConfig.builder().grpcAdaptiveTimeoutSeconds("4").build();

        try (val conn = getInterceptedClientConnection(configWithSleep)) {
            val stmt = conn.prepareStatement("SELECT g FROM generate_series(1,?) g WHERE pg_sleep(8)")
                    .unwrap(DataCloudPreparedStatement.class);
            stmt.setInt(1, size);
            val rs = stmt.executeQuery();

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            while (rs.next()) {
                results.add(rs.getInt(1));
            }

            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.rangeClosed(1, size).boxed().collect(Collectors.toList()));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(1));
        }
    }

    @Test
    void canHandleJsonOutputFormat() {
        val stub = getInterceptedStub();

        val query = QueryParam.newBuilder()
                .setQuery("SELECT g FROM generate_series(1,5) g")
                .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                .setOutputFormat(OutputFormat.JSON_ARRAY)
                .build();

        val iterator = QueryResultIterator.of(stub, query);

        // Verify that the iterator can be used and returns results
        // Skip schema
        iterator.next();
        // Get data
        val result = iterator.next();
        assertThat(result).isNotNull();
        assertThat(result.hasStringPart()).isTrue();
        assertThat(result.getStringPart().getData()).isEqualTo("[[1],[2],[3],[4]]");
        val result2 = iterator.next();
        assertThat(result2.getStringPart().getData()).isEqualTo("[[5]]");
    }

    @Test
    void canHandleAsyncMode() {
        val stub = getInterceptedStub();
        val query = QueryParam.newBuilder()
                .setQuery("SELECT g FROM generate_series(1,5) g")
                .setTransferMode(QueryParam.TransferMode.ASYNC)
                .setOutputFormat(OutputFormat.JSON_ARRAY)
                .build();

        val iterator = QueryResultIterator.of(stub, query);

        // Verify that the iterator can be used and returns results
        // Skip schema
        iterator.next();
        // Get data
        val result = iterator.next();
        assertThat(result).isNotNull();
        assertThat(result.hasStringPart()).isTrue();
        assertThat(result.getStringPart().getData()).isEqualTo("[[1],[2],[3],[4]]");
        val result2 = iterator.next();
        assertThat(result2.getStringPart().getData()).isEqualTo("[[5]]");
    }
}
