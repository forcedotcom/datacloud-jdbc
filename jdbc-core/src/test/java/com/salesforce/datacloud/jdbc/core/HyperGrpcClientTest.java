/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

class HyperGrpcClientTest extends HyperGrpcTestBase {

    private static final ExecuteQueryResponse chunk1 = ExecuteQueryResponse.newBuilder()
            .setBinaryPart(QueryResultPartBinary.newBuilder()
                    .setData(ByteString.copyFromUtf8("test 1"))
                    .build())
            .build();

    @Test
    public void testExecuteQuery() throws SQLException {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .willReturn(chunk1));

        String query = "SELECT * FROM test";
        Iterator<ExecuteQueryResponse> queryResultIterator =
                hyperGrpcClient.executeQuery(query, QueryTimeout.of(Duration.ZERO, Duration.ZERO));
        assertDoesNotThrow(() -> {
            while (queryResultIterator.hasNext()) {
                queryResultIterator.next();
            }
        });

        QueryParam expectedQueryParam = QueryParam.newBuilder()
                .setQuery(query)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                .build();
        GrpcMock.verifyThat(
                GrpcMock.calledMethod(HyperServiceGrpc.getExecuteQueryMethod()).withRequest(expectedQueryParam));
    }
}
