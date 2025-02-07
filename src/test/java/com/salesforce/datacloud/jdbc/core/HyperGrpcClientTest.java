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
package com.salesforce.datacloud.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.protobuf.ByteString;
import java.sql.SQLException;
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
        Iterator<ExecuteQueryResponse> queryResultIterator = hyperGrpcClient.executeQuery(query);
        assertDoesNotThrow(() -> {
            while (queryResultIterator.hasNext()) {
                queryResultIterator.next();
            }
        });

        QueryParam expectedQueryParam = QueryParam.newBuilder()
                .setQuery(query)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.SYNC)
                .build();
        GrpcMock.verifyThat(
                GrpcMock.calledMethod(HyperServiceGrpc.getExecuteQueryMethod()).withRequest(expectedQueryParam));
    }
}
