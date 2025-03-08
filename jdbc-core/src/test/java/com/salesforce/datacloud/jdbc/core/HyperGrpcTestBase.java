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

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.auth.DataCloudToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.RealisticArrowGenerator;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@ExtendWith(InProcessGrpcMockExtension.class)
public class HyperGrpcTestBase extends HyperTestBase {

    protected static HyperGrpcClientExecutor hyperGrpcClient;

    @Mock
    protected TokenProcessor mockSession;

    @Mock
    protected DataCloudToken mockToken;

    @Mock
    protected AuthenticationSettings mockSettings;

    @BeforeEach
    public void setUpClient() throws SQLException, IOException {
        mockToken = mock(DataCloudToken.class);
        lenient().when(mockToken.getAccessToken()).thenReturn("1234");
        lenient().when(mockToken.getTenantId()).thenReturn("testTenantId");
        lenient().when(mockToken.getTenantUrl()).thenReturn("tenant.salesforce.com");

        mockSettings = mock(AuthenticationSettings.class);
        lenient().when(mockSettings.getUserAgent()).thenReturn("userAgent");
        lenient().when(mockSettings.getDataspace()).thenReturn("testDataspace");

        mockSession = mock(TokenProcessor.class);
        lenient().when(mockSession.getDataCloudToken()).thenReturn(mockToken);
        lenient().when(mockSession.getSettings()).thenReturn(mockSettings);

        val channel = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext();
        hyperGrpcClient = HyperGrpcClientExecutor.of(channel, new Properties());
    }

    @SneakyThrows
    @AfterEach
    public void cleanup() {
        if (hyperGrpcClient != null) {
            hyperGrpcClient.close();
        }
    }

    private void willReturn(List<ExecuteQueryResponse> responses, QueryParam.TransferMode mode) {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req -> mode == null || req.getTransferMode() == mode)
                .willReturn(GrpcMock.stream(responses)));
    }

    private Stream<ExecuteQueryResponse> queryStatusResponse(String queryId) {
        return Stream.of(ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder()
                        .setQueryStatus(
                                QueryStatus.newBuilder().setQueryId(queryId).build())
                        .build())
                .build());
    }

    public void setupHyperGrpcClientWithMockedResultSet(
            String expectedQueryId, List<RealisticArrowGenerator.Student> students) {
        setupHyperGrpcClientWithMockedResultSet(expectedQueryId, students, null);
    }

    public void setupHyperGrpcClientWithMockedResultSet(
            String expectedQueryId, List<RealisticArrowGenerator.Student> students, QueryParam.TransferMode mode) {
        willReturn(
                Stream.concat(
                                queryStatusResponse(expectedQueryId),
                                RealisticArrowGenerator.getMockedData(students)
                                        .map(t -> ExecuteQueryResponse.newBuilder()
                                                .setQueryResult(t)
                                                .build()))
                        .collect(Collectors.toList()),
                mode);
    }

    public void setupExecuteQuery(
            String queryId, String query, QueryParam.TransferMode mode, ExecuteQueryResponse... responses) {
        val first = ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder()
                        .setQueryStatus(
                                QueryStatus.newBuilder().setQueryId(queryId).build())
                        .build())
                .build();

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req -> req.getQuery().equals(query) && req.getTransferMode() == mode)
                .willReturn(
                        Stream.concat(Stream.of(first), Stream.of(responses)).collect(Collectors.toList())));
    }

    public void setupGetQueryInfo(String queryId, QueryStatus.CompletionStatus completionStatus) {
        setupGetQueryInfo(queryId, completionStatus, 1);
    }

    protected void setupGetQueryInfo(String queryId, QueryStatus.CompletionStatus completionStatus, int chunkCount) {
        val queryInfo = QueryInfo.newBuilder()
                .setQueryStatus(QueryStatus.newBuilder()
                        .setQueryId(queryId)
                        .setCompletionStatus(completionStatus)
                        .setChunkCount(chunkCount)
                        .build())
                .build();
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(queryId))
                .willReturn(queryInfo));
    }

    protected void verifyGetQueryInfo(int times) {
        GrpcMock.verifyThat(GrpcMock.calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), GrpcMock.times(times));
    }

    public void setupGetQueryResult(
            String queryId, int chunkId, int parts, List<RealisticArrowGenerator.Student> students) {
        val results = IntStream.range(0, parts)
                .mapToObj(i -> RealisticArrowGenerator.getMockedData(students))
                .flatMap(UnaryOperator.identity())
                .collect(Collectors.toList());

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryResultMethod())
                .withRequest(req -> req.getQueryId().equals(queryId)
                        && req.getChunkId() == chunkId
                        && req.getOmitSchema() == chunkId > 0)
                .willReturn(results));
    }

    public static ExecuteQueryResponse executeQueryResponseWithData(List<RealisticArrowGenerator.Student> students) {
        val result = RealisticArrowGenerator.getMockedData(students).findFirst().orElseThrow(RuntimeException::new);
        return ExecuteQueryResponse.newBuilder().setQueryResult(result).build();
    }

    public static ExecuteQueryResponse executeQueryResponse(
            String queryId, QueryStatus.CompletionStatus status, Integer chunkCount) {
        val queryStatus = QueryStatus.newBuilder().setQueryId(queryId);

        if (status != null) {
            queryStatus.setCompletionStatus(status);
        }

        if (chunkCount != null) {
            queryStatus.setChunkCount(chunkCount);
        }

        return ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder().setQueryStatus(queryStatus).build())
                .build();
    }
}
