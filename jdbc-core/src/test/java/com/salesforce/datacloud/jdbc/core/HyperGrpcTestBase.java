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

import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.HyperServerProcess;
import com.salesforce.datacloud.jdbc.interceptor.QueryIdHeaderInterceptor;
import com.salesforce.datacloud.jdbc.util.RealisticArrowGenerator;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@Slf4j
@ExtendWith(InProcessGrpcMockExtension.class)
public class HyperGrpcTestBase {
    protected DataCloudJdbcManagedChannel channel;

    protected JdbcDriverStubProvider stubProvider;

    protected static HyperGrpcClientExecutor hyperGrpcClient;

    private final List<HyperServerProcess> servers = new ArrayList<>();

    private final List<ManagedChannel> channels = new ArrayList<>();

    private final List<DataCloudConnection> connections = new ArrayList<>();

    @AfterEach
    public void cleanUpServers() {
        servers.forEach(s -> {
            try {
                s.close();
            } catch (Exception e) {
                log.error("Failed to clean up hyper server process", e);
            }
        });

        channels.forEach(c -> {
            try {
                c.shutdownNow();
            } catch (Exception e) {
                log.error("Failed to clean up channel", e);
            }
        });

        connections.forEach(c -> {
            try {
                c.close();
            } catch (Exception e) {
                log.error("Failed to close data cloud connection", e);
            }
        });
    }

    protected DataCloudConnection getInterceptedClientConnection() {
        return getInterceptedClientConnection(HyperServerConfig.builder().build());
    }

    @SneakyThrows
    protected DataCloudConnection getInterceptedClientConnection(HyperServerConfig config) {

        val server = config.start();
        servers.add(server);

        val channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();

        channels.add(channel);

        val mocked = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext();

        GrpcMock.resetMappings();

        val stub = HyperServiceGrpc.newBlockingStub(channel);

        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getExecuteQueryMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::executeQuery);
        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getGetQueryInfoMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::getQueryInfo);
        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getGetQueryResultMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::getQueryResult);

        val conn = DataCloudConnection.of(mocked, new Properties());
        connections.add(conn);
        return conn;
    }

    public static <ReqT, RespT> void proxyStreamingMethod(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            MethodDescriptor<ReqT, RespT> mock,
            BiFunction<HyperServiceGrpc.HyperServiceBlockingStub, ReqT, Iterator<RespT>> method) {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(mock).willProxyTo((request, observer) -> {
            final String queryId;
            if (request instanceof salesforce.cdp.hyperdb.v1.QueryInfoParam) {
                queryId = ((salesforce.cdp.hyperdb.v1.QueryInfoParam) request).getQueryId();
            } else if (request instanceof salesforce.cdp.hyperdb.v1.QueryResultParam) {
                queryId = ((salesforce.cdp.hyperdb.v1.QueryResultParam) request).getQueryId();
            } else if (request instanceof salesforce.cdp.hyperdb.v1.CancelQueryParam) {
                queryId = ((salesforce.cdp.hyperdb.v1.CancelQueryParam) request).getQueryId();
            } else {
                queryId = null;
            }

            val response = method.apply(
                    queryId == null ? stub : stub.withInterceptors(new QueryIdHeaderInterceptor(queryId)), request);

            try {
                while (response.hasNext()) {
                    observer.onNext(response.next());
                }
            } catch (StatusRuntimeException ex) {
                observer.onError(ex);
                return;
            }

            observer.onCompleted();
        }));
    }

    @SneakyThrows
    public HyperGrpcClientExecutor setupClientWith(ClientInterceptor... interceptors) {
        val builder = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext()
                .intercept(interceptors);

        if (channel != null) {
            channel.close();
        }

        channel = DataCloudJdbcManagedChannel.of(builder);
        stubProvider = new JdbcDriverStubProvider(channel, false);
        val connection = DataCloudConnection.of(stubProvider, new Properties());

        return connection.getExecutor();
    }

    @BeforeEach
    public void setupClient() {
        hyperGrpcClient = setupClientWith();
    }

    @AfterEach
    @SneakyThrows
    public void cleanup() {
        if (channel != null) {
            stubProvider.close();
            channel.close();
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
