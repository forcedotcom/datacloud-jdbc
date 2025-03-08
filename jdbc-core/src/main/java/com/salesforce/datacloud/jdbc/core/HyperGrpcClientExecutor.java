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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.interceptor.QueryIdHeaderInterceptor;
import com.salesforce.datacloud.jdbc.util.PropertiesExtensions;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.jdbc.util.Unstable;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.CancelQueryParam;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultParam;
import salesforce.cdp.hyperdb.v1.ResultRange;

@Slf4j
@Builder(toBuilder = true)
public class HyperGrpcClientExecutor implements AutoCloseable {
    private static final int GRPC_INBOUND_MESSAGE_MAX_SIZE = 64 * 1024 * 1024;

    @NonNull private final ManagedChannel channel;

    @Getter
    private final QueryParam additionalQueryParams;

    private final QueryParam settingsQueryParams;

    @Builder.Default
    private int queryTimeout = -1;

    private final List<ClientInterceptor> interceptors;

    public static HyperGrpcClientExecutor of(@NonNull ManagedChannelBuilder<?> builder, @NonNull Properties properties)
            throws SQLException {
        val client = HyperGrpcClientExecutor.builder();

        val settings = ConnectionQuerySettings.of(properties).getSettings();
        if (!settings.isEmpty()) {
            client.settingsQueryParams(
                    QueryParam.newBuilder().putAllSettings(settings).build());
        }

        if (PropertiesExtensions.getBooleanOrDefault(properties, "grpc.enableRetries", Boolean.TRUE)) {
            int maxRetryAttempts =
                    PropertiesExtensions.getIntegerOrDefault(properties, "grpc.retryPolicy.maxAttempts", 5);
            builder.enableRetry()
                    .maxRetryAttempts(maxRetryAttempts)
                    .defaultServiceConfig(retryPolicy(maxRetryAttempts));
        }

        val channel = builder.maxInboundMessageSize(GRPC_INBOUND_MESSAGE_MAX_SIZE)
                .userAgent(DriverVersion.formatDriverInfo())
                .build();
        return client.channel(channel).build();
    }

    private static Map<String, Object> retryPolicy(int maxRetryAttempts) {
        return ImmutableMap.of(
                "methodConfig",
                ImmutableList.of(ImmutableMap.of(
                        "name", ImmutableList.of(Collections.EMPTY_MAP),
                        "retryPolicy",
                                ImmutableMap.of(
                                        "maxAttempts",
                                        String.valueOf(maxRetryAttempts),
                                        "initialBackoff",
                                        "0.5s",
                                        "maxBackoff",
                                        "30s",
                                        "backoffMultiplier",
                                        2.0,
                                        "retryableStatusCodes",
                                        ImmutableList.of("UNAVAILABLE")))));
    }

    public static HyperGrpcClientExecutor of(
            HyperGrpcClientExecutorBuilder builder, QueryParam additionalQueryParams, int queryTimeout) {
        return builder.additionalQueryParams(additionalQueryParams)
                .queryTimeout(queryTimeout)
                .build();
    }

    public Iterator<ExecuteQueryResponse> executeAdaptiveQuery(String sql) throws SQLException {
        return execute(sql, QueryParam.TransferMode.ADAPTIVE);
    }

    public Iterator<ExecuteQueryResponse> executeAsyncQuery(String sql) throws SQLException {
        return execute(sql, QueryParam.TransferMode.ASYNC);
    }

    public Iterator<ExecuteQueryResponse> executeQuery(String sql) throws SQLException {
        return execute(sql, QueryParam.TransferMode.SYNC);
    }

    public Iterator<QueryInfo> getQueryInfo(String queryId) {
        val param = getQueryInfoParam(queryId);
        return getStub(queryId).getQueryInfo(param);
    }

    @Unstable
    public Stream<DataCloudQueryStatus> getQueryStatus(String queryId) {
        val iterator = getQueryInfo(queryId);
        return StreamUtilities.toStream(iterator)
                .map(DataCloudQueryStatus::of)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public void cancel(String queryId) {
        val request = CancelQueryParam.newBuilder().setQueryId(queryId).build();
        val stub = getStub(queryId);
        stub.cancelQuery(request);
    }

    public Iterator<QueryResult> getQueryResult(String queryId, long offset, long limit, boolean omitSchema) {
        val rowRange =
                ResultRange.newBuilder().setRowOffset(offset).setRowLimit(limit).setByteLimit(1024);

        final QueryResultParam param = QueryResultParam.newBuilder()
                .setQueryId(queryId)
                .setResultRange(rowRange)
                .setOmitSchema(omitSchema)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .build();

        return getStub(queryId).getQueryResult(param);
    }

    public Iterator<QueryResult> getQueryResult(String queryId, long chunkId, boolean omitSchema) {
        val param = getQueryResultParam(queryId, chunkId, omitSchema);
        return getStub(queryId).getQueryResult(param);
    }

    private QueryParam getQueryParams(String sql, QueryParam.TransferMode transferMode) {
        val builder = QueryParam.newBuilder()
                .setQuery(sql)
                .setTransferMode(transferMode)
                .setOutputFormat(OutputFormat.ARROW_IPC);

        if (additionalQueryParams != null) {
            builder.mergeFrom(additionalQueryParams);
        }

        if (settingsQueryParams != null) {
            builder.mergeFrom(settingsQueryParams);
        }

        return builder.build();
    }

    private QueryResultParam getQueryResultParam(String queryId, long chunkId, boolean omitSchema) {
        val builder = QueryResultParam.newBuilder()
                .setQueryId(queryId)
                .setChunkId(chunkId)
                .setOmitSchema(omitSchema)
                .setOutputFormat(OutputFormat.ARROW_IPC);

        return builder.build();
    }

    private QueryInfoParam getQueryInfoParam(String queryId) {
        return QueryInfoParam.newBuilder()
                .setQueryId(queryId)
                .setStreaming(true)
                .build();
    }

    private Iterator<ExecuteQueryResponse> execute(String sql, QueryParam.TransferMode mode) throws SQLException {
        val request = getQueryParams(sql, mode);
        return getStub().executeQuery(request);
    }

    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    private final HyperServiceGrpc.HyperServiceBlockingStub stub = lazyStub();

    private HyperServiceGrpc.HyperServiceBlockingStub lazyStub() {
        HyperServiceGrpc.HyperServiceBlockingStub result = HyperServiceGrpc.newBlockingStub(channel);

        log.info("Stub will execute query. deadline={}", queryTimeout > 0 ? Duration.ofSeconds(queryTimeout) : "none");

        if (interceptors != null && !interceptors.isEmpty()) {
            log.info("Registering additional interceptors. count={}", interceptors.size());
            result = result.withInterceptors(interceptors.toArray(new ClientInterceptor[0]));
        }

        if (queryTimeout > 0) {
            return result.withDeadlineAfter(queryTimeout, TimeUnit.SECONDS);
        } else {
            return result;
        }
    }

    private HyperServiceGrpc.HyperServiceBlockingStub getStub(@NonNull String queryId) {
        val queryIdHeaderInterceptor = new QueryIdHeaderInterceptor(queryId);
        return getStub().withInterceptors(queryIdHeaderInterceptor);
    }

    @Override
    public void close() throws Exception {
        if (channel.isShutdown() || channel.isTerminated()) {
            return;
        }

        channel.shutdown();
    }
}
