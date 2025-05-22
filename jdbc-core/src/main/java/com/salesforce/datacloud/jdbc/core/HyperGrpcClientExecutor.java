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

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValue;

import com.salesforce.datacloud.jdbc.core.partial.DataCloudQueryPolling;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.interceptor.QueryIdHeaderInterceptor;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
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

/**
 * Although this class is public, we do not consider it to be part of our API.
 * It is for internal use only until it stabilizes.
 */
@Builder(access = AccessLevel.PRIVATE)
@Slf4j
@Unstable
public class HyperGrpcClientExecutor {

    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = 20971520;

    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = 1024;

    @NonNull private final HyperServiceGrpc.HyperServiceBlockingStub stub;

    private final int byteLimit;

    private final QueryParam settingsQueryParams;

    private QueryParam additionalQueryParams;

    public static HyperGrpcClientExecutor of(
            @NonNull HyperServiceGrpc.HyperServiceBlockingStub stub, @NonNull Properties properties) {
        return of(stub, properties, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    public static HyperGrpcClientExecutor of(
            @NonNull HyperServiceGrpc.HyperServiceBlockingStub stub, @NonNull Properties properties, int byteLimit) {
        val builder = HyperGrpcClientExecutor.builder().stub(stub).byteLimit(byteLimit);

        val settings = ConnectionQuerySettings.of(properties).getSettings();
        if (!settings.isEmpty()) {
            builder.settingsQueryParams(
                    QueryParam.newBuilder().putAllSettings(settings).build());
        }

        return builder.build();
    }

    public HyperGrpcClientExecutor withQueryParams(QueryParam additionalQueryParams) {
        this.additionalQueryParams = additionalQueryParams;
        return this;
    }

    public Iterator<ExecuteQueryResponse> executeQuery(String sql) throws SQLException {
        return execute(sql, QueryParam.TransferMode.ADAPTIVE, QueryParam.newBuilder());
    }

    public Iterator<ExecuteQueryResponse> executeQuery(String sql, long maxRows) throws SQLException {
        val builder = QueryParam.newBuilder();
        if (maxRows > 0) {
            log.info("setting row limit query. maxRows={}, byteLimit={}", maxRows, byteLimit);
            val range = ResultRange.newBuilder().setRowLimit(maxRows).setByteLimit(byteLimit);
            builder.setResultRange(range);
        }

        return execute(sql, QueryParam.TransferMode.ADAPTIVE, builder);
    }

    public Iterator<ExecuteQueryResponse> executeAsyncQuery(String sql) throws SQLException {
        return execute(sql, QueryParam.TransferMode.ASYNC, QueryParam.newBuilder());
    }

    public Iterator<QueryInfo> getQueryInfo(String queryId) throws DataCloudJDBCException {
        return logTimedValue(
                () -> {
                    val param = getQueryInfoParam(queryId);
                    return getStub(queryId).getQueryInfo(param);
                },
                "getQueryInfo queryId=" + queryId,
                log);
    }

    public DataCloudQueryStatus waitForRowsAvailable(
            String queryId, long offset, long limit, Duration timeout, boolean allowLessThan)
            throws DataCloudJDBCException {
        val stub = getStub(queryId);
        return DataCloudQueryPolling.waitForRowsAvailable(stub, queryId, offset, limit, timeout, allowLessThan);
    }

    public DataCloudQueryStatus waitForChunksAvailable(
            String queryId, long offset, long limit, Duration timeout, boolean allowLessThan)
            throws DataCloudJDBCException {
        val stub = getStub(queryId);
        return DataCloudQueryPolling.waitForChunksAvailable(stub, queryId, offset, limit, timeout, allowLessThan);
    }

    public DataCloudQueryStatus waitForQueryStatus(
            String queryId, Duration timeout, Predicate<DataCloudQueryStatus> predicate) throws DataCloudJDBCException {
        val stub = getStub(queryId);
        return DataCloudQueryPolling.waitForQueryStatus(stub, queryId, timeout, predicate);
    }

    public Stream<DataCloudQueryStatus> getQueryStatus(String queryId) throws DataCloudJDBCException {
        val iterator = getQueryInfo(queryId);
        return StreamUtilities.toStream(iterator)
                .map(DataCloudQueryStatus::of)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public void cancel(String queryId) throws DataCloudJDBCException {
        logTimedValue(
                () -> {
                    val request =
                            CancelQueryParam.newBuilder().setQueryId(queryId).build();
                    val stub = getStub(queryId);
                    stub.cancelQuery(request);
                    return null;
                },
                "cancel queryId=" + queryId,
                log);
    }

    public Iterator<QueryResult> getQueryResult(String queryId, long offset, long rowLimit, boolean omitSchema)
            throws DataCloudJDBCException {
        val rowRange = ResultRange.newBuilder()
                .setRowOffset(offset)
                .setRowLimit(rowLimit)
                .setByteLimit(byteLimit);

        val param = QueryResultParam.newBuilder()
                .setQueryId(queryId)
                .setResultRange(rowRange)
                .setOmitSchema(omitSchema)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .build();

        val message = String.format(
                "getQueryResult queryId=%s, offset=%d, rowLimit=%d, byteLimit=%d, omitSchema=%s",
                queryId, offset, rowLimit, byteLimit, omitSchema);
        return logTimedValue(() -> getStub(queryId).getQueryResult(param), message, log);
    }

    public Iterator<QueryResult> getQueryResult(String queryId, long chunkId, boolean omitSchema) {
        val param = getQueryResultParam(queryId, chunkId, omitSchema);
        return getStub(queryId).getQueryResult(param);
    }

    private QueryParam getQueryParams(String sql, QueryParam.Builder builder) {
        builder.setQuery(sql).setOutputFormat(OutputFormat.ARROW_IPC);

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

    private Iterator<ExecuteQueryResponse> execute(String sql, QueryParam.TransferMode mode, QueryParam.Builder builder)
            throws SQLException {
        val message = "executeQuery. mode=" + mode.name();
        builder.setTransferMode(mode);
        return logTimedValue(
                () -> {
                    val request = getQueryParams(sql, builder);
                    return stub.executeQuery(request);
                },
                message,
                log);
    }

    private HyperServiceGrpc.HyperServiceBlockingStub getStub(@NonNull String queryId) {
        val queryIdHeaderInterceptor = new QueryIdHeaderInterceptor(queryId);
        return stub.withInterceptors(queryIdHeaderInterceptor);
    }
}
