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

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getIntegerOrDefault;

import com.salesforce.datacloud.jdbc.core.listener.AdaptiveQueryStatusListener;
import com.salesforce.datacloud.jdbc.core.listener.AsyncQueryStatusListener;
import com.salesforce.datacloud.jdbc.core.listener.QueryStatusListener;
import com.salesforce.datacloud.jdbc.core.listener.SyncQueryStatusListener;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * TODO: DataCloudStatement should be an interface that makes more clear the public methods available from this type,
 * this change should be mostly non-breaking, but it does require a fair amount of refactors to do in test to do it now:
 * DataCloudStatement::getRowBasedResultSet(RowIndex, Count) -> DataCloudResultSet
 * DataCloudStatement::getChunkBasedResultSet(ChunkId) -> DataCloudResultSet
 * DataCloudStatement::executeAdaptiveQuery(Query) -> DataCloudResultSet
 * DataCloudStatement::executeSyncQuery(Query) -> DataCloudResultSet
 * DataCloudStatement::executeAsyncQuery(Query) -> DataCloudStatement
 */
@Slf4j
public class DataCloudStatement implements Statement {
    protected ResultSet resultSet;

    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY = "Write is not supported in Data Cloud query";
    protected static final String BATCH_EXECUTION_IS_NOT_SUPPORTED =
            "Batch execution is not supported in Data Cloud query";
    private static final String QUERY_TIMEOUT = "queryTimeout";
    public static final int DEFAULT_QUERY_TIMEOUT = 3 * 60 * 60;

    protected final DataCloudConnection dataCloudConnection;

    private int queryTimeout;

    public DataCloudStatement(@NonNull DataCloudConnection connection) {
        this.dataCloudConnection = connection;
        this.queryTimeout = getIntegerOrDefault(connection.getProperties(), QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT);
    }

    protected QueryStatusListener listener;

    protected HyperGrpcClientExecutor getQueryExecutor() {
        return getQueryExecutor(null);
    }

    protected HyperGrpcClientExecutor getQueryExecutor(QueryParam additionalQueryParams) {
        val clientBuilder = dataCloudConnection.getExecutor().toBuilder();

        clientBuilder.interceptors(dataCloudConnection.getInterceptors());

        if (additionalQueryParams != null) {
            clientBuilder.additionalQueryParams(additionalQueryParams);
        }

        return clientBuilder.queryTimeout(getQueryTimeout()).build();
    }

    private void assertQueryActive() throws SQLException {
        if (listener == null) {
            throw new DataCloudJDBCException("a query was not executed before attempting to access results");
        }
    }

    private void assertQueryReady() throws SQLException {
        val status = getStatus();
        val ready = status.isResultsProduced() || status.isExecutionFinished();
        if (!ready) {
            throw new DataCloudJDBCException("query results were not ready");
        }
    }

    public DataCloudQueryStatus getStatus() throws SQLException {
        assertQueryActive();
        return listener.getStatus();
    }

    public boolean isReady() throws SQLException {
        val status = getStatus();
        return Optional.ofNullable(status)
                .map(t -> t.isResultsProduced() || t.isExecutionFinished())
                .orElse(false);
    }

    @Getter(lazy = true, value = AccessLevel.PROTECTED)
    private final boolean useFSM = getBooleanOrDefault(dataCloudConnection.getProperties(), Constants.FORCE_FSM, false);

    @Getter(lazy = true, value = AccessLevel.PROTECTED)
    private final boolean forceSync =
            getBooleanOrDefault(dataCloudConnection.getProperties(), Constants.FORCE_SYNC, false);

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("Entering execute");
        this.resultSet = executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Entering executeQuery");

        resultSet = isForceSync() ? executeSyncQuery(sql) : executeAdaptiveQuery(sql);

        return resultSet;
    }

    public DataCloudResultSet executeSyncQuery(String sql) throws SQLException {
        log.debug("Entering executeSyncQuery");
        val client = getQueryExecutor();
        return executeSyncQuery(sql, client);
    }

    protected DataCloudResultSet executeSyncQuery(String sql, HyperGrpcClientExecutor client) throws SQLException {
        listener = SyncQueryStatusListener.of(sql, client);
        resultSet = listener.generateResultSet();
        log.info("executeSyncQuery completed. queryId={}", listener.getQueryId());
        return (DataCloudResultSet) resultSet;
    }

    public DataCloudResultSet executeAdaptiveQuery(String sql) throws SQLException {
        log.debug("Entering executeAdaptiveQuery");
        val client = getQueryExecutor();
        val timeout = Duration.ofSeconds(getQueryTimeout());
        return executeAdaptiveQuery(sql, client, timeout);
    }

    protected DataCloudResultSet executeAdaptiveQuery(String sql, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        listener = AdaptiveQueryStatusListener.of(sql, client, timeout);
        resultSet = listener.generateResultSet();
        log.info("executeAdaptiveQuery completed. queryId={}", listener.getQueryId());
        return (DataCloudResultSet) resultSet;
    }

    public DataCloudStatement executeAsyncQuery(String sql) throws SQLException {
        log.debug("Entering executeAsyncQuery");
        val client = getQueryExecutor();
        return executeAsyncQuery(sql, client);
    }

    protected DataCloudStatement executeAsyncQuery(String sql, HyperGrpcClientExecutor client) throws SQLException {
        listener = AsyncQueryStatusListener.of(sql, client);
        log.info("executeAsyncQuery completed. queryId={}", listener.getQueryId());
        return this;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void close() throws SQLException {
        log.debug("Entering close");
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new DataCloudJDBCException(e);
            }
        }
        log.debug("Exiting close");
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {}

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {}

    @Override
    public void setEscapeProcessing(boolean enable) {}

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        if (seconds < 0) {
            this.queryTimeout = DEFAULT_QUERY_TIMEOUT;
        } else {
            this.queryTimeout = seconds;
        }
    }

    @Override
    public void cancel() {}

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public void setCursorName(String name) {}

    @Override
    public ResultSet getResultSet() throws SQLException {
        log.debug("Entering getResultSet");
        assertQueryReady();

        if (resultSet == null) {
            resultSet = listener.generateResultSet();
        }
        log.info("getResultSet completed. queryId={}", listener.getQueryId());
        return resultSet;
    }

    @Override
    public int getUpdateCount() {
        return 0;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) {}

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return 0;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public Connection getConnection() {
        return dataCloudConnection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) {}

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {}

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iFace) throws SQLException {
        if (iFace.isInstance(this)) {
            return iFace.cast(this);
        }
        throw new DataCloudJDBCException("Cannot unwrap to " + iFace.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iFace) {
        return iFace.isInstance(this);
    }
}
