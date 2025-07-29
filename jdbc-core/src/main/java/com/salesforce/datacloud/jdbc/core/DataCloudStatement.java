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

import com.salesforce.datacloud.jdbc.core.fsm.AdaptiveQueryResultIterator;
import com.salesforce.datacloud.jdbc.core.fsm.AsyncQueryResultHandle;
import com.salesforce.datacloud.jdbc.core.fsm.QueryResultHandle;
import com.salesforce.datacloud.jdbc.core.fsm.QueryResultIterator;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class DataCloudStatement implements Statement, AutoCloseable {
    @Getter
    protected final DataCloudConnection connection;

    protected ResultSet resultSet;

    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY = "Write is not supported in Data Cloud query";
    protected static final String BATCH_EXECUTION_IS_NOT_SUPPORTED =
            "Batch execution is not supported in Data Cloud query";
    protected static final String CHANGE_FETCH_DIRECTION_IS_NOT_SUPPORTED = "Changing fetch direction is not supported";

    protected StatementProperties statementProperties;

    /**
     * The target maximum number of rows for a query. The default means disabled.
     */
    @Getter(AccessLevel.PACKAGE)
    private int targetMaxRows = 0;

    /**
     * The target maximum number of bytes per RPC response. This is only relevant when `targetMaxRows > 0`.
     * The default means disabled.
     */
    @Getter(AccessLevel.PACKAGE)
    private int targetMaxBytes = 0;

    public DataCloudStatement(@NonNull DataCloudConnection connection) {
        this.connection = connection;
        this.statementProperties = connection.getConnectionProperties().getStatementProperties();
    }

    protected HyperGrpcClientExecutor getQueryClient(QueryTimeout queryTimeout) throws DataCloudJDBCException {
        val stub = connection.getStub();
        val querySettings = new HashMap<>(statementProperties.getQuerySettings());
        if (!queryTimeout.getServerQueryTimeout().isZero()) {
            querySettings.put(
                    "query_timeout", queryTimeout.getServerQueryTimeout().toMillis() + "ms");
        }
        return HyperGrpcClientExecutor.of(stub, querySettings);
    }

    @Getter
    protected QueryResultHandle queryHandle;

    private void assertQueryExecuted() throws DataCloudJDBCException {
        if (queryHandle == null) {
            throw new DataCloudJDBCException("a query was not executed before attempting to access results");
        }
    }

    /**
     * @return The Data Cloud query id of the last executed query from this statement.
     * @throws SQLException throws an exception if a query has not been executed from this statement
     */
    public String getQueryId() throws DataCloudJDBCException {
        assertQueryExecuted();
        return queryHandle.getQueryId();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("Entering execute");
        resultSet = executeAdaptiveQuery(sql);
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Entering executeQuery");
        resultSet = executeAdaptiveQuery(sql);
        return resultSet;
    }

    private DataCloudResultSet executeAdaptiveQuery(String sql) throws SQLException {
        val queryTimeout = QueryTimeout.of(
                statementProperties.getQueryTimeout(), statementProperties.getQueryTimeoutLocalEnforcementDelay());
        val client = getQueryClient(queryTimeout);

        queryHandle = targetMaxRows > 0
                ? AdaptiveQueryResultIterator.of(sql, client, queryTimeout, targetMaxRows, targetMaxBytes)
                : AdaptiveQueryResultIterator.of(sql, client, queryTimeout);

        resultSet = StreamingResultSet.of((AdaptiveQueryResultIterator) queryHandle);
        log.info("executeAdaptiveQuery completed. queryId={}", queryHandle.getQueryId());
        return (DataCloudResultSet) resultSet;
    }

    public DataCloudStatement executeAsyncQuery(String sql) throws SQLException {
        log.debug("Entering executeAsyncQuery");
        val queryTimeout = QueryTimeout.of(
                statementProperties.getQueryTimeout(), statementProperties.getQueryTimeoutLocalEnforcementDelay());
        val client = getQueryClient(queryTimeout);
        queryHandle = AsyncQueryResultHandle.of(sql, client, queryTimeout);
        log.info("executeAsyncQuery completed. queryId={}", queryHandle.getQueryId());
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

    public void clearResultSetConstraints() throws DataCloudJDBCException {
        setResultSetConstraints(0, RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    /**
     * Sets the constraints that would limit the number of rows and overall bytes in any ResultSet object generated by this Statement.
     * This is used to tell the database the maximum number of rows and bytes to return, you may get less than expected because of this.
     *
     * @param maxRows The maximum number of rows a ResultSet can have, zero means there is no limit.
     * @param maxBytes The maximum byte size a ResultSet can be,
     *                 must fall in the range {@link RowBased#HYPER_MIN_ROW_LIMIT_BYTE_SIZE}
     *                 and {@link RowBased#HYPER_MAX_ROW_LIMIT_BYTE_SIZE}
     * @throws DataCloudJDBCException If the target maximum byte size is outside the aforementioned range
     */
    public void setResultSetConstraints(int maxRows, int maxBytes) throws DataCloudJDBCException {
        if (maxRows < 0) {
            throw new DataCloudJDBCException(
                    "setResultSetConstraints maxRows must be set to 0 to be disabled but was " + maxRows);
        }

        if (maxBytes < RowBased.HYPER_MIN_ROW_LIMIT_BYTE_SIZE || maxBytes > RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE) {
            throw new DataCloudJDBCException(String.format(
                    "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                    maxBytes, RowBased.HYPER_MIN_ROW_LIMIT_BYTE_SIZE, RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE));
        }

        targetMaxRows = maxRows;
        targetMaxBytes = maxBytes;
    }

    /**
     * @see DataCloudStatement#setResultSetConstraints
     * @param maxRows The target maximum number of rows a ResultSet can have, zero means there is no limit.
     */
    public void setResultSetConstraints(int maxRows) throws DataCloudJDBCException {
        setResultSetConstraints(maxRows, RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {}

    @Override
    public void setEscapeProcessing(boolean enable) {}

    /**
     * @return The query timeout for this statement in seconds.
     */
    @Override
    public int getQueryTimeout() {
        return (int) statementProperties.getQueryTimeout().getSeconds();
    }

    /**
     * Sets the query timeout for this statement. A zero or negative value is interpreted as infinite timeout.
     * @param seconds The query timeout in seconds.
     */
    @Override
    public void setQueryTimeout(int seconds) {
        // We use the ``withQueryTimeout`` method to create a new statement properties object with the updated timeout
        // to avoid changing the shared object with the default connection query timeout.
        if (seconds <= 0) {
            statementProperties = statementProperties.withQueryTimeout(Duration.ZERO);
        } else {
            statementProperties = statementProperties.withQueryTimeout(Duration.ofSeconds(seconds));
        }
    }

    /**
     * Cancels the most recently executed query from this statement.
     */
    @Override
    public void cancel() throws DataCloudJDBCException {
        if (queryHandle == null) {
            log.warn("There was no in-progress query registered with this statement to cancel");
            return;
        }

        val queryId = getQueryId();
        HyperGrpcClientExecutor.forSubmittedQuery(connection.getStub()).cancel(queryId);
    }

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
        return logTimedValue(
                () -> {
                    assertQueryExecuted();
                    if (resultSet == null && queryHandle instanceof QueryResultIterator) {
                        resultSet = StreamingResultSet.of((QueryResultIterator) queryHandle);
                    } else if (resultSet == null) {
                        log.warn(
                                "Prefer acquiring async result sets from helper methods DataCloudConnection::getChunkBasedResultSet and DataCloudConnection::getRowBasedResultSet. We will wait for the query's results to be produced in their entirety before returning a result set.");
                        val status = connection.waitForQueryStatus(
                                queryHandle.getQueryId(),
                                Duration.ofDays(10),
                                DataCloudQueryStatus::allResultsProduced);
                        resultSet =
                                connection.getChunkBasedResultSet(queryHandle.getQueryId(), 0, status.getChunkCount());
                    }
                    log.info("resultSet created for queryId={}", queryHandle.getQueryId());
                    return resultSet;
                },
                "getResultSet",
                log);
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
    public void setFetchDirection(int direction) throws SQLException {
        throw new DataCloudJDBCException(CHANGE_FETCH_DIRECTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        assertQueryExecuted();
        return resultSet.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        assertQueryExecuted();
        return resultSet.getConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        assertQueryExecuted();
        return resultSet.getType();
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
