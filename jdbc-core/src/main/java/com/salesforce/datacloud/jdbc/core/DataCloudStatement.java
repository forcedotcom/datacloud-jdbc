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

import static com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor.HYPER_MAX_ROW_LIMIT_BYTE_SIZE;
import static com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor.HYPER_MIN_ROW_LIMIT_BYTE_SIZE;
import static com.salesforce.datacloud.jdbc.util.Constants.DEFAULT_QUERY_TIMEOUT;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getIntegerOrDefault;

import com.salesforce.datacloud.jdbc.core.listener.AdaptiveQueryStatusListener;
import com.salesforce.datacloud.jdbc.core.listener.AsyncQueryStatusListener;
import com.salesforce.datacloud.jdbc.core.listener.QueryStatusListener;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
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
    private static final String QUERY_TIMEOUT = "queryTimeout";

    private int queryTimeout;

    public DataCloudStatement(@NonNull DataCloudConnection connection) {
        this.connection = connection;
        val properties = connection.getClientInfo();
        this.queryTimeout = getIntegerOrDefault(properties, QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT);
        this.targetMaxBytes = getIntegerOrDefault(properties, Constants.BYTE_LIMIT, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    protected HyperGrpcClientExecutor getQueryExecutor() throws DataCloudJDBCException {
        val properties = connection.getClientInfo();
        val stub = connection.getChannel().getStub(properties, getQueryTimeoutDuration());
        return HyperGrpcClientExecutor.of(stub, properties, targetMaxBytes);
    }

    protected QueryStatusListener listener;

    private void assertQueryExecuted() throws DataCloudJDBCException {
        if (listener == null) {
            throw new DataCloudJDBCException("a query was not executed before attempting to access results");
        }
    }

    /**
     * @return The Data Cloud query id of the last executed query from this statement.
     * @throws SQLException throws an exception if a query has not been executed from this statement
     */
    public String getQueryId() throws DataCloudJDBCException {
        assertQueryExecuted();

        return listener.getQueryId();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("Entering execute");
        val client = getQueryExecutor();
        resultSet = executeAdaptiveQuery(sql, client, getQueryTimeoutDuration());
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Entering executeQuery");
        val client = getQueryExecutor();
        resultSet = executeAdaptiveQuery(sql, client, getQueryTimeoutDuration());
        return resultSet;
    }

    protected DataCloudResultSet executeAdaptiveQuery(String sql, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        val queryTimeout = resolveQueryTimeout(timeout);

        listener = targetMaxRows > 0
                ? AdaptiveQueryStatusListener.of(sql, client, queryTimeout, targetMaxRows)
                : AdaptiveQueryStatusListener.of(sql, client, queryTimeout);

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
        listener = AsyncQueryStatusListener.of(sql, client, getQueryTimeoutDuration());
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

    @Getter(AccessLevel.PACKAGE)
    private int targetMaxRows;

    @Getter(AccessLevel.PACKAGE)
    private int targetMaxBytes;

    public void clearResultSetConstraints() throws DataCloudJDBCException {
        setResultSetConstraints(0, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    private void checkResultSetConstraints(int maxRows, int maxBytes) throws DataCloudJDBCException {
        if (maxRows < 0) {
            throw new DataCloudJDBCException(
                    "setResultSetConstraints maxRows must be set to 0 to be disabled but was " + maxRows);
        }

        if (maxBytes < HYPER_MIN_ROW_LIMIT_BYTE_SIZE || maxBytes > HYPER_MAX_ROW_LIMIT_BYTE_SIZE) {
            throw new DataCloudJDBCException(String.format(
                    "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                    maxBytes, HYPER_MIN_ROW_LIMIT_BYTE_SIZE, HYPER_MAX_ROW_LIMIT_BYTE_SIZE));
        }
    }

    /**
     * Sets the constraints that would limit the number of rows and overall bytes in any ResultSet object generated by this Statement.
     * This is used to tell the database the maximum number of rows and bytes to return, you may get less than expected because of this.
     *
     * @param maxRows The target maximum number of rows a ResultSet can have, zero means there is no limit.
     * @param maxBytes The target maximum byte size a ResultSet can be,
     *                 must fall in the range {@link HyperGrpcClientExecutor#HYPER_MIN_ROW_LIMIT_BYTE_SIZE}
     *                 and {@link HyperGrpcClientExecutor#HYPER_MAX_ROW_LIMIT_BYTE_SIZE}
     * @throws DataCloudJDBCException If the target maximum byte size is outside the aforementioned range
     */
    public void setResultSetConstraints(int maxRows, int maxBytes) throws DataCloudJDBCException {
        checkResultSetConstraints(maxRows, maxBytes);

        targetMaxRows = maxRows;
        targetMaxBytes = maxBytes;
    }

    /**
     * @see DataCloudStatement#setResultSetConstraints
     * @param maxRows The target maximum number of rows a ResultSet can have, zero means there is no limit.
     */
    public void setResultSetConstraints(int maxRows) throws DataCloudJDBCException {
        setResultSetConstraints(maxRows, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {}

    @Override
    public void setEscapeProcessing(boolean enable) {}

    protected Duration resolveQueryTimeout(Duration timeout) {
        if (timeout == null) {
            return getQueryTimeoutDuration();
        }

        if (timeout.isZero() || timeout.isNegative()) {
            return Duration.ofSeconds(DEFAULT_QUERY_TIMEOUT);
        }

        return timeout;
    }

    protected Duration getQueryTimeoutDuration() {
        return Duration.ofSeconds(getQueryTimeout());
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        if (seconds <= 0) {
            this.queryTimeout = DEFAULT_QUERY_TIMEOUT;
        } else {
            this.queryTimeout = seconds;
        }
    }

    /**
     * Cancels the most recently executed query from this statement.
     */
    @Override
    public void cancel() throws DataCloudJDBCException {
        if (listener == null) {
            log.warn("There was no in-progress query registered with this statement to cancel");
            return;
        }

        val queryId = getQueryId();
        val executor = getQueryExecutor();
        executor.cancel(queryId);
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
        log.debug("Entering getResultSet");
        assertQueryExecuted();

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
