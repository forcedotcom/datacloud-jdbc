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

    private Settings.StatementSettings statementSettings;

    // The target maximum number of rows for a query. The default means disabled.
    @Getter(AccessLevel.PACKAGE)
    private int targetMaxRows = 0;

    // The target maximum number of bytes per RPC response. This is only relevant when `targetMaxRows > 0`.
    // The default means disabled.
    @Getter(AccessLevel.PACKAGE)
    private int targetMaxBytes = 0;

    public DataCloudStatement(@NonNull DataCloudConnection connection) {
        this.connection = connection;
        statementSettings = connection.getSettings().getStatementSettings();
    }

    protected HyperGrpcClientExecutor getQueryExecutor() throws DataCloudJDBCException {
        val stub = connection.getStub();
        return HyperGrpcClientExecutor.of(stub, statementSettings.getQuerySettings());
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
        resultSet = executeAdaptiveQuery(sql, client, getEffectiveQueryTimeoutDuration());
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Entering executeQuery");
        val client = getQueryExecutor();
        resultSet = executeAdaptiveQuery(sql, client, getEffectiveQueryTimeoutDuration());
        return resultSet;
    }

    protected DataCloudResultSet executeAdaptiveQuery(String sql, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        listener = targetMaxRows > 0
                ? AdaptiveQueryStatusListener.of(
                        sql, client, getEffectiveQueryTimeoutDuration(), targetMaxRows, targetMaxBytes)
                : AdaptiveQueryStatusListener.of(sql, client, getEffectiveQueryTimeoutDuration());

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
        listener = AsyncQueryStatusListener.of(sql, client, getEffectiveQueryTimeoutDuration());
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

    // The maximum byte size limit for a row based RPC response. While the server enforces a max as well, also having
    // the constant available on the client side allows to set appropriate default values and also to provide immediate
    // feedback on the
    // ``setResultSetConstraints`` method.
    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = 20971520;
    // The minimal byte size limit for a row based RPC response. The driver enforces this to guard against code that
    // accidentally provides the limit in megabytes.
    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = 1024;

    public void clearResultSetConstraints() throws DataCloudJDBCException {
        setResultSetConstraints(0, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    /**
     * Sets the constraints that would limit the number of rows and overall bytes in any ResultSet object generated by this Statement.
     * This is used to tell the database the maximum number of rows and bytes to return, you may get less than expected because of this.
     *
     * @param maxRows The target maximum number of rows a ResultSet can have, zero means there is no limit.
     * @param maxBytes The target maximum byte size a ResultSet can be,
     *                 must fall in the range {@link DataCloudStatement#HYPER_MIN_ROW_LIMIT_BYTE_SIZE}
     *                 and {@link DataCloudStatement#HYPER_MAX_ROW_LIMIT_BYTE_SIZE}
     * @throws DataCloudJDBCException If the target maximum byte size is outside the aforementioned range
     */
    public void setResultSetConstraints(int maxRows, int maxBytes) throws DataCloudJDBCException {
        if (maxRows < 0) {
            throw new DataCloudJDBCException(
                    "setResultSetConstraints maxRows must be set to 0 to be disabled but was " + maxRows);
        }

        if (maxBytes < HYPER_MIN_ROW_LIMIT_BYTE_SIZE || maxBytes > HYPER_MAX_ROW_LIMIT_BYTE_SIZE) {
            throw new DataCloudJDBCException(String.format(
                    "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                    maxBytes, HYPER_MIN_ROW_LIMIT_BYTE_SIZE, HYPER_MAX_ROW_LIMIT_BYTE_SIZE));
        }

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

    /**
     * This translates the user visible query timeout behavior (zero is infinite) to the internal expectation of the code
     * (which would interpret zero as literal zero timeout). Thus zero is translated to a practically infinite timeout by using
     * a large second count.
     *
     * Note: This behavior should be fixed in the future by changing the internal code to interpret zero as not setting any timeout at all.
     * @return The effective query timeout duration.
     */
    protected Duration getEffectiveQueryTimeoutDuration() {
        if (statementSettings.getQueryTimeout() == Duration.ZERO) {
            return Duration.ofSeconds(Constants.INFINITE_QUERY_TIMEOUT);
        }
        return statementSettings.getQueryTimeout();
    }

    /**
     * @return The query timeout for this statement in seconds.
     */
    @Override
    public int getQueryTimeout() {
        return (int) statementSettings.getQueryTimeout().getSeconds();
    }

    /**
     * Sets the query timeout for this statement. A zero or negative value is interpreted as infinite timeout.
     * @param seconds The query timeout in seconds.
     */
    @Override
    public void setQueryTimeout(int seconds) {
        // Zero or negative values are interpreted as infinite timeout
        // We use the ``withQueryTimeout`` method to create a new statement settings object with the updated timeout
        // to avoid changing the shared object with the default connection query timeout.
        if (seconds <= 0) {
            statementSettings = statementSettings.withQueryTimeout(Duration.ZERO);
        } else {
            statementSettings = statementSettings.withQueryTimeout(Duration.ofSeconds(seconds));
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
