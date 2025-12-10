/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValue;

import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.protocol.*;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.StatusRuntimeException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryParam;

@Slf4j
public class DataCloudStatement implements Statement, AutoCloseable {
    @Getter
    protected final DataCloudConnection connection;

    protected ResultSet resultSet;

    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY = "Feature is not supported in Data Cloud query";
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

    protected ExecuteQueryParamBuilder getQueryParamBuilder(QueryTimeout queryTimeout) throws SQLException {
        val querySettings = new HashMap<>(statementProperties.getQuerySettings());
        if (!queryTimeout.getServerQueryTimeout().isZero()) {
            querySettings.put(
                    "query_timeout", queryTimeout.getServerQueryTimeout().toMillis() + "ms");
        }
        return ExecuteQueryParamBuilder.of(querySettings);
    }

    @Getter
    protected QueryAccessHandle queryHandle;

    private void assertQueryExecuted() throws SQLException {
        if (queryHandle == null) {
            throw new SQLException("a query was not executed before attempting to access results");
        }
    }

    /**
     * @return The Data Cloud query id of the last executed query from this statement.
     * @throws SQLException throws an exception if a query has not been executed from this statement
     */
    public String getQueryId() throws SQLException {
        assertQueryExecuted();
        return queryHandle.getQueryStatus().getQueryId();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("Entering execute");
        try {
            executeAdaptiveQuery(sql);
        } catch (StatusRuntimeException ex) {
            String queryId = null;
            if (queryHandle != null && queryHandle.getQueryStatus() != null) {
                queryId = queryHandle.getQueryStatus().getQueryId();
            }
            val includeCustomerDetail = connection.getConnectionProperties().isIncludeCustomerDetailInReason();
            throw QueryExceptionHandler.createException(includeCustomerDetail, sql, queryId, ex);
        }
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Entering executeQuery");
        val includeCustomerDetail = connection.getConnectionProperties().isIncludeCustomerDetailInReason();
        try {
            val iterator = executeAdaptiveQuery(sql);
            val arrowStream = SQLExceptionQueryResultIterator.createSqlExceptionArrowStreamReader(
                    iterator, includeCustomerDetail, iterator.getQueryStatus().getQueryId(), sql);
            resultSet =
                    StreamingResultSet.of(arrowStream, iterator.getQueryStatus().getQueryId());
            log.info(
                    "executeAdaptiveQuery completed. queryId={}",
                    queryHandle.getQueryStatus().getQueryId());
            return resultSet;
        } catch (StatusRuntimeException ex) {
            String queryId = null;
            if (queryHandle != null && queryHandle.getQueryStatus() != null) {
                queryId = queryHandle.getQueryStatus().getQueryId();
            }
            throw QueryExceptionHandler.createException(includeCustomerDetail, sql, queryId, ex);
        }
    }

    private QueryResultIterator executeAdaptiveQuery(String sql) throws SQLException {
        val queryTimeout = QueryTimeout.of(
                statementProperties.getQueryTimeout(), statementProperties.getQueryTimeoutLocalEnforcementDelay());
        val paramBuilder = getQueryParamBuilder(queryTimeout);
        val queryParam = targetMaxRows > 0
                ? paramBuilder.getAdaptiveRowLimitQueryParams(sql, targetMaxRows, targetMaxBytes)
                : paramBuilder.getAdaptiveQueryParams(sql);
        val stub = connection
                .getStub()
                .withDeadlineAfter(
                        queryTimeout.getLocalDeadline().getRemaining().toMillis(), TimeUnit.MILLISECONDS);
        val iterator = QueryResultIterator.of(stub, queryParam);
        queryHandle = iterator;
        // Ensure query status is initialized
        iterator.hasNext();
        return iterator;
    }

    protected void executeAsyncQueryInternal(String sql) throws SQLException {
        val includeCustomerDetail = connection.getConnectionProperties().isIncludeCustomerDetailInReason();
        try {
            val queryTimeout = QueryTimeout.of(
                    statementProperties.getQueryTimeout(), statementProperties.getQueryTimeoutLocalEnforcementDelay());
            val paramBuilder = getQueryParamBuilder(queryTimeout);
            val request = paramBuilder.getQueryParams(sql, QueryParam.TransferMode.ASYNC);
            val stub = connection
                    .getStub()
                    .withDeadlineAfter(
                            queryTimeout.getLocalDeadline().getRemaining().toMillis(), TimeUnit.MILLISECONDS);

            // We set the deadline based off the query timeout here as the server-side doesn't properly enforce
            // the query timeout during the initial compilation phase. By setting the deadline, we can ensure
            // that the query timeout is enforced also when the server hangs during compilation.
            queryHandle = AsyncQueryAccessHandle.of(stub, request);
            log.info(
                    "executeAsyncQuery completed. queryId={}",
                    queryHandle.getQueryStatus().getQueryId());
        } catch (StatusRuntimeException ex) {
            String queryId = null;
            if (queryHandle != null && queryHandle.getQueryStatus() != null) {
                queryId = queryHandle.getQueryStatus().getQueryId();
            }
            throw QueryExceptionHandler.createException(includeCustomerDetail, sql, queryId, ex);
        }
    }

    public DataCloudStatement executeAsyncQuery(String sql) throws SQLException {
        executeAsyncQueryInternal(sql);
        return this;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void close() throws SQLException {
        log.debug("Entering close");
        if (resultSet != null) {
            resultSet.close();
        }
        log.debug("Exiting close");
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {}

    public void clearResultSetConstraints() throws SQLException {
        targetMaxRows = 0;
        targetMaxBytes = 0;
    }

    /**
     * Sets the constraints that would limit the number of rows and overall bytes in any ResultSet object generated by this Statement.
     * This is used to tell the database the maximum number of rows and bytes to return, you may get less than expected because of this.
     *
     * @param maxRows The maximum number of rows a ResultSet can have, zero means there is no limit.
     * @param maxBytes The maximum byte size a ResultSet can be,
     *                 must fall in the range {@link RowRangeIterator#HYPER_MIN_ROW_LIMIT_BYTE_SIZE}
     *                 and {@link RowRangeIterator#HYPER_MAX_ROW_LIMIT_BYTE_SIZE}
     * @throws SQLException If the target maximum byte size is outside the aforementioned range
     */
    public void setResultSetConstraints(int maxRows, int maxBytes) throws SQLException {
        if (maxRows < 0) {
            throw new SQLException(
                    "setResultSetConstraints maxRows must be set to 0 to be disabled but was " + maxRows);
        }

        if (maxBytes < RowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE
                || maxBytes > RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE) {
            throw new SQLException(String.format(
                    "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                    maxBytes,
                    RowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE,
                    RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE));
        }

        targetMaxRows = maxRows;
        targetMaxBytes = maxBytes;
    }

    /**
     * @see DataCloudStatement#setResultSetConstraints
     * @param maxRows The target maximum number of rows a ResultSet can have, zero means there is no limit.
     */
    public void setResultSetConstraints(int maxRows) throws SQLException {
        setResultSetConstraints(maxRows, RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
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
    public void cancel() throws SQLException {
        if (queryHandle == null) {
            log.warn("There was no in-progress query registered with this statement to cancel");
            return;
        }

        connection.cancelQuery(getQueryId());
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
        assertQueryExecuted();
        val includeCustomerDetail = connection.getConnectionProperties().isIncludeCustomerDetailInReason();
        try {
            return logTimedValue(
                    () -> {
                        if (resultSet == null && queryHandle instanceof QueryResultIterator) {
                            val adaptiveIter = (QueryResultIterator) queryHandle;
                            val arrowStream = SQLExceptionQueryResultIterator.createSqlExceptionArrowStreamReader(
                                    adaptiveIter,
                                    includeCustomerDetail,
                                    adaptiveIter.getQueryStatus().getQueryId(),
                                    null);
                            resultSet = StreamingResultSet.of(
                                    arrowStream, adaptiveIter.getQueryStatus().getQueryId());
                        } else if (resultSet == null) {
                            log.warn(
                                    "Prefer acquiring async result sets from helper methods DataCloudConnection::getChunkBasedResultSet and DataCloudConnection::getRowBasedResultSet. We will wait for the query's results to be produced in their entirety before returning a result set.");
                            val status = connection.waitFor(
                                    queryHandle.getQueryStatus().getQueryId(), QueryStatus::allResultsProduced);
                            resultSet = connection.getChunkBasedResultSet(
                                    queryHandle.getQueryStatus().getQueryId(), 0, status.getChunkCount());
                        }
                        log.info(
                                "resultSet created for queryId={}",
                                queryHandle.getQueryStatus().getQueryId());
                        return resultSet;
                    },
                    "getResultSet",
                    log);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException(
                    includeCustomerDetail, null, queryHandle.getQueryStatus().getQueryId(), ex);
        }
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
        throw new SQLException(CHANGE_FETCH_DIRECTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
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
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
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
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
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
        throw new SQLException("Cannot unwrap to " + iFace.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iFace) {
        return iFace.isInstance(this);
    }
}
