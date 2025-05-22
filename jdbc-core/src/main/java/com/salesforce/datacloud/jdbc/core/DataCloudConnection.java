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

import com.salesforce.datacloud.jdbc.core.partial.ChunkBased;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class DataCloudConnection implements Connection, AutoCloseable {
    public static final int DEFAULT_PORT = 443;

    @Getter(AccessLevel.PACKAGE)
    private final DataCloudJdbcManagedChannel channel;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ThrowingJdbcSupplier<String> lakehouseSupplier;

    private final ThrowingJdbcSupplier<List<String>> dataspacesSupplier;

    private final DataCloudConnectionString connectionString;

    @Builder.Default
    private final boolean shouldCloseChannelWithConnection = true;

    @Builder.Default
    @Getter
    @Setter
    private Properties clientInfo = new Properties();

    HyperGrpcClientExecutor getExecutor() {
        val stub = channel.getStub(clientInfo, Duration.ofSeconds(Constants.DEFAULT_QUERY_TIMEOUT));
        return HyperGrpcClientExecutor.of(stub, clientInfo);
    }

    /**
     * This allows you to create a DataCloudConnection with minimal adjustments to your ManagedChannel.
     * To configure the channel with the bare minimum use {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder)}
     * To configure the channel with property controlled settings like retries and keep alive use {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     *
     * No matter which {@link DataCloudJdbcManagedChannel} builder you choose,
     * all stubs will be wired with interceptors that control at least the following headers: "x-hyperdb-external-client-context", "x-hyperdb-workload", and "dataspace";
     * the values these headers specify can be controlled by properties with the keys: "external-client-context", "workload", and "dataspace" respectively.
     * This method will not provide auth / tracing, users of this API are expected to wire their own interceptors before supplying a {@link ManagedChannelBuilder} to {@link DataCloudJdbcManagedChannel}.
     * @param channel The channel to use for the connection
     * @param properties The properties to use for the connection
     * @param closeChannelWithConnection Whether to close the channel when the connection is closed, if false you are responsible for cleaning up your managed channel.
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(
            @NonNull DataCloudJdbcManagedChannel channel,
            @NonNull Properties properties,
            boolean closeChannelWithConnection)
            throws DataCloudJDBCException {
        return logTimedValue(
                () -> DataCloudConnection.builder()
                        .channel(channel)
                        .shouldCloseChannelWithConnection(closeChannelWithConnection)
                        .clientInfo(properties)
                        .build(),
                "DataCloudConnection::of with provided channel. closeChannelWithConnection="
                        + closeChannelWithConnection,
                log);
    }

    /**
     * This is a convenience overload for {@link DataCloudConnection#of(DataCloudJdbcManagedChannel, Properties, boolean)}
     * We pass true for closeChannelWithConnection to ensure the channel that is built internally is cleaned up.
     *
     * @param builder The builder to be passed to {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     * @param properties The properties to be passed to {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(@NonNull ManagedChannelBuilder<?> builder, @NonNull Properties properties)
            throws DataCloudJDBCException {
        return of(DataCloudJdbcManagedChannel.of(builder, properties), properties, true);
    }

    /**
     * This overload is intended to be used from the {@code DataCloudJDBCDriver} and assumes a Data Cloud token is wired to the suppliers
     * We pass true for closeChannelWithConnection to ensure the channel that is built internally is cleaned up.
     *
     * @param properties The properties to be passed to {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     * @param authInterceptor a {@link ClientInterceptor} wired to provide an auth token for network requests
     * @param lakehouseSupplier a supplier that acquires the lakehouse from a Data Cloud token
     * @param dataspacesSupplier a supplier that acquires available dataspaces using a Data Cloud token
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(
            @NonNull ManagedChannelBuilder<?> builder,
            @NonNull Properties properties,
            @NonNull ClientInterceptor authInterceptor,
            @NonNull ThrowingJdbcSupplier<String> lakehouseSupplier,
            @NonNull ThrowingJdbcSupplier<List<String>> dataspacesSupplier,
            @NonNull DataCloudConnectionString connectionString)
            throws DataCloudJDBCException {
        return logTimedValue(
                () -> DataCloudConnection.builder()
                        .channel(DataCloudJdbcManagedChannel.of(builder.intercept(authInterceptor), properties))
                        .clientInfo(properties)
                        .lakehouseSupplier(lakehouseSupplier)
                        .dataspacesSupplier(dataspacesSupplier)
                        .connectionString(connectionString)
                        .build(),
                "DataCloudConnection::of with oauth enabled suppliers",
                log);
    }

    @Override
    public Statement createStatement() {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) {
        return getQueryPreparedStatement(sql);
    }

    private DataCloudPreparedStatement getQueryPreparedStatement(String sql) {
        return new DataCloudPreparedStatement(this, sql, new DefaultParameterManager());
    }

    /**
     * Retrieves a collection of rows for the specified query once it is ready.
     * Use {@link #getQueryStatus(String)} to check if the query has produced results or finished execution before calling this method.
     * You can get the Query Id from the executeQuery `DataCloudResultSet`.
     * <p>
     * When using {@link RowBased.Mode#FULL_RANGE}, this method does not handle pagination near the end of available rows.
     * The caller is responsible for calculating the correct offset and limit to avoid out-of-range errors.
     *
     * @param queryId The identifier of the query to fetch results for.
     * @param offset  The starting row offset.
     * @param limit   The maximum number of rows to retrieve.
     * @param mode    The fetching mode—either {@link RowBased.Mode#SINGLE_RPC} for a single request or
     *                {@link RowBased.Mode#FULL_RANGE} to iterate through all available rows.
     * @return A {@link DataCloudResultSet} containing the query results.
     */
    public DataCloudResultSet getRowBasedResultSet(String queryId, long offset, long limit, RowBased.Mode mode)
            throws DataCloudJDBCException {
        log.info("Get row-based result set. queryId={}, offset={}, limit={}, mode={}", queryId, offset, limit, mode);
        val executor = getExecutor();
        val iterator = RowBased.of(executor, queryId, offset, limit, mode);
        return StreamingResultSet.of(queryId, executor, iterator);
    }

    public DataCloudResultSet getChunkBasedResultSet(String queryId, long chunkId, long limit)
            throws DataCloudJDBCException {
        log.info("Get chunk-based result set. queryId={}, chunkId={}, limit={}", queryId, chunkId, limit);
        val executor = getExecutor();
        val iterator = ChunkBased.of(executor, queryId, chunkId, limit);
        return StreamingResultSet.of(queryId, executor, iterator);
    }

    public DataCloudResultSet getChunkBasedResultSet(String queryId, long chunkId) throws DataCloudJDBCException {
        return getChunkBasedResultSet(queryId, chunkId, 1);
    }

    /**
     * Checks if a given row range is available for a query.
     * This method will wait until the row range specified by the other params is available in the given timeout.
     *
     * @param queryId The identifier of the query to check
     * @param offset The starting row offset.
     * @param limit The quantity of rows relative to the offset to wait for
     * @param timeout The duration to wait for the engine have results produced.
     * @param allowLessThan Whether to return early when the available rows is less than {@code offset + limit}
     * @return The first status where the rows available meet the constraints defined by the parameters or the last status the server replied with.
     */
    public DataCloudQueryStatus waitForRowsAvailable(
            String queryId, long offset, long limit, Duration timeout, boolean allowLessThan)
            throws DataCloudJDBCException {
        val executor = getExecutor();
        return executor.waitForRowsAvailable(queryId, offset, limit, timeout, allowLessThan);
    }

    /**
     * Checks if a given chunk range is available for a query.
     * This method will wait until the chunk range specified by the other params is available in the given timeout.
     *
     * @param queryId The identifier of the query to check
     * @param offset The starting chunk offset.
     * @param limit The quantity of chunks relative to the offset to wait for
     * @param timeout The duration to wait for the engine have results produced.
     * @param allowLessThan Whether to return early when the available chunks is less than {@code offset + limit}
     * @return The first status where the chunks available meet the constraints defined by the parameters or the last status the server replied with.
     */
    public DataCloudQueryStatus waitForChunksAvailable(
            String queryId, long offset, long limit, Duration timeout, boolean allowLessThan)
            throws DataCloudJDBCException {
        val executor = getExecutor();
        return executor.waitForChunksAvailable(queryId, offset, limit, timeout, allowLessThan);
    }

    /**
     * Checks if all the query's results are ready, the row count and chunk count will be stable.
     * @param queryId The identifier of the query to check
     * @param timeout The duration to wait for the engine have results produced.
     * @return The first status where {@link DataCloudQueryStatus#allResultsProduced()} or the last status the server replied with.
     */
    public DataCloudQueryStatus waitForResultsProduced(String queryId, Duration timeout) throws DataCloudJDBCException {
        return waitForQueryStatus(queryId, timeout, DataCloudQueryStatus::allResultsProduced);
    }

    /**
     * Checks if a given predicate is satisfied by the status of the query.
     * This method will wait until the server responds with a satisfactory status or the timeout is reached.
     * @param queryId The identifier of the query to check
     * @param timeout The duration to wait for the engine have results produced.
     * @return The first satisfactory status or the last {@link DataCloudQueryStatus} the server replied with.
     */
    public DataCloudQueryStatus waitForQueryStatus(
            String queryId, Duration timeout, Predicate<DataCloudQueryStatus> predicate) throws DataCloudJDBCException {
        val executor = getExecutor();
        return executor.waitForQueryStatus(queryId, timeout, predicate);
    }

    /**
     * Sends a command to the server to cancel the query with the specified query id.
     * @param queryId The query id for the query you want to cancel
     */
    public void cancelQuery(String queryId) throws DataCloudJDBCException {
        getExecutor().cancel(queryId);
    }

    /**
     * Use this to determine when a given query is complete by filtering the responses and a subsequent findFirst()
     */
    public Stream<DataCloudQueryStatus> getQueryStatus(String queryId) throws DataCloudJDBCException {
        val executor = getExecutor();
        return executor.getQueryStatus(queryId);
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        return null;
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {}

    @Override
    public boolean getAutoCommit() {
        return false;
    }

    @Override
    public void commit() {}

    @Override
    public void rollback() {}

    @Override
    public void close() {
        if (!shouldCloseChannelWithConnection) {
            log.debug("Called DataCloudConnection::close when shouldCloseChannelWithConnection=false");
            return;
        }

        try {
            if (closed.compareAndSet(false, true)) {
                channel.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() {
        val userName = this.clientInfo.getProperty("userName");
        return new DataCloudDatabaseMetadata(this, connectionString, lakehouseSupplier, dataspacesSupplier, userName);
    }

    @Override
    public void setReadOnly(boolean readOnly) {}

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setCatalog(String catalog) {}

    @Override
    public String getCatalog() {
        return "";
    }

    @Override
    public void setTransactionIsolation(int level) {}

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) {
        return new DataCloudStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        return getQueryPreparedStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {}

    @Override
    public void setHoldability(int holdability) {}

    @Override
    public int getHoldability() {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) {}

    @Override
    public void releaseSavepoint(Savepoint savepoint) {}

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return getQueryPreparedStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        return null;
    }

    @Override
    public Clob createClob() {
        return null;
    }

    @Override
    public Blob createBlob() {
        return null;
    }

    @Override
    public NClob createNClob() {
        return null;
    }

    @Override
    public SQLXML createSQLXML() {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new DataCloudJDBCException(String.format("Invalid timeout value: %d", timeout));
        }
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) {
        clientInfo.setProperty(name, value);
    }

    @Override
    public String getClientInfo(String name) {
        return clientInfo.getProperty(name);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return null;
    }

    @Override
    public void setSchema(String schema) {}

    @Override
    public String getSchema() {
        return "";
    }

    @Override
    public void abort(Executor executor) {}

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {}

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new DataCloudJDBCException(this.getClass().getName() + " not unwrappable from " + iface.getName());
        }
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
