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
import com.salesforce.datacloud.jdbc.interceptor.NetworkTimeoutInterceptor;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
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
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc.HyperServiceBlockingStub;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class DataCloudConnection implements Connection, AutoCloseable {
    public static final int DEFAULT_PORT = 443;

    private final HyperGrpcStubProvider stubProvider;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ThrowingJdbcSupplier<String> lakehouseSupplier;

    private final ThrowingJdbcSupplier<List<String>> dataspacesSupplier;

    private final DataCloudConnectionString connectionString;

    // The timeout used for network operations. This can be used as a last resort safety net to protect against
    // hanging connections. The default is zero which means no timeout.
    @Builder.Default
    private Duration networkTimeout = Duration.ZERO;

    @Getter(AccessLevel.PACKAGE)
    private ConnectionProperties connectionProperties;

    /**
     * This allows you to create a DataCloudConnection with full control over gRPC stub details.
     * To configure the channel with the bare minimum use {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder)}
     * To configure the channel with property controlled settings like retries and keep alive use {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     *
     * This method will also not provide auth or tracing.
     * @param stubProvider The stub provider to use for the connection
     * @param properties The properties to use for the connection
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(@NonNull HyperGrpcStubProvider stubProvider, @NonNull Properties properties)
            throws DataCloudJDBCException {
        return logTimedValue(
                () -> DataCloudConnection.builder()
                        .stubProvider(stubProvider)
                        .connectionProperties(ConnectionProperties.of(properties))
                        .build(),
                "DataCloudConnection::of with provided stub provider",
                log);
    }

    /**
     * This is a convenience overload for {@link DataCloudConnection#of(HyperGrpcStubProvider, Properties)}
     * We pass true for closeChannelWithConnection to ensure the channel that is built internally is cleaned up.
     *
     * @param builder The builder to be passed to {@link DataCloudJdbcManagedChannel#of(ManagedChannelBuilder, Properties)}
     * @param properties The properties for the JDBC connection
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(@NonNull ManagedChannelBuilder<?> builder, @NonNull Properties properties)
            throws DataCloudJDBCException {
        val stubProvider = new JdbcDriverStubProvider(DataCloudJdbcManagedChannel.of(builder, properties), true);
        return of(stubProvider, properties);
    }

    /**
     * This overload is intended to be used from the {@code DataCloudJDBCDriver} and assumes a Data Cloud token is wired to the suppliers
     * We pass true for closeChannelWithConnection to ensure the channel that is built internally is cleaned up.
     *
     * @param properties The properties for this JDBC connection
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
                        .stubProvider(new JdbcDriverStubProvider(
                                DataCloudJdbcManagedChannel.of(builder.intercept(authInterceptor), properties), true))
                        .connectionProperties(ConnectionProperties.of(properties))
                        .lakehouseSupplier(lakehouseSupplier)
                        .dataspacesSupplier(dataspacesSupplier)
                        .connectionString(connectionString)
                        .build(),
                "DataCloudConnection::of with oauth enabled suppliers",
                log);
    }

    /**
     * Initializes a stub with the appropriate interceptors based on the properties and timeout configured in the JDBC Connection.
     * @return the initialized stub
     */
    HyperServiceBlockingStub getStub() {
        HyperServiceBlockingStub stub = stubProvider.getStub();

        // Attach headers derived from properties to the stub
        val metadata = deriveHeadersFromProperties(connectionProperties);
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        // The interceptor will enforce the network timeout per gRPC call
        if (!networkTimeout.isZero()) {
            stub = stub.withInterceptors(new NetworkTimeoutInterceptor(networkTimeout));
        }

        log.info("Built stub with networkTimeout={}, headers={}", networkTimeout, metadata.keys());
        return stub;
    }

    static Metadata deriveHeadersFromProperties(ConnectionProperties connectionProperties) {
        Metadata metadata = new Metadata();
        // We always add a workload name, if the property is not set we use the default value
        metadata.put(
                Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER),
                connectionProperties.getWorkload());
        if (!connectionProperties.getExternalClientContext().isEmpty()) {
            metadata.put(
                    Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER),
                    connectionProperties.getExternalClientContext());
        }
        if (!connectionProperties.getDataspace().isEmpty()) {
            metadata.put(
                    Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER),
                    connectionProperties.getDataspace());
        }
        return metadata;
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
     * Retrieves a collection of rows for the specified query within the specified range.
     * <p>
     * <b>Important:</b> Before calling this method, you must ensure that the requested row range is available on the server.
     * Use {@link #waitFor(String, Duration, Predicate)} with an appropriate predicate to wait until the desired number of rows
     * (based on <code>offset</code> and <code>limit</code>) are available for the given <code>queryId</code>.
     * <p>
     * For example, to wait until at least <code>offset + limit</code> rows are available, use a predicate like:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= offset + limit
     * </pre>
     * <p>
     * If you call this method before the specified range of rows are available (e.g., the query is still running and hasn't produced enough rows),
     * or if the query completes without ever producing enough rows, this method will throw a {@link DataCloudJDBCException}.
     * <p>
     * The <code>queryId</code> can be obtained from {@link DataCloudResultSet#getQueryId()} or {@link DataCloudStatement#getQueryId()}.
     *
     * @see #waitFor(String, Duration, Predicate)
     *
     * @param queryId The identifier of the query to fetch results for.
     * @param offset  The starting row offset.
     * @param limit   The maximum number of rows to retrieve.
     * @return A {@link DataCloudResultSet} containing the query results.
     * @throws DataCloudJDBCException if the specified range of rows is not available on the server
     */
    public DataCloudResultSet getRowBasedResultSet(String queryId, long offset, long limit)
            throws DataCloudJDBCException {
        log.info("Get row-based result set. queryId={}, offset={}, limit={}", queryId, offset, limit);
        val executor = HyperGrpcClientExecutor.forSubmittedQuery(getStub());
        val iterator = RowBased.of(executor, queryId, offset, limit);
        return StreamingResultSet.of(queryId, iterator);
    }

    /**
     * Retrieves a collection of chunks for the specified query within the specified range.
     * <p>
     * <b>Important:</b> Before calling this method, you must ensure that the requested chunk range is available on the server.
     * Use {@link #waitFor(String, Duration, Predicate)} with an appropriate predicate to wait until the desired number of chunks
     * (based on <code>chunkId</code> and <code>limit</code>) are available for the given <code>queryId</code>.
     * <p>
     * For example, to wait until at least <code>chunkId + limit</code> chunks are available, use a predicate like:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= chunkId + limit
     * </pre>
     * <p>
     * If you call this method before the specified range of chunks are available (e.g., the query is still running and hasn't produced enough chunks),
     * or if the query completes without ever producing enough chunks, this method will throw a {@link DataCloudJDBCException}.
     * <p>
     * The <code>queryId</code> can be obtained from {@link DataCloudResultSet#getQueryId()} or {@link DataCloudStatement#getQueryId()}.
     *
     * @see #waitFor(String, Duration, Predicate)
     *
     * @param queryId The identifier of the query to fetch results for.
     * @param chunkId The starting chunk offset.
     * @param limit   The maximum number of chunks to retrieve.
     * @return A {@link DataCloudResultSet} containing the query results.
     * @throws DataCloudJDBCException if the specified range of chunks is not available on the server
     */
    public DataCloudResultSet getChunkBasedResultSet(String queryId, long chunkId, long limit)
            throws DataCloudJDBCException {
        log.info("Get chunk-based result set. queryId={}, chunkId={}, limit={}", queryId, chunkId, limit);
        val executor = HyperGrpcClientExecutor.forSubmittedQuery(getStub());
        val iterator = ChunkBased.of(executor, queryId, chunkId, limit);
        return StreamingResultSet.of(queryId, iterator);
    }

    /**
     * Retrieves a single chunk for the specified query at the specified index.
     *
     * @see #getChunkBasedResultSet(String, long, long)
     */
    public DataCloudResultSet getChunkBasedResultSet(String queryId, long chunkId) throws DataCloudJDBCException {
        return getChunkBasedResultSet(queryId, chunkId, 1);
    }

    /**
     * Waits for the status of the specified query to satisfy the given predicate, polling until the predicate returns true or the timeout is reached.
     * <p>
     * This method is the recommended way to wait for a query to reach a desired state (such as producing enough rows or chunks) before attempting to fetch results.
     * The <code>predicate</code> determines what condition you are waiting for. For example, to wait until at least <code>offset + limit</code> rows are available, use:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= offset + limit
     * </pre>
     * Or, to wait for enough chunks:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= chunkId + limit
     * </pre>
     * If the predicate is never satisfied and the timeout is reached, or if the server reports all results produced but the predicate is still false, a {@link DataCloudJDBCException} is thrown.
     *
     * @param queryId The identifier of the query to check
     * @param timeout The duration to wait for the engine to produce results
     * @param predicate The condition to check against the query status
     * @return The first status that satisfies the predicate, or the last status received before timeout
     * @throws DataCloudJDBCException if the server reports all results produced but the predicate returns false, or if the timeout is exceeded
     */
    public QueryStatus waitFor(String queryId, Duration timeout, Predicate<QueryStatus> predicate)
            throws DataCloudJDBCException {
        val executor = HyperGrpcClientExecutor.forSubmittedQuery(getStub());
        return executor.waitFor(queryId, Deadline.of(timeout), predicate);
    }

    /**
     * Waits indefinitely for the status of the specified query to satisfy the given predicate.
     * <p>
     * This is a convenience overload of {@link #waitFor(String, Duration, Predicate)} that waits without a timeout.
     * See the documentation for that method for guidance on choosing an appropriate predicate.
     *
     * @see #waitFor(String, Duration, Predicate)
     *
     * @param queryId The identifier of the query to check
     * @param predicate The condition to check against the query status
     * @return The first status that satisfies the predicate
     * @throws DataCloudJDBCException if the server reports all results produced but the predicate returns false
     */
    public QueryStatus waitFor(String queryId, Predicate<QueryStatus> predicate) throws DataCloudJDBCException {
        val executor = HyperGrpcClientExecutor.forSubmittedQuery(getStub());
        return executor.waitFor(queryId, Deadline.infinite(), predicate);
    }

    /**
     * Sends a command to the server to cancel the query with the specified query id.
     * @param queryId The query id for the query you want to cancel
     */
    public void cancelQuery(String queryId) throws DataCloudJDBCException {
        HyperGrpcClientExecutor.forSubmittedQuery(getStub()).cancel(queryId);
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
        try {
            if (closed.compareAndSet(false, true)) {
                stubProvider.close();
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
        val userName = connectionProperties.getUserName();
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

    /**
     * The driver doesn't support any client info properties and thus this method does nothing
     */
    @Override
    public void setClientInfo(String name, String value) {
        return;
    }

    /**
     * The driver doesn't support any client info properties and thus this method does nothing
     */
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        return;
    }

    /**
     * The driver doesn't support any client info properties and thus returns null
     */
    @Override
    public String getClientInfo(String name) {
        return null;
    }
    /**
     * The driver doesn't support any client info properties and thus returns an empty properties object
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
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

    /**
     * Set the network timeout for network operations in this connection. This is a safety net to protect against hanging connections.
     * To enforce a query timeout rather use {@link DataCloudStatement#setQueryTimeout(int)}.
     * A too low network timeout might cause the JDBC driver to fail to operate properly.
     * @param executor This will be ignored
     * @param milliseconds The network timeout in milliseconds.
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        networkTimeout = Duration.ofMillis(milliseconds);
    }

    /**
     * Returns the network timeout for this connection.
     * @return The network timeout for this connection in milliseconds.
     */
    @Override
    public int getNetworkTimeout() {
        return (int) networkTimeout.toMillis();
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
