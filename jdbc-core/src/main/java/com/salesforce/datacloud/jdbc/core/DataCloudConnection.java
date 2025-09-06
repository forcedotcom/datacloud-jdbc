/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler.createException;
import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValue;
import static com.salesforce.datacloud.jdbc.util.ArrowUtils.toColumnMetaData;
import static org.apache.arrow.vector.types.pojo.Schema.deserializeMessage;

import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.core.partial.ChunkBased;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.interceptor.NetworkTimeoutInterceptor;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.JdbcURL;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.QueryStatus;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Arrays;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc.HyperServiceBlockingStub;
import salesforce.cdp.hyperdb.v1.QueryInfo;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class DataCloudConnection implements Connection {
    private final HyperGrpcStubProvider stubProvider;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Returned by DatabaseMetadata.getURL().
    private final JdbcURL jdbcUrl;

    // Returned by DatabaseMetadata.getUserName().
    private final String userName;

    private final String dataspace;

    @NonNull private final ThrowingJdbcSupplier<String> lakehouseSupplier;

    @NonNull private final ThrowingJdbcSupplier<List<String>> dataspacesSupplier;

    // The timeout used for network operations. This can be used as a last resort safety net to protect against
    // hanging connections. The default is zero which means no timeout.
    @Builder.Default
    private Duration networkTimeout = Duration.ZERO;

    @Getter(AccessLevel.PACKAGE)
    private ConnectionProperties connectionProperties;

    /**
     * Creates a DataCloudConnection with the given stub provider, properties, lakehouse supplier, dataspaces supplier, and connection string.
     *
     * For the external-facing JDBC driver, we use JdbcDriverStubProvider as the `stubProvider`.
     * For internal, more flexible uses, we use a custom HyperGrpcStubProvider as the `stubProvider`.
     * The stub provider must be already wired up to set the authentication and tracing interceptors.
     *
     * @param stubProvider The stub provider to use for the connection
     * @param properties The properties to use for the connection
     * @param lakehouseSupplier a supplier that acquires the lakehouse name
     * @param dataspacesSupplier a supplier that acquires available dataspace names
     * @param jdbcUrl The connection URL to use for the connection
     * @return A DataCloudConnection with the given channel and properties
     */
    public static DataCloudConnection of(
            @NonNull HyperGrpcStubProvider stubProvider,
            @NonNull ConnectionProperties properties,
            JdbcURL jdbcUrl,
            @NonNull String userName,
            @NonNull String dataspace,
            @NonNull ThrowingJdbcSupplier<String> lakehouseSupplier,
            @NonNull ThrowingJdbcSupplier<List<String>> dataspacesSupplier)
            throws DataCloudJDBCException {
        return logTimedValue(
                () -> {
                    return DataCloudConnection.builder()
                            .stubProvider(stubProvider)
                            .connectionProperties(properties)
                            .jdbcUrl(jdbcUrl)
                            .userName(userName)
                            .dataspace(dataspace)
                            .lakehouseSupplier(lakehouseSupplier)
                            .dataspacesSupplier(dataspacesSupplier)
                            .build();
                },
                "DataCloudConnection::of creation",
                log);
    }

    /**
     * Convenience overload without `userName`, `lakehouseSupplier`, `dataspacesSupplier`.
     */
    public static DataCloudConnection of(
            @NonNull HyperGrpcStubProvider stubProvider,
            @NonNull ConnectionProperties properties,
            @NonNull String dataspace,
            JdbcURL jdbcUrl)
            throws DataCloudJDBCException {
        return of(stubProvider, properties, jdbcUrl, "", dataspace, () -> "", () -> Arrays.asList());
    }

    /**
     * Initializes a stub with the appropriate interceptors based on the properties and timeout configured in the JDBC Connection.
     * @return the initialized stub
     */
    HyperServiceBlockingStub getStub() {
        HyperServiceBlockingStub stub = stubProvider.getStub();

        // Attach headers derived from properties to the stub
        val metadata = deriveHeadersFromProperties(dataspace, connectionProperties);
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        // The interceptor will enforce the network timeout per gRPC call
        if (!networkTimeout.isZero()) {
            stub = stub.withInterceptors(new NetworkTimeoutInterceptor(networkTimeout));
        }

        log.info("Built stub with networkTimeout={}, headers={}", networkTimeout, metadata.keys());
        return stub;
    }

    static Metadata deriveHeadersFromProperties(String dataspace, ConnectionProperties connectionProperties) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("User-Agent", Metadata.ASCII_STRING_MARSHALLER), formatDriverInfo());
        // We always add a workload name, if the property is not set we use the default value
        metadata.put(
                Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER),
                connectionProperties.getWorkload());
        if (!connectionProperties.getExternalClientContext().isEmpty()) {
            metadata.put(
                    Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER),
                    connectionProperties.getExternalClientContext());
        }
        if (!dataspace.isEmpty()) {
            metadata.put(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER), dataspace);
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
        return StreamingResultSet.of(iterator, queryId);
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
        val iterator = ChunkBased.of(executor, queryId, chunkId, limit, false);
        return StreamingResultSet.of(iterator, queryId);
    }

    public DataCloudResultSet getChunkBasedResultSet(String queryId, long chunkId) throws DataCloudJDBCException {
        return getChunkBasedResultSet(queryId, chunkId, 1);
    }

    public ResultSetMetaData getSchemaForQueryId(String queryId) throws SQLException {
        HyperGrpcClientExecutor executor = HyperGrpcClientExecutor.forSubmittedQuery(getStub());
        Iterator<QueryInfo> infos = executor.getQuerySchema(queryId);

        try {
            Iterator<ByteString> byteStringIterator = ProtocolMappers.fromQueryInfo(infos);
            if (!byteStringIterator.hasNext()) {
                throw new DataCloudJDBCException("No schema data available for queryId: " + queryId);
            }
            Schema schema = deserializeMessage(byteStringIterator.next().asReadOnlyByteBuffer());
            List<ColumnMetaData> columns = toColumnMetaData(schema.getFields());

            // Create metadata directly without full ResultSet infrastructure
            Meta.Signature signature = new Meta.Signature(
                    columns, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
            return new AvaticaResultSetMetaData(null, null, signature);
        } catch (Exception ex) {
            throw createException("Failed to fetch schema for queryId: " + queryId, ex);
        }
    }

    /**
     * Waits indefinitely (see {@link Deadline#infinite()}) for the status of the specified query to satisfy the given predicate,
     * polling until the predicate returns true or the query completes.
     * The predicate determines what condition you are waiting for. For example, to wait until at least a certain number of rows are available, use:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= targetRows
     * </pre>
     * Or, to wait for enough chunks:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= targetChunks
     * </pre>
     *
     * @param queryId The identifier of the query to check
     * @param predicate The condition to check against the query status
     * @return The first status that satisfies the predicate
     * @throws DataCloudJDBCException if the server reports all results produced but the predicate returns false or if the query fails
     * @see #waitFor(String, Duration, Predicate)
     */
    public QueryStatus waitFor(String queryId, Predicate<QueryStatus> predicate) throws DataCloudJDBCException {
        return HyperGrpcClientExecutor.forSubmittedQuery(getStub()).waitFor(queryId, Deadline.infinite(), predicate);
    }

    /**
     * Waits for the status of the specified query to satisfy the given predicate,
     * polling until the predicate returns true, the query completes, or the timeout is reached.
     * The predicate determines what condition you are waiting for. For example, to wait until at least a certain number of rows are available, use:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= targetRows
     * </pre>
     * Or, to wait for enough chunks:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= targetChunks
     * </pre>
     *
     * @param queryId The identifier of the query to check
     * @param waitTimeout the maximum time to wait for the predicate to be satisfied before timing out
     * @param predicate The condition to check against the query status
     * @return The first status that satisfies the predicate
     * @throws DataCloudJDBCException if the server reports all results produced but the predicate returns false, if the query fails, or if the timeout is exceeded
     * @see #waitFor(String, Duration, Predicate)
     */
    public QueryStatus waitFor(String queryId, Duration waitTimeout, Predicate<QueryStatus> predicate)
            throws DataCloudJDBCException {
        return HyperGrpcClientExecutor.forSubmittedQuery(getStub())
                .waitFor(queryId, Deadline.of(waitTimeout), predicate);
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
        return new DataCloudDatabaseMetadata(this, jdbcUrl, lakehouseSupplier, dataspacesSupplier, userName);
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
