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

import static com.salesforce.datacloud.jdbc.util.Constants.CONNECTION_PROTOCOL;
import static com.salesforce.datacloud.jdbc.util.Constants.LOGIN_URL;
import static com.salesforce.datacloud.jdbc.util.Constants.USER;
import static com.salesforce.datacloud.jdbc.util.Constants.USER_NAME;

import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProcessor;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.ClientBuilder;
import com.salesforce.datacloud.jdbc.interceptor.AuthorizationHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.DataspaceHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.HyperExternalClientContextHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.HyperWorkloadHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.MaxMetadataSizeHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.TracingHeadersInterceptor;
import com.salesforce.datacloud.jdbc.util.Messages;
import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@Getter
@Builder(access = AccessLevel.PACKAGE)
public class DataCloudConnection implements Connection, AutoCloseable {
    private static final int DEFAULT_PORT = 443;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final TokenProcessor tokenProcessor;

    @NonNull @Builder.Default
    private final Properties properties = new Properties();

    @Setter
    @Builder.Default
    private List<ClientInterceptor> interceptors = new ArrayList<>();

    @NonNull private final HyperGrpcClientExecutor executor;

    public static DataCloudConnection fromChannel(@NonNull ManagedChannelBuilder<?> builder, Properties properties)
            throws SQLException {
        val interceptors = getClientInterceptors(null, properties);
        val executor = HyperGrpcClientExecutor.of(builder.intercept(interceptors), properties);

        return DataCloudConnection.builder()
                .executor(executor)
                .properties(properties)
                .build();
    }

    /** This flow is not supported by the JDBC Driver Manager, only use it if you know what you're doing. */
    public static DataCloudConnection fromTokenSupplier(
            AuthorizationHeaderInterceptor authInterceptor, @NonNull String host, int port, Properties properties)
            throws SQLException {
        val channel = ManagedChannelBuilder.forAddress(host, port);
        return fromTokenSupplier(authInterceptor, channel, properties);
    }

    /** This flow is not supported by the JDBC Driver Manager, only use it if you know what you're doing. */
    public static DataCloudConnection fromTokenSupplier(
            AuthorizationHeaderInterceptor authInterceptor, ManagedChannelBuilder<?> builder, Properties properties)
            throws SQLException {
        val interceptors = getClientInterceptors(authInterceptor, properties);
        val executor = HyperGrpcClientExecutor.of(builder.intercept(interceptors), properties);

        return DataCloudConnection.builder()
                .executor(executor)
                .properties(properties)
                .build();
    }

    static List<ClientInterceptor> getClientInterceptors(
            AuthorizationHeaderInterceptor authInterceptor, Properties properties) {
        return Stream.of(
                        authInterceptor,
                        new MaxMetadataSizeHeaderInterceptor(),
                        TracingHeadersInterceptor.of(),
                        HyperExternalClientContextHeaderInterceptor.of(properties),
                        HyperWorkloadHeaderInterceptor.of(properties),
                        DataspaceHeaderInterceptor.of(properties))
                .filter(Objects::nonNull)
                .peek(t -> log.info("Registering interceptor. interceptor={}", t))
                .collect(Collectors.toList());
    }

    public static DataCloudConnection of(String url, Properties properties) throws SQLException {
        val serviceRootUrl = getServiceRootUrl(url);
        properties.put(LOGIN_URL, serviceRootUrl);
        addClientUsernameIfRequired(properties);

        if (!AuthenticationSettings.hasAny(properties)) {
            throw new DataCloudJDBCException("No authentication settings provided");
        }

        val tokenProcessor = DataCloudTokenProcessor.of(properties);

        val host = tokenProcessor.getDataCloudToken().getTenantUrl();

        val builder = ManagedChannelBuilder.forAddress(host, DEFAULT_PORT);
        val authInterceptor = AuthorizationHeaderInterceptor.of(tokenProcessor);

        val interceptors = getClientInterceptors(authInterceptor, properties);
        val executor = HyperGrpcClientExecutor.of(builder.intercept(interceptors), properties);

        return DataCloudConnection.builder()
                .tokenProcessor(tokenProcessor)
                .executor(executor)
                .properties(properties)
                .build();
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
                executor.close();
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
        val client = ClientBuilder.buildOkHttpClient(properties);
        val userName = this.properties.getProperty("userName");
        val loginUrl = this.properties.getProperty("loginURL");
        return new DataCloudDatabaseMetadata(
                getQueryStatement(), Optional.ofNullable(tokenProcessor), client, loginUrl, userName);
    }

    private @NonNull DataCloudStatement getQueryStatement() {
        return new DataCloudStatement(this);
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
        return getQueryStatement();
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
    public void setClientInfo(String name, String value) {}

    @Override
    public void setClientInfo(Properties properties) {}

    @Override
    public String getClientInfo(String name) {
        return "";
    }

    @Override
    public Properties getClientInfo() {
        return properties;
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

    public static boolean acceptsUrl(String url) {
        return url != null && url.startsWith(CONNECTION_PROTOCOL) && urlDoesNotContainScheme(url);
    }

    private static boolean urlDoesNotContainScheme(String url) {
        val suffix = url.substring(CONNECTION_PROTOCOL.length());
        return !suffix.startsWith("http://") && !suffix.startsWith("https://");
    }

    /**
     * Returns the extracted service url from given jdbc endpoint
     *
     * @param url of the form jdbc:salesforce-datacloud://login.salesforce.com
     * @return service url
     * @throws SQLException when given url doesn't belong with required datasource
     */
    static String getServiceRootUrl(String url) throws SQLException {
        if (!acceptsUrl(url)) {
            throw new DataCloudJDBCException(Messages.ILLEGAL_CONNECTION_PROTOCOL);
        }

        val serviceRootUrl = url.substring(CONNECTION_PROTOCOL.length());
        val noTrailingSlash = StringUtils.removeEnd(serviceRootUrl, "/");
        val host = StringUtils.removeStart(noTrailingSlash, "//");

        return StringCompatibility.isBlank(host) ? host : createURI(host).toString();
    }

    private static URI createURI(String host) throws SQLException {
        try {
            return URI.create("https://" + host);
        } catch (IllegalArgumentException e) {
            throw new DataCloudJDBCException(Messages.ILLEGAL_CONNECTION_PROTOCOL, e);
        }
    }

    static void addClientUsernameIfRequired(Properties properties) {
        if (properties.containsKey(USER)) {
            properties.computeIfAbsent(USER_NAME, p -> properties.get(USER));
        }
    }
}
