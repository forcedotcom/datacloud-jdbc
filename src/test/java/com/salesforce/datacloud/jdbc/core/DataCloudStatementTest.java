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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import com.salesforce.datacloud.jdbc.util.RequestRecordingInterceptor;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import com.salesforce.hyperdb.grpc.HyperServiceGrpc;
import com.salesforce.hyperdb.grpc.QueryParam;
import io.grpc.StatusRuntimeException;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;

@ExtendWith(InProcessGrpcMockExtension.class)
public class DataCloudStatementTest extends HyperGrpcTestBase {
    @Mock
    private DataCloudConnection connection;

    static DataCloudStatement statement;

    @BeforeEach
    public void beforeEach() {
        connection = Mockito.mock(DataCloudConnection.class);
        val properties = new Properties();
        Mockito.when(connection.getExecutor()).thenReturn(hyperGrpcClient);
        Mockito.when(connection.getProperties()).thenReturn(properties);
        statement = new DataCloudStatement(connection);
    }

    @Test
    @SneakyThrows
    public void forwardOnly() {
        assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    private static Stream<Executable> unsupportedBatchExecutes() {
        return Stream.of(
                () -> statement.execute("", 1),
                () -> statement.execute("", new int[] {}),
                () -> statement.execute("", new String[] {}));
    }

    @ParameterizedTest
    @MethodSource("unsupportedBatchExecutes")
    @SneakyThrows
    public void batchExecutesAreNotSupported(Executable func) {
        val ex = Assertions.assertThrows(DataCloudJDBCException.class, func);
        assertThat(ex)
                .hasMessage("Batch execution is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testForceSyncOverride(boolean forceSync) {
        val p = new Properties();
        p.setProperty(Constants.FORCE_SYNC, Boolean.toString(forceSync));
        when(connection.getProperties()).thenReturn(p);

        val statement = new DataCloudStatement(connection);

        setupHyperGrpcClientWithMockedResultSet(
                "query id",
                ImmutableList.of(),
                forceSync ? QueryParam.TransferMode.SYNC : QueryParam.TransferMode.ADAPTIVE);
        ResultSet response = statement.executeQuery("SELECT * FROM table");
        AssertionsForClassTypes.assertThat(statement.isReady()).isTrue();
        assertNotNull(response);
        AssertionsForClassTypes.assertThat(response.getMetaData().getColumnCount())
                .isEqualTo(3);
    }

    @Test
    @SneakyThrows
    public void testExecuteQuery() {
        setupHyperGrpcClientWithMockedResultSet("query id", ImmutableList.of());
        ResultSet response = statement.executeQuery("SELECT * FROM table");
        assertThat(statement.isReady()).isTrue();
        assertNotNull(response);
        assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
        assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
        assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
        assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
    }

    @Test
    @SneakyThrows
    public void testExecute() {
        setupHyperGrpcClientWithMockedResultSet("query id", ImmutableList.of());
        statement.execute("SELECT * FROM table");
        ResultSet response = statement.getResultSet();
        assertNotNull(response);
        assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
        assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
        assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
        assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
    }

    @Test
    @SneakyThrows
    public void testExecuteQueryIncludesInterceptorsProvidedByCaller() {
        setupHyperGrpcClientWithMockedResultSet("abc", ImmutableList.of());
        val interceptor = new RequestRecordingInterceptor();
        Mockito.when(connection.getInterceptors()).thenReturn(ImmutableList.of(interceptor));

        assertThat(interceptor.getQueries().size()).isEqualTo(0);
        statement.executeQuery("SELECT * FROM table");
        assertThat(interceptor.getQueries().size()).isEqualTo(1);
        statement.executeQuery("SELECT * FROM table");
        assertThat(interceptor.getQueries().size()).isEqualTo(2);
        statement.executeQuery("SELECT * FROM table");
        assertThat(interceptor.getQueries().size()).isEqualTo(3);
        assertDoesNotThrow(() -> statement.close());
    }

    @Test
    public void testExecuteQueryWithSqlException() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();

        GrpcMock.stubFor(GrpcMock.unaryMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .willReturn(GrpcMock.exception(fakeException)));

        assertThrows(DataCloudJDBCException.class, () -> statement.executeQuery("SELECT * FROM table"));
    }

    @Test
    public void testExecuteUpdate() {
        String sql = "UPDATE table SET column = value";
        val e = assertThrows(DataCloudJDBCException.class, () -> statement.executeUpdate(sql));
        assertThat(e)
                .hasMessageContaining("is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    public void testSetQueryTimeoutNegativeValue() {
        statement.setQueryTimeout(-100);
        assertThat(statement.getQueryTimeout()).isEqualTo(DataCloudStatement.DEFAULT_QUERY_TIMEOUT);
    }

    @Test
    public void testGetQueryTimeoutDefaultValue() {
        assertThat(statement.getQueryTimeout()).isEqualTo(DataCloudStatement.DEFAULT_QUERY_TIMEOUT);
    }

    @Test
    public void testGetQueryTimeoutSetByConfig() {
        Properties properties = new Properties();
        properties.setProperty("queryTimeout", Integer.toString(30));
        connection = Mockito.mock(DataCloudConnection.class);
        Mockito.when(connection.getProperties()).thenReturn(properties);
        val statement = new DataCloudStatement(connection);
        assertThat(statement.getQueryTimeout()).isEqualTo(30);
    }

    @Test
    public void testGetQueryTimeoutSetInQueryStatementLevel() {
        statement.setQueryTimeout(10);
        assertThat(statement.getQueryTimeout()).isEqualTo(10);
    }

    @Test
    @SneakyThrows
    public void testCloseIsNullSafe() {
        assertDoesNotThrow(() -> statement.close());
    }
}
