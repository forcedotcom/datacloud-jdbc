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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import io.grpc.StatusRuntimeException;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
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
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@ExtendWith(InProcessGrpcMockExtension.class)
public class DataCloudStatementTest extends HyperGrpcTestBase {
    @Mock
    private DataCloudConnection connection;

    static DataCloudStatement statement;

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        connection = DataCloudConnection.of(stubProvider, new Properties());
        statement = new DataCloudStatement(connection);
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

    @Test
    @SneakyThrows
    public void testExecuteQuery() {
        setupHyperGrpcClientWithMockedResultSet("query id", ImmutableList.of());
        ResultSet response = statement.executeQuery("SELECT * FROM table");
        assertNotNull(response);
        assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
        assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
        assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
        assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
    }

    @Test
    @SneakyThrows
    public void testExecute() {
        try (val connection = getInterceptedClientConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.execute(
                    "SELECT md5(random()::text) AS id, md5(random()::text) AS name, round((random() * 3 + 1)::numeric, 2) AS grade FROM generate_series(1, 3);");
            connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30));
            val response = statement.getResultSet();
            assertNotNull(response);
            assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
            assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
            assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
            assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
        }
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
        assertThat(statement.getQueryTimeout()).isEqualTo(Constants.DEFAULT_QUERY_TIMEOUT);
    }

    @Test
    public void testGetQueryTimeoutDefaultValue() {
        assertThat(statement.getQueryTimeout()).isEqualTo(Constants.DEFAULT_QUERY_TIMEOUT);
    }

    @SneakyThrows
    @Test
    public void testGetQueryTimeoutSetByConfig() {
        Properties properties = new Properties();
        properties.setProperty("queryTimeout", Integer.toString(30));
        connection = Mockito.mock(DataCloudConnection.class);
        Mockito.when(connection.getClientInfo()).thenReturn(properties);
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

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(
            ints = {
                HYPER_MAX_ROW_LIMIT_BYTE_SIZE + 1,
                HYPER_MIN_ROW_LIMIT_BYTE_SIZE - 1,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE
            })
    public void testConstraintsInvalid(int bytes) {
        assertThatThrownBy(() -> statement.setResultSetConstraints(0, bytes))
                .hasMessageContaining(
                        "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                        bytes, HYPER_MIN_ROW_LIMIT_BYTE_SIZE, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(ints = {HYPER_MAX_ROW_LIMIT_BYTE_SIZE, HYPER_MIN_ROW_LIMIT_BYTE_SIZE, 100000})
    public void testConstraintsValid(int bytes) {
        val rows = 123 + bytes;
        statement.setResultSetConstraints(rows, bytes);
        assertThat(statement.getTargetMaxBytes()).isEqualTo(bytes);
        assertThat(statement.getTargetMaxRows()).isEqualTo(rows);
    }

    @SneakyThrows
    @Test
    public void testConstraintsDefaults() {
        val stmt = new DataCloudStatement(connection);
        assertThat(stmt.getTargetMaxBytes()).isEqualTo(HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
        assertThat(stmt.getTargetMaxRows()).isEqualTo(0);
    }

    @SneakyThrows
    @Test
    public void testConstraintsConfiguration() {
        val bytes = ThreadLocalRandom.current().nextInt(HYPER_MIN_ROW_LIMIT_BYTE_SIZE, HYPER_MAX_ROW_LIMIT_BYTE_SIZE);

        val properties = new Properties();
        properties.setProperty(Constants.BYTE_LIMIT, Integer.toString(bytes));
        val connection = DataCloudConnection.of(new JdbcDriverStubProvider(channel, false), properties);

        val stmt = new DataCloudStatement(connection);

        assertThat(stmt.getTargetMaxBytes()).isEqualTo(bytes);

        stmt.clearResultSetConstraints();

        assertThat(stmt.getTargetMaxBytes()).isEqualTo(HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
        assertThat(stmt.getTargetMaxRows()).isEqualTo(0);
    }
}
