/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.StatusRuntimeException;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@ExtendWith(InProcessGrpcMockExtension.class)
public class DataCloudStatementTest extends InterceptedHyperTestBase {
    @Mock
    private DataCloudConnection connection;

    static DataCloudStatement statement;

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        connection = getInterceptedClientConnection();
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
        val ex = Assertions.assertThrows(SQLException.class, func);
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
            connection.waitFor(statement.getQueryId(), QueryStatus::allResultsProduced);
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
        val e = assertThrows(SQLException.class, () -> statement.executeUpdate(sql));
        assertThat(e)
                .hasMessageContaining("is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    public void testSetQueryTimeoutNegativeValue() {
        statement.setQueryTimeout(-100);
        assertThat(statement.getQueryTimeout()).isEqualTo(0);
    }

    @Test
    public void testGetQueryTimeoutDefaultValue() {
        assertThat(statement.getQueryTimeout()).isEqualTo(0);
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
                RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE + 1,
                RowBased.HYPER_MIN_ROW_LIMIT_BYTE_SIZE - 1,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE
            })
    public void testConstraintsInvalid(int bytes) {
        assertThatThrownBy(() -> statement.setResultSetConstraints(0, bytes))
                .hasMessageContaining(
                        "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                        bytes, RowBased.HYPER_MIN_ROW_LIMIT_BYTE_SIZE, RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
    }

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(ints = {RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE, RowBased.HYPER_MIN_ROW_LIMIT_BYTE_SIZE, 100000})
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
        assertThat(stmt.getTargetMaxBytes()).isEqualTo(0);
        assertThat(stmt.getTargetMaxRows()).isEqualTo(0);
    }
}
