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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.DateTimeUtils;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import io.grpc.StatusRuntimeException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.ThrowingConsumer;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

public class DataCloudPreparedStatementTest extends HyperGrpcTestBase {

    @Mock
    private DataCloudConnection mockConnection;

    @Mock
    private ParameterManager mockParameterManager;

    private DataCloudPreparedStatement preparedStatement;

    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+2"));

    @BeforeEach
    public void beforeEach() throws DataCloudJDBCException {

        mockConnection = mock(DataCloudConnection.class);
        val properties = new Properties();
        when(mockConnection.getChannel()).thenReturn(channel);
        when(mockConnection.getClientInfo()).thenReturn(properties);

        mockParameterManager = mock(ParameterManager.class);

        preparedStatement = new DataCloudPreparedStatement(mockConnection, "SELECT * FROM table", mockParameterManager);
    }

    @Test
    @SneakyThrows
    public void testExecuteQuery() {
        assertThatThrownBy(() -> preparedStatement.executeQuery("SELECT * FROM table"))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(
                        "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::executeQuery() instead.");
    }

    @Test
    @SneakyThrows
    public void testExecute() {
        assertThatThrownBy(() -> preparedStatement.execute("SELECT * FROM table"))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(
                        "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::execute() instead.");
    }

    @Test
    public void testExecuteQueryWithSqlException() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();

        GrpcMock.stubFor(GrpcMock.unaryMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .willReturn(GrpcMock.exception(fakeException)));

        assertThrows(DataCloudJDBCException.class, () -> preparedStatement.executeQuery());
    }

    @Test
    @SneakyThrows
    void testClearParameters() {
        preparedStatement.setString(1, "TEST");
        preparedStatement.setInt(2, 123);

        preparedStatement.clearParameters();
        verify(mockParameterManager).clearParameters();
    }

    @Test
    void testSetParameterNegativeIndexThrowsSQLException() {
        ParameterManager parameterManager = new DefaultParameterManager();
        preparedStatement = new DataCloudPreparedStatement(mockConnection, parameterManager);

        assertThatThrownBy(() -> preparedStatement.setString(0, "TEST"))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Parameter index must be greater than 0");

        assertThatThrownBy(() -> preparedStatement.setString(-1, "TEST"))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Parameter index must be greater than 0");
    }

    @Test
    @SneakyThrows
    void testAllSetMethods() {
        preparedStatement.setString(1, "TEST");
        verify(mockParameterManager).setParameter(1, Types.VARCHAR, "TEST");

        preparedStatement.setBoolean(2, true);
        verify(mockParameterManager).setParameter(2, Types.BOOLEAN, true);

        preparedStatement.setByte(3, (byte) 1);
        verify(mockParameterManager).setParameter(3, Types.TINYINT, (byte) 1);

        preparedStatement.setShort(4, (short) 2);
        verify(mockParameterManager).setParameter(4, Types.SMALLINT, (short) 2);

        preparedStatement.setInt(5, 3);
        verify(mockParameterManager).setParameter(5, Types.INTEGER, 3);

        preparedStatement.setLong(6, 4L);
        verify(mockParameterManager).setParameter(6, Types.BIGINT, 4L);

        preparedStatement.setFloat(7, 5.0f);
        verify(mockParameterManager).setParameter(7, Types.FLOAT, 5.0f);

        preparedStatement.setDouble(8, 6.0);
        verify(mockParameterManager).setParameter(8, Types.DOUBLE, 6.0);

        preparedStatement.setBigDecimal(9, new java.math.BigDecimal("7.0"));
        verify(mockParameterManager).setParameter(9, Types.DECIMAL, new java.math.BigDecimal("7.0"));

        Date date = new Date(System.currentTimeMillis());
        preparedStatement.setDate(10, date);
        verify(mockParameterManager).setParameter(10, Types.DATE, date);

        Time time = new Time(System.currentTimeMillis());
        preparedStatement.setTime(11, time);
        verify(mockParameterManager).setParameter(11, Types.TIME, time);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        preparedStatement.setTimestamp(12, timestamp);
        verify(mockParameterManager).setParameter(12, Types.TIMESTAMP, timestamp);

        preparedStatement.setNull(13, Types.NULL);
        verify(mockParameterManager).setParameter(13, Types.NULL, null);

        preparedStatement.setObject(14, "TEST");
        verify(mockParameterManager).setParameter(14, Types.VARCHAR, "TEST");

        preparedStatement.setObject(15, null);
        verify(mockParameterManager).setParameter(15, Types.NULL, null);

        preparedStatement.setObject(16, "TEST", Types.VARCHAR);
        verify(mockParameterManager).setParameter(16, Types.VARCHAR, "TEST");

        preparedStatement.setObject(17, null, Types.VARCHAR);
        verify(mockParameterManager).setParameter(17, Types.NULL, null);

        try (MockedStatic<DateTimeUtils> mockedDateTimeUtil = mockStatic(DateTimeUtils.class)) {
            mockedDateTimeUtil
                    .when(() -> DateTimeUtils.getUTCDateFromDateAndCalendar(Date.valueOf("1970-01-01"), calendar))
                    .thenReturn(Date.valueOf("1969-12-31"));

            preparedStatement.setDate(18, Date.valueOf("1970-01-01"), calendar);

            mockedDateTimeUtil.verify(
                    () -> DateTimeUtils.getUTCDateFromDateAndCalendar(Date.valueOf("1970-01-01"), calendar), times(1));
            verify(mockParameterManager).setParameter(18, Types.DATE, Date.valueOf("1969-12-31"));
        }

        try (MockedStatic<DateTimeUtils> mockedDateTimeUtil = mockStatic(DateTimeUtils.class)) {
            mockedDateTimeUtil
                    .when(() -> DateTimeUtils.getUTCTimeFromTimeAndCalendar(Time.valueOf("00:00:00"), calendar))
                    .thenReturn(Time.valueOf("22:00:00"));

            preparedStatement.setTime(19, Time.valueOf("00:00:00"), calendar);

            mockedDateTimeUtil.verify(
                    () -> DateTimeUtils.getUTCTimeFromTimeAndCalendar(Time.valueOf("00:00:00"), calendar), times(1));
            verify(mockParameterManager).setParameter(19, Types.TIME, Time.valueOf("22:00:00"));
        }

        try (MockedStatic<DateTimeUtils> mockedDateTimeUtil = mockStatic(DateTimeUtils.class)) {
            mockedDateTimeUtil
                    .when(() -> DateTimeUtils.getUTCTimestampFromTimestampAndCalendar(
                            Timestamp.valueOf("1970-01-01 00:00:00.000000000"), calendar))
                    .thenReturn(Timestamp.valueOf("1969-12-31 22:00:00.000000000"));

            preparedStatement.setTimestamp(20, Timestamp.valueOf("1970-01-01 00:00:00.000000000"), calendar);

            mockedDateTimeUtil.verify(
                    () -> DateTimeUtils.getUTCTimestampFromTimestampAndCalendar(
                            Timestamp.valueOf("1970-01-01 00:00:00.000000000"), calendar),
                    times(1));
            verify(mockParameterManager)
                    .setParameter(20, Types.TIMESTAMP, Timestamp.valueOf("1969-12-31 22:00:00.000000000"));
        }

        assertThatThrownBy(() -> preparedStatement.setObject(1, new InvalidClass()))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Object type not supported for:");
    }

    private static Arguments impl(String name, ThrowingConsumer<DataCloudPreparedStatement> impl) {
        return arguments(named(name, impl));
    }

    private static Stream<Arguments> unsupported() {
        return Stream.of(
                impl("setAsciiStream", s -> s.setAsciiStream(1, null, 0)),
                impl("setUnicodeStream", s -> s.setUnicodeStream(1, null, 0)),
                impl("setBinaryStream", s -> s.setBinaryStream(1, null, 0)),
                impl("addBatch", DataCloudPreparedStatement::addBatch),
                impl("clearBatch", DataCloudStatement::clearBatch),
                impl("setCharacterStream", s -> s.setCharacterStream(1, null, 0)),
                impl("setRef", s -> s.setRef(1, null)),
                impl("setBlob", s -> s.setBlob(1, (Blob) null)),
                impl("setClob", s -> s.setClob(1, (Clob) null)),
                impl("setArray", s -> s.setArray(1, null)),
                impl("setURL", s -> s.setURL(1, null)),
                impl("setRowId", s -> s.setRowId(1, null)),
                impl("setNString", s -> s.setNString(1, null)),
                impl("setNCharacterStream", s -> s.setNCharacterStream(1, null, 0)),
                impl("setNClob", s -> s.setNClob(1, (NClob) null)),
                impl("setClob", s -> s.setClob(1, null, 0)),
                impl("setBlob", s -> s.setBlob(1, null, 0)),
                impl("setNClob", s -> s.setNClob(1, null, 0)),
                impl("setSQLXML", s -> s.setSQLXML(1, null)),
                impl("setObject", s -> s.setObject(1, null, Types.OTHER, 0)),
                impl("setAsciiStream", s -> s.setAsciiStream(1, null, (long) 0)),
                impl("setUnicodeStream", s -> s.setUnicodeStream(1, null, 0)),
                impl("setBinaryStream", s -> s.setBinaryStream(1, null, (long) 0)),
                impl("setAsciiStream", s -> s.setAsciiStream(1, null, 0)),
                impl("setBinaryStream", s -> s.setBinaryStream(1, null, 0)),
                impl("setCharacterStream", s -> s.setCharacterStream(1, null, (long) 0)),
                impl("setAsciiStream", s -> s.setAsciiStream(1, null)),
                impl("setBinaryStream", s -> s.setBinaryStream(1, null)),
                impl("setCharacterStream", s -> s.setCharacterStream(1, null)),
                impl("setNCharacterStream", s -> s.setNCharacterStream(1, null)),
                impl("setClob", s -> s.setClob(1, (Reader) null)),
                impl("setBlob", s -> s.setBlob(1, (InputStream) null)),
                impl("setNClob", s -> s.setNClob(1, (Reader) null)),
                impl("setBytes", s -> s.setBytes(1, null)),
                impl("setNull", s -> s.setNull(1, Types.ARRAY, "ARRAY")),
                impl("executeUpdate", DataCloudPreparedStatement::executeUpdate),
                impl("executeUpdate", s -> s.executeUpdate("")),
                impl("addBatch", s -> s.addBatch("")),
                impl("executeBatch", DataCloudStatement::executeBatch),
                impl("executeUpdate", s -> s.executeUpdate("", Statement.RETURN_GENERATED_KEYS)),
                impl("executeUpdate", s -> s.executeUpdate("", new int[] {})),
                impl("executeUpdate", s -> s.executeUpdate("", new String[] {})),
                impl("getMetaData", DataCloudPreparedStatement::getMetaData),
                impl("getParameterMetaData", DataCloudPreparedStatement::getParameterMetaData));
    }

    @ParameterizedTest
    @MethodSource("unsupported")
    void testUnsupportedOperations(ThrowingConsumer<DataCloudPreparedStatement> func) {
        val e = Assertions.assertThrows(RuntimeException.class, () -> func.accept(preparedStatement));
        AssertionsForClassTypes.assertThat(e).hasRootCauseInstanceOf(DataCloudJDBCException.class);
        AssertionsForClassTypes.assertThat(e.getCause())
                .hasMessageContaining("is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    @SneakyThrows
    void testUnwrapWithCorrectInterface() {
        DataCloudPreparedStatement result = preparedStatement.unwrap(DataCloudPreparedStatement.class);
        assertThat(result).isExactlyInstanceOf(DataCloudPreparedStatement.class);

        assertThatThrownBy(() -> preparedStatement.unwrap(String.class))
                .isExactlyInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Cannot unwrap to java.lang.String");

        assertThat(preparedStatement.isWrapperFor(DataCloudPreparedStatement.class))
                .isTrue();
        assertThat(preparedStatement.isWrapperFor(String.class)).isFalse();
    }

    @Test
    void testSetQueryTimeout() {
        preparedStatement.setQueryTimeout(30);
        assertEquals(30, preparedStatement.getQueryTimeout());

        preparedStatement.setQueryTimeout(-1);
        assertThat(preparedStatement.getQueryTimeout()).isEqualTo(Constants.DEFAULT_QUERY_TIMEOUT);
    }

    @Test
    void testTypeHandlerMapInitialization() {
        assertEquals(TypeHandlers.STRING_HANDLER, TypeHandlers.typeHandlerMap.get(String.class));
        assertEquals(TypeHandlers.BIGDECIMAL_HANDLER, TypeHandlers.typeHandlerMap.get(BigDecimal.class));
        assertEquals(TypeHandlers.SHORT_HANDLER, TypeHandlers.typeHandlerMap.get(Short.class));
        assertEquals(TypeHandlers.INTEGER_HANDLER, TypeHandlers.typeHandlerMap.get(Integer.class));
        assertEquals(TypeHandlers.LONG_HANDLER, TypeHandlers.typeHandlerMap.get(Long.class));
        assertEquals(TypeHandlers.FLOAT_HANDLER, TypeHandlers.typeHandlerMap.get(Float.class));
        assertEquals(TypeHandlers.DOUBLE_HANDLER, TypeHandlers.typeHandlerMap.get(Double.class));
        assertEquals(TypeHandlers.DATE_HANDLER, TypeHandlers.typeHandlerMap.get(Date.class));
        assertEquals(TypeHandlers.TIME_HANDLER, TypeHandlers.typeHandlerMap.get(Time.class));
        assertEquals(TypeHandlers.TIMESTAMP_HANDLER, TypeHandlers.typeHandlerMap.get(Timestamp.class));
        assertEquals(TypeHandlers.BOOLEAN_HANDLER, TypeHandlers.typeHandlerMap.get(Boolean.class));
    }

    @Test
    @SneakyThrows
    void testAllTypeHandlers() {
        PreparedStatement ps = mock(PreparedStatement.class);

        TypeHandlers.STRING_HANDLER.setParameter(ps, 1, "test");
        verify(ps).setString(1, "test");

        TypeHandlers.BIGDECIMAL_HANDLER.setParameter(ps, 1, new BigDecimal("123.45"));
        verify(ps).setBigDecimal(1, new BigDecimal("123.45"));

        TypeHandlers.SHORT_HANDLER.setParameter(ps, 1, (short) 123);
        verify(ps).setShort(1, (short) 123);

        TypeHandlers.INTEGER_HANDLER.setParameter(ps, 1, 123);
        verify(ps).setInt(1, 123);

        TypeHandlers.LONG_HANDLER.setParameter(ps, 1, 123L);
        verify(ps).setLong(1, 123L);

        TypeHandlers.FLOAT_HANDLER.setParameter(ps, 1, 123.45f);
        verify(ps).setFloat(1, 123.45f);

        TypeHandlers.DOUBLE_HANDLER.setParameter(ps, 1, 123.45);
        verify(ps).setDouble(1, 123.45);

        TypeHandlers.DATE_HANDLER.setParameter(ps, 1, Date.valueOf("2024-08-15"));
        verify(ps).setDate(1, Date.valueOf("2024-08-15"));

        TypeHandlers.TIME_HANDLER.setParameter(ps, 1, Time.valueOf("12:34:56"));
        verify(ps).setTime(1, Time.valueOf("12:34:56"));

        TypeHandlers.TIMESTAMP_HANDLER.setParameter(ps, 1, Timestamp.valueOf("2024-08-15 12:34:56"));
        verify(ps).setTimestamp(1, Timestamp.valueOf("2024-08-15 12:34:56"));

        TypeHandlers.BOOLEAN_HANDLER.setParameter(ps, 1, true);
        verify(ps).setBoolean(1, true);
    }

    static class InvalidClass {}
}
