/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import lombok.val;
import org.junit.jupiter.api.Test;

class QueryExceptionHandlerTest {

    @Test
    public void testCreateExceptionWithStatusRuntimeException() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();
        SQLException actualException = QueryExceptionHandler.createException("test message", fakeException);

        assertInstanceOf(SQLException.class, actualException);
        assertEquals("42P01", actualException.getSQLState());
        val sep = System.lineSeparator();
        assertEquals(
                "42P01: Table not found" + sep + "DETAIL:" + sep + sep + "HINT:" + sep, actualException.getMessage());
        assertEquals(StatusRuntimeException.class, actualException.getCause().getClass());
    }

    @Test
    void testCreateExceptionWithGenericException() {
        Exception mockException = new Exception("Generic exception");
        SQLException sqlException = QueryExceptionHandler.createException("Default message", mockException);

        assertEquals("Default message", sqlException.getMessage());
        assertEquals(mockException, sqlException.getCause());
    }

    @Test
    void testCreateException() {
        SQLException actualException = new SQLException("test message");

        assertInstanceOf(SQLException.class, actualException);
        assertEquals("test message", actualException.getMessage());
    }

    @Test
    public void testCreateExceptionWithSQLStateAndThrowableCause() {
        Exception mockException = new Exception("Generic exception");
        String mockSQLState = "42P01";
        SQLException sqlException = QueryExceptionHandler.createException("test message", mockSQLState, mockException);

        assertInstanceOf(SQLException.class, sqlException);
        assertEquals("42P01", sqlException.getSQLState());
        assertEquals("test message", sqlException.getMessage());
    }

    @Test
    public void testCreateExceptionWithSQLStateAndMessage() {
        String mockSQLState = "42P01";
        SQLException sqlException = new SQLException("test message", mockSQLState);

        assertInstanceOf(SQLException.class, sqlException);
        assertEquals("42P01", sqlException.getSQLState());
        assertEquals("test message", sqlException.getMessage());
    }
}
