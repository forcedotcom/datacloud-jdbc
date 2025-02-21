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
package com.salesforce.datacloud.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class QueryExceptionHandlerTest {

    @Test
    public void testCreateExceptionWithStatusRuntimeException() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();
        SQLException actualException = QueryExceptionHandler.createException("test message", fakeException);

        assertInstanceOf(SQLException.class, actualException);
        assertEquals("42P01", actualException.getSQLState());
        assertEquals("42P01: Table not found\n" + "DETAIL:\n" + "\nHINT:\n", actualException.getMessage());
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
        SQLException actualException = QueryExceptionHandler.createException("test message");

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
        SQLException sqlException = QueryExceptionHandler.createException("test message", mockSQLState);

        assertInstanceOf(SQLException.class, sqlException);
        assertEquals("42P01", sqlException.getSQLState());
        assertEquals("test message", sqlException.getMessage());
    }
}
