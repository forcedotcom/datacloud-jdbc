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

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
class QueryExceptionHandlerTest {
    @Test
    public void testServerReportedErrorWithCustomerDetails() throws SQLException {
        // Verify that the normal exception messages contains the query and customer details & hints from the server
        // error info
        try (val connection = getHyperQueryConnection()) {
            try (val stmt = (DataCloudStatement) connection.createStatement()) {
                String query = "WITH \"A\" AS (SELECT 1) SELECT * FROM A";
                DataCloudJDBCException ex = assertThrows(DataCloudJDBCException.class, () -> stmt.executeQuery(query));
                String customerDetail = "\n"
                        // Indentation to more easily see that the ascii art matches (the mismatch is from the escape
                        // characters for the double quotes)
                        + "line 1, column 38: WITH \"A\" AS (SELECT 1) SELECT * FROM A\n"
                        + "                                                        ^";
                String customerHint = "Try quoting the identifier: `\"A\"`";
                String expectedMessage = "Failed to execute query: table \"a\" does not exist\n"
                        + "SQLSTATE: 42P01\n"
                        + "QUERY-ID: " + stmt.getQueryId() + "\n"
                        + "DETAIL: " + customerDetail + "\n"
                        + "HINT: " + customerHint;
                assertEquals(expectedMessage, ex.getMessage());
                assertEquals(ex.getMessage(), ex.getFullCustomerMessage());
                assertEquals("42P01", ex.getSQLState());
                assertEquals("table \"a\" does not exist", ex.getPrimaryMessage());
                assertEquals(customerDetail, ex.getCustomerDetail());
                assertEquals(customerHint, ex.getCustomerHint());
            }
        }
    }

    @Test
    public void testServerReportedErrorWithoutCustomerDetails() throws SQLException {
        // Verify that the normal exception messages does not contain the query or customer details & hints from the
        // server error info
        // Also verify that the full customer message does still contain all the information for explicit forwarding.
        Properties properties = new Properties();
        properties.setProperty("errorsIncludeCustomerDetails", "false");
        try (val connection = getHyperQueryConnection(properties)) {
            try (val stmt = (DataCloudStatement) connection.createStatement()) {
                String query = "WITH \"A\" AS (SELECT 1) SELECT * FROM A";
                DataCloudJDBCException ex = assertThrows(DataCloudJDBCException.class, () -> stmt.executeQuery(query));
                String customerDetail = "\n"
                        // Indentation to more easily see that the ascii art matches (the mismatch is from the escape
                        // characters for the double quotes)
                        + "line 1, column 38: WITH \"A\" AS (SELECT 1) SELECT * FROM A\n"
                        + "                                                        ^";
                String customerHint = "Try quoting the identifier: `\"A\"`";
                String expectedMessage = "Failed to execute query: table \"a\" does not exist\n"
                        + "SQLSTATE: 42P01\n"
                        + "QUERY-ID: " + stmt.getQueryId();
                String expectedFullCustomerMessage = "Failed to execute query: table \"a\" does not exist\n"
                        + "SQLSTATE: 42P01\n"
                        + "QUERY-ID: " + stmt.getQueryId() + "\n"
                        + "DETAIL: " + customerDetail + "\n"
                        + "HINT: " + customerHint;
                assertEquals(expectedMessage, ex.getMessage());
                assertEquals(expectedFullCustomerMessage, ex.getFullCustomerMessage());
                assertEquals("42P01", ex.getSQLState());
                assertEquals("table \"a\" does not exist", ex.getPrimaryMessage());
                assertEquals(customerDetail, ex.getCustomerDetail());
                assertEquals(customerHint, ex.getCustomerHint());
            }
        }
    }

    @Test
    public void testCreateExceptionWithStatusRuntimeExceptionAndCustomerDetails() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();
        String fullMessage = "Failed to execute query: Resource Not Found\n"
                + "SQLSTATE: 42P01\n"
                + "QUERY-ID: 1-2-3-4\n"
                + "QUERY: SELECT 1";
        String redactedMessage =
                "Failed to execute query: Resource Not Found\n" + "SQLSTATE: 42P01\n" + "QUERY-ID: 1-2-3-4";

        // Verify with customer details
        DataCloudJDBCException actualException =
                QueryExceptionHandler.createException("SELECT 1", "1-2-3-4", true, fakeException);
        assertInstanceOf(SQLException.class, actualException);
        assertEquals("42P01", actualException.getSQLState());
        assertEquals(fullMessage, actualException.getMessage());
        assertEquals(fullMessage, actualException.getFullCustomerMessage());
        assertEquals(StatusRuntimeException.class, actualException.getCause().getClass());

        // Verify without customer details
        actualException = QueryExceptionHandler.createException("SELECT 1", "1-2-3-4", false, fakeException);
        assertEquals(redactedMessage, actualException.getMessage());
        assertEquals(fullMessage, actualException.getFullCustomerMessage());
        assertEquals(StatusRuntimeException.class, actualException.getCause().getClass());
    }

    @Test
    void testCreateExceptionWithGenericException() {
        Exception mockException = new Exception("Host not found");
        String fullMessage = "Failed to execute query: Host not found\n"
                + "SQLSTATE: HY000\n"
                + "QUERY-ID: 1-2-3-4\n"
                + "QUERY: SELECT 1";
        String redactedMessage =
                "Failed to execute query: Host not found\n" + "SQLSTATE: HY000\n" + "QUERY-ID: 1-2-3-4";

        // Test with customer details
        DataCloudJDBCException sqlException =
                QueryExceptionHandler.createException("SELECT 1", "1-2-3-4", true, mockException);
        assertEquals(fullMessage, sqlException.getMessage());
        assertEquals(sqlException.getMessage(), sqlException.getFullCustomerMessage());
        assertEquals("HY000", sqlException.getSQLState());
        assertEquals("Host not found", sqlException.getCause().getMessage());
        assertEquals(mockException, sqlException.getCause());

        // Test without customer details
        sqlException = QueryExceptionHandler.createException("SELECT 1", "1-2-3-4", false, mockException);
        assertEquals(redactedMessage, sqlException.getMessage());
        assertEquals(fullMessage, sqlException.getFullCustomerMessage());
        assertEquals("HY000", sqlException.getSQLState());
        assertEquals("Host not found", sqlException.getCause().getMessage());
        assertEquals(mockException, sqlException.getCause());
    }
}
