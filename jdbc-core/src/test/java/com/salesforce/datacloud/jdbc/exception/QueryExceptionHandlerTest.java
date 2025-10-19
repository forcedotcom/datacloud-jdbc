/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.exception;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.*;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Properties;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class QueryExceptionHandlerTest {
    @Test
    public void testServerReportedErrorWithCustomerDetails() throws SQLException {
        // Verify that the normal exception messages contains the query and customer details & hints from the server
        // error info
        try (val connection = getHyperQueryConnection()) {
            try (val stmt = (DataCloudStatement) connection.createStatement()) {
                String query = "WITH \"A\" AS (SELECT 1) SELECT * FROM A";
                DataCloudJDBCException ex = assertThrows(DataCloudJDBCException.class, () -> stmt.executeQuery(query));
                String customerDetail = String.format(
                        "%n%s%n%s",
                        // Indentation to more easily see that the ascii art matches (the mismatch is from the escape
                        // characters for the double quotes)
                        "line 1, column 38: WITH \"A\" AS (SELECT 1) SELECT * FROM A",
                        "                                                        ^");
                String customerHint = "Try quoting the identifier: `\"A\"`";
                String expectedMessage = String.format(
                        "Failed to execute query: table \"a\" does not exist%n"
                                + "SQLSTATE: 42P01%n"
                                + "QUERY-ID: %s%n"
                                + "DETAIL: %s%n"
                                + "HINT: %s",
                        stmt.getQueryId(), customerDetail, customerHint);
                assertEquals(expectedMessage, ex.getMessage());
                assertEquals(ex.getMessage(), ex.getFullCustomerMessage());
                assertEquals("42P01", ex.getSQLState());
                assertEquals("table \"a\" does not exist", ex.getPrimaryMessage());
                assertEquals(customerDetail, ex.getCustomerDetail());
                assertEquals(customerHint, ex.getCustomerHint());
                assertEquals("", ex.getSystemDetail());

                // With GetResultData we can test the system details as calling GetResultData on a failed query will
                // return the original primary message as part of the system detail. We can thus easily compare
                // `ex.getPrimaryMessage()` with `resultEx` getSystemD
                DataCloudJDBCException resultEx = assertThrows(
                        DataCloudJDBCException.class, () -> connection.getChunkBasedResultSet(stmt.getQueryId(), 1));
                assertEquals("Query error: " + ex.getPrimaryMessage(), resultEx.getSystemDetail());
                String expectedMessageResult = String.format(
                        "Failed to execute query: Query execution has failed, call GetQueryInfo to get the error message%n"
                                + "SQLSTATE: 55000%n"
                                + "QUERY-ID: %s",
                        stmt.getQueryId());
                assertEquals(expectedMessageResult, resultEx.getMessage());
                assertEquals(resultEx.getMessage(), resultEx.getFullCustomerMessage());
                String expectedSystemMessageResult = String.format(
                        "%s%n%s", expectedMessageResult, "SYSTEM-DETAIL: Query error: table \"a\" does not exist");
                assertEquals(expectedSystemMessageResult, resultEx.getFullSystemMessage());
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
                String customerDetail = String.format(
                        "%n%s%n%s",
                        // Indentation to more easily see that the ascii art matches (the mismatch is from the escape
                        // characters for the double quotes)
                        "line 1, column 38: WITH \"A\" AS (SELECT 1) SELECT * FROM A",
                        "                                                        ^");
                String customerHint = "Try quoting the identifier: `\"A\"`";
                String expectedMessage = String.format(
                        "Failed to execute query: table \"a\" does not exist%n" + "SQLSTATE: 42P01%n" + "QUERY-ID: %s",
                        stmt.getQueryId());
                String expectedFullCustomerMessage = String.format(
                        "Failed to execute query: table \"a\" does not exist%n"
                                + "SQLSTATE: 42P01%n"
                                + "QUERY-ID: %s%n"
                                + "DETAIL: %s%n"
                                + "HINT: %s",
                        stmt.getQueryId(), customerDetail, customerHint);
                assertEquals(expectedMessage, ex.getMessage());
                assertEquals(expectedFullCustomerMessage, ex.getFullCustomerMessage());
                assertEquals("42P01", ex.getSQLState());
                assertEquals("table \"a\" does not exist", ex.getPrimaryMessage());
                assertEquals(customerDetail, ex.getCustomerDetail());
                assertEquals(customerHint, ex.getCustomerHint());
                assertEquals("", ex.getSystemDetail());
                assertEquals(ex.getMessage(), ex.getFullSystemMessage());

                // With GetResultData we can test the system details as calling GetResultData on a failed query will
                // return the original primary message as part of the system detail. We can thus easily compare
                // `ex.getPrimaryMessage()` with `resultEx` getSystemD
                DataCloudJDBCException resultEx = assertThrows(
                        DataCloudJDBCException.class, () -> connection.getChunkBasedResultSet(stmt.getQueryId(), 1));
                assertEquals("Query error: " + ex.getPrimaryMessage(), resultEx.getSystemDetail());
                String expectedMessageResult = String.format(
                        "Failed to execute query: Query execution has failed, call GetQueryInfo to get the error message%n"
                                + "SQLSTATE: 55000%n"
                                + "QUERY-ID: %s",
                        stmt.getQueryId());
                assertEquals(expectedMessageResult, resultEx.getMessage());
                assertEquals(resultEx.getMessage(), resultEx.getFullCustomerMessage());
                String expectedSystemMessageResult = String.format(
                        "%s%n%s", expectedMessageResult, "SYSTEM-DETAIL: Query error: table \"a\" does not exist");
                assertEquals(expectedSystemMessageResult, resultEx.getFullSystemMessage());
            }
        }
    }

    @Test
    public void testCreateExceptionWithStatusRuntimeExceptionAndCustomerDetails() {
        StatusRuntimeException fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();
        String fullMessage = String.format("Failed to execute query: Resource Not Found%n"
                + "SQLSTATE: 42P01%n"
                + "QUERY-ID: 1-2-3-4%n"
                + "QUERY: SELECT 1");
        String redactedMessage = String.format(
                "Failed to execute query: Resource Not Found%n" + "SQLSTATE: 42P01%n" + "QUERY-ID: 1-2-3-4");

        // Verify with customer details
        DataCloudJDBCException actualException = (DataCloudJDBCException)
                QueryExceptionHandler.createException(true, "SELECT 1", "1-2-3-4", fakeException);
        assertInstanceOf(SQLException.class, actualException);
        assertEquals("42P01", actualException.getSQLState());
        assertEquals(fullMessage, actualException.getMessage());
        assertEquals(fullMessage, actualException.getFullCustomerMessage());
        assertEquals(StatusRuntimeException.class, actualException.getCause().getClass());

        // Verify without customer details
        actualException = (DataCloudJDBCException)
                QueryExceptionHandler.createException(false, "SELECT 1", "1-2-3-4", fakeException);
        assertEquals(redactedMessage, actualException.getMessage());
        assertEquals(fullMessage, actualException.getFullCustomerMessage());
        assertEquals(StatusRuntimeException.class, actualException.getCause().getClass());
    }

    @Test
    void testCreateExceptionWithGenericException() {
        Exception mockException = new Exception("Host not found");
        String fullMessage = String.format("Failed to execute query: Host not found%n"
                + "SQLSTATE: HY000%n"
                + "QUERY-ID: 1-2-3-4%n"
                + "QUERY: SELECT 1");
        String redactedMessage =
                String.format("Failed to execute query: Host not found%n" + "SQLSTATE: HY000%n" + "QUERY-ID: 1-2-3-4");

        // Test with customer details
        DataCloudJDBCException sqlException = (DataCloudJDBCException)
                QueryExceptionHandler.createException(true, "SELECT 1", "1-2-3-4", mockException);
        assertEquals(fullMessage, sqlException.getMessage());
        assertEquals(sqlException.getMessage(), sqlException.getFullCustomerMessage());
        assertEquals("HY000", sqlException.getSQLState());
        assertEquals("Host not found", sqlException.getCause().getMessage());
        assertEquals(mockException, sqlException.getCause());

        // Test without customer details
        sqlException = (DataCloudJDBCException)
                QueryExceptionHandler.createException(false, "SELECT 1", "1-2-3-4", mockException);
        assertEquals(redactedMessage, sqlException.getMessage());
        assertEquals(fullMessage, sqlException.getFullCustomerMessage());
        assertEquals("HY000", sqlException.getSQLState());
        assertEquals("Host not found", sqlException.getCause().getMessage());
        assertEquals(mockException, sqlException.getCause());
    }
}
