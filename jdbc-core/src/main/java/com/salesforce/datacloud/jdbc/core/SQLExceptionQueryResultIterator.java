/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.protocol.QueryResultArrowStream;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import salesforce.cdp.hyperdb.v1.QueryResult;

/**
 * An iterator wrapper that converts gRPC {@link StatusRuntimeException} to SQL exceptions.
 * <p>
 * This iterator wraps a {@link QueryResult} iterator and intercepts any {@link StatusRuntimeException}
 * thrown during iteration, converting them to appropriate SQL exceptions using the
 * {@link QueryExceptionHandler}. This ensures consistent exception handling throughout the JDBC driver.
 * </p>
 *
 * @see QueryExceptionHandler
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class SQLExceptionQueryResultIterator implements Iterator<QueryResult> {
    Iterator<QueryResult> grpcIterator;
    boolean includeCustomerDetail;
    String queryId;
    String sql;

    /**
     * Creates an {@link ArrowStreamReader} that wraps the given iterator with SQL exception handling.
     * <p>
     * This factory method creates a {@link SQLExceptionQueryResultIterator} that wraps the provided
     * iterator and converts it to an {@link ArrowStreamReader}. Any gRPC exceptions thrown during
     * iteration will be converted to SQL exceptions with appropriate context information.
     * </p>
     *
     * @param resultIterator the source iterator of {@link QueryResult} objects
     * @param includeCustomerDetail whether to include customer-specific details in exceptions
     * @param queryId the unique identifier of the query being executed
     * @param sql the SQL statement being executed
     * @return an {@link ArrowStreamReader} that converts gRPC exceptions to SQL exceptions
     */
    public static ArrowStreamReader createSqlExceptionArrowStreamReader(
            Iterator<QueryResult> resultIterator, boolean includeCustomerDetail, String queryId, String sql) {
        val throwingSqlExceptionIterator =
                new SQLExceptionQueryResultIterator(resultIterator, includeCustomerDetail, queryId, sql);
        return QueryResultArrowStream.toArrowStreamReader(throwingSqlExceptionIterator);
    }

    /**
     * Checks if there are more elements in the iteration.
     * <p>
     * Delegates to the underlying iterator and converts any {@link StatusRuntimeException}
     * to a SQL exception.
     * </p>
     *
     * @return {@code true} if the iteration has more elements
     * @throws java.sql.SQLException if a gRPC error occurs during the check
     */
    @SneakyThrows
    @Override
    public boolean hasNext() {
        try {
            return grpcIterator.hasNext();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException(includeCustomerDetail, sql, queryId, ex);
        }
    }

    /**
     * Returns the next element in the iteration.
     * <p>
     * Delegates to the underlying iterator and converts any {@link StatusRuntimeException}
     * to a SQL exception.
     * </p>
     *
     * @return the next {@link QueryResult} in the iteration
     * @throws java.sql.SQLException if a gRPC error occurs while retrieving the next element
     * @throws java.util.NoSuchElementException if the iteration has no more elements
     */
    @SneakyThrows
    @Override
    public QueryResult next() {
        try {
            return grpcIterator.next();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException(includeCustomerDetail, queryId, sql, ex);
        }
    }
}
