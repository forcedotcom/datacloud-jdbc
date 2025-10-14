/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Iterator;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;

final class InitialQueryInfoUtility {
    private InitialQueryInfoUtility() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * The first message in an ExecuteQueryResponse stream is always guaranteed to be either a {@link QueryInfo}
     * with the QueryId or cause a {@link StatusRuntimeException} to be thrown with details about why the query failed.
     * Use this utility to make sure we always get the first response the same way and craft an exception if necessary.
     */
    static QueryInfo getInitialQueryInfo(
            boolean includeCustomerDetailInReason, String sql, Iterator<ExecuteQueryResponse> response)
            throws SQLException {
        try {
            return response.next().getQueryInfo();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException(includeCustomerDetailInReason, sql, null, ex);
        }
    }
}
