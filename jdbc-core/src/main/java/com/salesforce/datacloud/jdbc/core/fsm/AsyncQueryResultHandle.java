/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import static com.salesforce.datacloud.jdbc.core.fsm.InitialQueryInfoUtility.getInitialQueryInfo;

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

@AllArgsConstructor
public class AsyncQueryResultHandle implements QueryResultHandle {
    @Getter
    private final String queryId;

    public static AsyncQueryResultHandle of(String sql, HyperGrpcClientExecutor client, QueryTimeout timeout)
            throws SQLException {
        val response = client.executeAsyncQuery(sql, timeout);
        val queryInfo = getInitialQueryInfo(sql, response);
        val queryId = queryInfo.getQueryStatus().getQueryId();
        return new AsyncQueryResultHandle(queryId);
    }
}
