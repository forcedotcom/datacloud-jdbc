/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncStreamObserverIterator;
import com.salesforce.datacloud.jdbc.protocol.async.core.SyncIteratorAdapter;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryParam;

@Slf4j
public class AsyncQueryAccessHandle implements QueryAccessHandle {
    @Getter
    private final salesforce.cdp.hyperdb.v1.QueryStatus queryStatus;

    private volatile QueryStatus latestWrapperStatus;

    private AsyncQueryAccessHandle(salesforce.cdp.hyperdb.v1.QueryStatus queryStatus, QueryStatus initial) {
        this.queryStatus = queryStatus;
        this.latestWrapperStatus = initial;
    }

    public static AsyncQueryAccessHandle of(HyperServiceGrpc.HyperServiceStub stub, QueryParam param)
            throws SQLException {
        val message = "executeQuery. mode=" + param.getTransferMode();
        // Submit request to start feeding the iterator
        val asyncIterator = new AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse>(message, log);
        stub.executeQuery(param, asyncIterator.getObserver());
        val messages = new SyncIteratorAdapter<>(asyncIterator);

        // The protocol guarantees that the first message is a Query Status message with a Query Id.
        val queryStatus = messages.next().getQueryInfo().getQueryStatus();
        // Consume all the remaining messages to ensure that the initial compilation succeeded.
        messages.forEachRemaining(x -> {});
        val initialWrapper = QueryStatus.of(queryStatus);
        return new AsyncQueryAccessHandle(queryStatus, initialWrapper);
    }

    @Override
    public QueryStatus getLatestWrapperStatus() {
        return latestWrapperStatus;
    }

    @Override
    public void observeQueryStatus(QueryStatus status) {
        if (status != null) {
            this.latestWrapperStatus = status;
        }
    }
}
