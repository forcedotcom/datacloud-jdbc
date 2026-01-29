/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.async.AsyncQueryResultIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.SyncIteratorAdapter;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Synchronous iterator over query results.
 *
 * <p>This extends {@link SyncIteratorAdapter} wrapping {@link AsyncQueryResultIterator} to provide
 * a blocking iterator interface for backward compatibility.</p>
 *
 * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
 * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
 * {@link #hasNext()} and {@link #next()} calls.</p>
 *
 * @see AsyncQueryResultIterator
 */
@Slf4j
public class QueryResultIterator extends SyncIteratorAdapter<QueryResult> implements QueryAccessHandle {

    /**
     * Initializes a new query result iterator. Will start query execution.
     *
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link #hasNext()} and {@link #next()} calls.</p>
     *
     * @param stub - the stub used to execute the gRPC calls to fetch the results
     * @param executeQueryParam - the query parameters to execute
     * @return a new QueryResultIterator instance
     */
    public static QueryResultIterator of(HyperServiceGrpc.HyperServiceStub stub, QueryParam executeQueryParam) {
        return new QueryResultIterator(AsyncQueryResultIterator.of(stub, executeQueryParam));
    }

    private final AsyncQueryResultIterator asyncIterator;

    private QueryResultIterator(AsyncQueryResultIterator asyncIterator) {
        super(asyncIterator);
        this.asyncIterator = asyncIterator;
    }

    @Override
    public QueryStatus getQueryStatus() {
        return asyncIterator.getQueryStatus();
    }
}
