/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.async.AsyncQueryInfoIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.SyncIteratorAdapter;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.QueryInfo;

/**
 * Synchronous iterator over QueryInfo messages of a Query.
 *
 * <p>This extends {@link SyncIteratorAdapter} wrapping {@link AsyncQueryInfoIterator} to provide
 * a blocking iterator interface for backward compatibility.</p>
 *
 * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
 * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
 * {@link QueryInfoIterator#hasNext()} and {@link QueryInfoIterator#next()} calls.</p>
 *
 * @see AsyncQueryInfoIterator
 */
@Slf4j
public class QueryInfoIterator extends SyncIteratorAdapter<QueryInfo> {

    /**
     * Provides an Iterator over QueryInfo messages of a Query. It'll keep iterating until the query is finished.
     * For finished queries it'll do at most a single RPC call and return all infos returned from that call. No network
     * calls will be done as part of this method call, only once the iterator is used.
     *
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link QueryInfoIterator#hasNext()} and {@link QueryInfoIterator#next()} calls.</p>
     *
     * @param queryClient The client for a specific query id
     * @return A new QueryInfoIterator instance
     */
    public static QueryInfoIterator of(@NonNull QueryAccessGrpcClient queryClient) {
        return new QueryInfoIterator(AsyncQueryInfoIterator.of(queryClient));
    }

    private QueryInfoIterator(AsyncQueryInfoIterator asyncIterator) {
        super(asyncIterator);
    }
}
