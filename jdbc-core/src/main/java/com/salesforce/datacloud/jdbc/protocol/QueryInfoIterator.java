/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValueNonThrowing;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * See {@link QueryInfoIterator#of(QueryAccessGrpcClient)}.
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryInfoIterator implements Iterator<QueryInfo> {

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
        return new QueryInfoIterator(queryClient, null, false);
    }

    private final QueryAccessGrpcClient client;
    private Iterator<QueryInfo> iterator;
    private boolean isFinished;

    /**
     * This extends the normal hasNext() logic with graceful handling of the CANCELLED error code which indicates
     * that the stream has finished and that a new stream should be started.
     * @return whether there is a next element
     */
    private boolean hasNextFromIterator(int retryCount) {
        try {
            return iterator.hasNext();
        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode() == Status.Code.CANCELLED && (retryCount < 2)) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean hasNext() {
        // Failsafe if cancelled happens too many times sequentially without an info message in-between. Every
        // call should have at least one info returned.
        int retryCount = 0;
        while (true) {
            // We have an iterator that still has infos
            if ((iterator != null) && (hasNextFromIterator(retryCount))) {
                retryCount = 0;
                return true;
            } else if (isFinished) {
                // We have observed a query finish and thus have all query info objects. This check is consciously after
                // the
                // hasNext() check on an existing iterator to allow consumption of all QueryInfo events that might still
                // trigger after a query finish.
                return false;
            } else {
                ++retryCount;
                // Get a new set of infos
                val message = String.format("getQueryInfo queryId=%s, streaming=%s", client.getQueryId(), true);
                iterator = logTimedValueNonThrowing(
                        () -> client.getStub()
                                .getQueryInfo(client.getQueryInfoParamBuilder()
                                        .setStreaming(true)
                                        .build()),
                        message,
                        log);
                // Continue with next iteration of the loop
            }
        }
    }

    @Override
    public QueryInfo next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        val result = iterator.next();
        if (result.hasQueryStatus()) {
            isFinished = (result.getQueryStatus().getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED);
        }
        return result;
    }
}
