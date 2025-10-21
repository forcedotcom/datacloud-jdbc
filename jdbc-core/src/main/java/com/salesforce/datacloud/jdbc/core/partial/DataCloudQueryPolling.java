/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import static com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler.createException;

import com.salesforce.datacloud.jdbc.protocol.QueryInfoIterator;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Unstable
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataCloudQueryPolling {
    private final QueryAccessGrpcClient client;
    private final boolean includeCustomerDetailInReason;
    private final Predicate<QueryStatus> predicate;
    private QueryStatus lastStatus = null;

    /**
     * Creates a new instance for polling query status.
     * <p>To set a timeout, configure the stub in the client accordingly before creating the poller
     *
     * @param queryClient The client for a specific query id
     * @param includeCustomerDetailInReason Whether to include customer detail in the reason for exceptions
     * @param predicate The condition to check against the query status
     * @return A new DataCloudQueryPolling instance
     */
    public static DataCloudQueryPolling of(
            QueryAccessGrpcClient queryClient,
            boolean includeCustomerDetailInReason,
            Predicate<QueryStatus> predicate) {
        return new DataCloudQueryPolling(queryClient, includeCustomerDetailInReason, predicate);
    }

    /**
     * Waits for the status of the specified query to satisfy the given predicate, polling until the predicate returns true or the timeout is reached.
     * The predicate determines what condition you are waiting for. For example, to wait until at least a certain number of rows are available, use:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= targetRows
     * </pre>
     * Or, to wait for enough chunks:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= targetChunks
     * </pre>
     *
     * @return The first status that satisfies the predicate, or the last status received before timeout
     * @throws SQLException if the server reports all results produced but the predicate returns false, or if the timeout is exceeded
     */
    public QueryStatus waitFor() throws SQLException {
        try {
            val queryInfos = QueryInfoIterator.of(client);
            while (queryInfos.hasNext()) {
                val info = queryInfos.next();
                // Skip non status messages
                if (!info.hasQueryStatus()) {
                    continue;
                }

                lastStatus = QueryStatus.of(info.getQueryStatus());
                if (predicate.test(lastStatus)) {
                    return lastStatus;
                } else if (lastStatus.isExecutionFinished()) {
                    throw new SQLException("Predicate was not satisfied when execution finished. " + this, "HYT00");
                }
            }
            // This should never happen, we expect that the iterator will throw an exception if there is a timeout
            // or that otherwise we either get a query error or the query is finished.
            throw new SQLException("Unexpected end of query info polling before finish. " + this, "XX000");
        } catch (StatusRuntimeException ex) {
            val queryEx = createException(includeCustomerDetailInReason, null, client.getQueryId(), ex);
            if (ex.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new SQLException("Predicate was not satisfied before timeout. " + this, "HYT00", queryEx);
            } else {
                log.info("waitFor failed with {}", this, ex);
                throw queryEx;
            }
        }
    }

    @Override
    public String toString() {
        return String.format("queryId=%s, queryStatus=%s", client.getQueryId(), lastStatus);
    }
}
