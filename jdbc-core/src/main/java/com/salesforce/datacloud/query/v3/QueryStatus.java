/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.query.v3;

import com.salesforce.datacloud.jdbc.util.Unstable;
import java.util.Optional;
import lombok.Value;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;

/**
 * Represents the status of a query.
 * The {@link CompletionStatus} enum defines the possible states of the query, which are:
 * <ul>
 *   <li><b>RUNNING</b>: The query is still running or its status is unspecified.</li>
 *   <li><b>RESULTS_PRODUCED</b>: The query has completed, and the results are ready for retrieval.</li>
 *   <li><b>FINISHED</b>: The query has finished execution and its results have been persisted, guaranteed to be available until the expiration time.</li>
 * </ul>
 */
@Value
@Unstable
public class QueryStatus {
    public enum CompletionStatus {
        RUNNING,
        RESULTS_PRODUCED,
        FINISHED
    }

    String queryId;

    long chunkCount;

    long rowCount;

    double progress;

    CompletionStatus completionStatus;

    /**
     * Checks if all the query's results are ready, the row count and chunk count are stable.
     *
     * @return {@code true} if the query's results are ready, otherwise {@code false}.
     */
    public boolean allResultsProduced() {
        return completionStatus == CompletionStatus.RESULTS_PRODUCED || isExecutionFinished();
    }

    /**
     * Checks if the query execution is finished.
     *
     * @return {@code true} if the query has completed execution and results have been persisted, otherwise {@code false}.
     */
    public boolean isExecutionFinished() {
        return completionStatus == CompletionStatus.FINISHED;
    }

    public static Optional<QueryStatus> of(QueryInfo queryInfo) {
        return Optional.ofNullable(queryInfo).map(QueryInfo::getQueryStatus).map(QueryStatus::of);
    }

    public static QueryStatus of(salesforce.cdp.hyperdb.v1.QueryStatus s) {
        val completionStatus = of(s.getCompletionStatus());
        return new QueryStatus(s.getQueryId(), s.getChunkCount(), s.getRowCount(), s.getProgress(), completionStatus);
    }

    private static CompletionStatus of(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus completionStatus) {
        switch (completionStatus) {
            case RUNNING_OR_UNSPECIFIED:
                return CompletionStatus.RUNNING;
            case RESULTS_PRODUCED:
                return CompletionStatus.RESULTS_PRODUCED;
            case FINISHED:
                return CompletionStatus.FINISHED;
            default:
                throw new IllegalArgumentException("Unknown completion status. status=" + completionStatus);
        }
    }
}
