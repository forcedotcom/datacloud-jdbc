/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
@Unstable // TODO: it might be time to remove this
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
     * Should be composed to craft predicates in conjunction with waitFor, e.g.: waiting for a number of rows: <br />
     * {@code s -> s.allResultsProduced() || s.getRowCount() >= offset + limit}<br />
     * or waiting for a number of chunks: <br />
     * {@code s -> s.allResultsProduced() || s.getChunkCount() >= offset + limit}<br />
     * or getting the current status: <br />
     * {@code s -> true}
     *
     * @return {@code true} if the query's results are ready, otherwise {@code false}.
     */
    public boolean allResultsProduced() {
        return isResultProduced() || isExecutionFinished();
    }

    /**
     * Checks if the query's results have been produced.
     *
     * @return {@code true} if the query's results are available for retrieval, otherwise {@code false}.
     */
    public boolean isResultProduced() {
        return completionStatus == CompletionStatus.RESULTS_PRODUCED;
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

    //    /**
    //     * Provides a set of suggested predicates for determining the status of a query.
    //     */
    //    public static class Predicates {
    //        /**
    //         * Simply get the first query status that the server replies with.
    //         */
    //        public static Predicate<QueryStatus> first() {
    //            return status -> true;
    //        }
    //
    //        /**
    //         * Checks if a given row range is available for a query.
    //         * Especially useful in conjunction with {@link
    // com.salesforce.datacloud.jdbc.core.DataCloudConnection#getRowBasedResultSet}
    //         *
    //         * @param offset The starting row offset.
    //         * @param limit The quantity of rows relative to the offset to wait for
    //         */
    //        public static Predicate<QueryStatus> rowsAvailable(long offset, long limit) {
    //            return status -> status.getRowCount() >= offset + limit;
    //        }
    //
    //        /**
    //         * Checks if a given chunk range is available for a query.
    //         * Especially useful in conjunction with {@link
    // com.salesforce.datacloud.jdbc.core.DataCloudConnection#getChunkBasedResultSet}
    //         *
    //         * @param offset The starting chunk offset.
    //         * @param limit The quantity of chunks relative to the offset to wait for
    //         */
    //        public static Predicate<QueryStatus> chunksAvailable(long offset, long limit) {
    //            return status -> status.getChunkCount() >= offset + limit;
    //        }
    //    }
}
