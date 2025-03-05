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
package com.salesforce.datacloud.jdbc.core;

import java.util.Optional;
import lombok.Value;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryStatus;

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
public class DataCloudQueryStatus {
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

    static Optional<DataCloudQueryStatus> of(QueryInfo queryInfo) {
        return Optional.ofNullable(queryInfo).map(QueryInfo::getQueryStatus).map(DataCloudQueryStatus::of);
    }

    private static DataCloudQueryStatus of(QueryStatus s) {
        val completionStatus = of(s.getCompletionStatus());
        return new DataCloudQueryStatus(
                s.getQueryId(), s.getChunkCount(), s.getRowCount(), s.getProgress(), completionStatus);
    }

    private static CompletionStatus of(QueryStatus.CompletionStatus completionStatus) {
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
