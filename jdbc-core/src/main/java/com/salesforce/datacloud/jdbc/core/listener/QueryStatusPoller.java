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
package com.salesforce.datacloud.jdbc.core.listener;

import java.sql.SQLException;
import java.util.Optional;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Marked as deprecated since the streaming form of GetQueryInfo makes this construct mostly unnecessary.
 */
@Deprecated
public interface QueryStatusPoller {
    QueryStatus pollQueryStatus();

    long pollChunkCount() throws SQLException;

    default boolean pollIsReady() {
        return Optional.ofNullable(pollQueryStatus())
                .map(QueryStatus::getCompletionStatus)
                .map(QueryStatusPoller::isReady)
                .orElse(false);
    }

    static boolean isReady(QueryStatus.CompletionStatus completionStatus) {
        return completionStatus == QueryStatus.CompletionStatus.RESULTS_PRODUCED
                || completionStatus == QueryStatus.CompletionStatus.FINISHED;
    }
}
