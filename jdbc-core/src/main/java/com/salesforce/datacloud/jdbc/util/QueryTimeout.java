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
package com.salesforce.datacloud.jdbc.util;

import java.time.Duration;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

/**
 * This class captures all the information needed to enforce a query timeout.
 * <p>
 * The serverQueryTimeout is the timeout that should be enforced by the server.
 * <p>
 * The localDeadline is the deadline that should be enforced by the JDBC driver.
 * <p>
 * The localDeadline is the server timeout plus a "slack" buffer to account for network latency and ensure the server has time to
 * produce and transmit the error message.
 */
@Builder(access = AccessLevel.PRIVATE)
@Getter
public class QueryTimeout {
    // This is the query timeout duration that should be enforced server-side
    private final Duration serverQueryTimeout;
    /*
     * This is the deadline that should be enforced by the JDBC driver. Generally server-enforced timeouts are preferable as the server
     * gets the chance to produce detailed error messages. The driver still enforces a timeout locally to avoid blocking the client.
     * The local deadline is the server timeout plus a "slack" buffer to account for network latency and ensure the server has time
     * to produce and transmit the error message.
     */
    private final Deadline localDeadline;

    /**
     * Creates a QueryTimeout object based off the JDBC query timeout.
     * @param queryTimeout The query timeout duration, zero is interpreted as infinite timeout
     * @return A QueryTimeout object
     */
    public static QueryTimeout of(Duration queryTimeout, Duration localEnforcementDelay) {
        if (queryTimeout.isZero()) {
            // When the query timeout is zero we'll set both server query timeout
            // and local deadline to be effectively infinite.
            return QueryTimeout.builder()
                    .serverQueryTimeout(Duration.ZERO)
                    .localDeadline(Deadline.of(Duration.ZERO))
                    .build();
        } else {
            return QueryTimeout.builder()
                    .serverQueryTimeout(queryTimeout)
                    .localDeadline(Deadline.of(queryTimeout.plus(localEnforcementDelay)))
                    .build();
        }
    }
}
