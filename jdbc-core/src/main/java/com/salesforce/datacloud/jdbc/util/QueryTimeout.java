/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
