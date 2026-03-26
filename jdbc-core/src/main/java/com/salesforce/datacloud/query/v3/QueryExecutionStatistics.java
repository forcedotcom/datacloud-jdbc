/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.query.v3;

import com.salesforce.datacloud.jdbc.util.Unstable;
import java.time.Duration;
import java.util.Optional;
import lombok.Value;

/**
 * Query execution statistics from Hyper.
 * Provides metrics about query execution for performance monitoring and analysis.
 *
 * <p>These statistics include:
 * <ul>
 *   <li><b>wallClockTime</b>: Server-side elapsed wall clock time</li>
 *   <li><b>rowsProcessed</b>: Total number of rows processed (includes native, BYOL file federation, and BYOL live queries)</li>
 * </ul>
 */
@Value
@Unstable
public class QueryExecutionStatistics {
    /**
     * Server-side elapsed wall clock time.
     * This represents the total time spent executing the query on the server.
     */
    Duration wallClockTime;

    /**
     * Total number of rows processed during query execution.
     * This includes rows from native sources, BYOL file federation, and BYOL live queries.
     */
    long rowsProcessed;

    /**
     * Converts proto QueryExecutionStatistics to Java QueryExecutionStatistics.
     *
     * @param proto the proto QueryExecutionStatistics message
     * @return Optional containing QueryExecutionStatistics if proto is not null, empty otherwise
     */
    public static Optional<QueryExecutionStatistics> of(salesforce.cdp.hyperdb.v1.QueryExecutionStatistics proto) {
        if (proto == null) {
            return Optional.empty();
        }
        return Optional.of(new QueryExecutionStatistics(
                Duration.ofNanos((long) (proto.getWallClockTime() * 1_000_000_000L)), proto.getRowsProcessed()));
    }
}
