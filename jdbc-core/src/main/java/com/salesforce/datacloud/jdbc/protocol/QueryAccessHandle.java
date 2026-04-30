/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;

/**
 * To access a query result one has to have the query id. By exposing a method to get the QueryStatus, the query id can
 * be accessed. We share the full query status to also allow seamless access to other query state.
 */
public interface QueryAccessHandle {
    salesforce.cdp.hyperdb.v1.QueryStatus getQueryStatus();

    /**
     * Returns the latest observed query status as the public wrapper type.
     * Implementations must never return {@code null} once the handle has been initialized.
     */
    QueryStatus getLatestWrapperStatus() throws SQLException;

    /**
     * Feeds an externally observed wrapper {@link QueryStatus} back into the handle. Handles that
     * already track status via their own stream (e.g. the adaptive iterator) may ignore this call.
     */
    default void observeQueryStatus(QueryStatus status) {
        // default: no-op
    }
}
