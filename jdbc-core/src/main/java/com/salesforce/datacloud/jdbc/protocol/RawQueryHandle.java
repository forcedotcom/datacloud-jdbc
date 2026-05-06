/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Lower-level protocol view over a query result. Exposes the gRPC proto {@link QueryStatus},
 * which carries the query id and any state observed by the protocol layer.
 */
public interface RawQueryHandle {
    QueryStatus getQueryStatus();
}
