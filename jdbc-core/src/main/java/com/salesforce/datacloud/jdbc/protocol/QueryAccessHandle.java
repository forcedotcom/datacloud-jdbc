/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * To access a query result one has to have the query id. By exposing a method to get the QueryStatus, the query id can
 * be accessed. We share the full query status to also allow seamless access to other query state.
 */
public interface QueryAccessHandle {
    QueryStatus getQueryStatus();
}
