/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;

public interface QueryResultHandle {
    String getQueryId() throws DataCloudJDBCException;
}
