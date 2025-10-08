/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;
import java.util.Iterator;
import salesforce.cdp.hyperdb.v1.QueryResult;

public interface QueryResultIterator extends Iterator<QueryResult>, QueryResultHandle {
    QueryStatus getQueryStatus() throws SQLException;
}
