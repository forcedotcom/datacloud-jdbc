/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.protocol.RawQueryHandle;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;

/**
 * Higher-level query handle held by {@link DataCloudStatement}. Extends {@link RawQueryHandle}
 * (which exposes the gRPC proto status) with a wrapper-typed view used by the public
 * {@link DataCloudStatement#getQueryStatus()} accessor.
 */
interface QueryHandle extends RawQueryHandle {
    QueryStatus getStatus() throws SQLException;
}
