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
package com.salesforce.datacloud.jdbc.core.fsm;

import static com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler.createQueryException;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;

final class InitialQueryInfoUtility {
    private InitialQueryInfoUtility() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * The first message in an ExecuteQueryResponse stream is always guaranteed to be either a {@link QueryInfo}
     * with the QueryId or cause a {@link StatusRuntimeException} to be thrown with details about why the query failed.
     * Use this utility to make sure we always get the first response the same way and craft an exception if necessary.
     */
    static QueryInfo getInitialQueryInfo(String sql, Iterator<ExecuteQueryResponse> response)
            throws DataCloudJDBCException {
        try {
            return response.next().getQueryInfo();
        } catch (StatusRuntimeException ex) {
            throw createQueryException(sql, ex);
        }
    }
}
