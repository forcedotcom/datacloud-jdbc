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
package com.salesforce.datacloud.jdbc.core.listener;

import com.salesforce.datacloud.jdbc.core.DataCloudQueryStatus;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import salesforce.cdp.hyperdb.v1.QueryResult;

// TODO: decide if we should deprecate, or just update this to return the entire QueryStatus object
@Deprecated
public interface QueryStatusListener {
    String BEFORE_READY = "Results were requested before ready";

    DataCloudQueryStatus getStatus();

    String getQueryId();

    String getQuery();

    DataCloudResultSet generateResultSet();

    Stream<QueryResult> stream() throws SQLException;
}
