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
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;

import java.sql.ResultSet;
import java.util.stream.Stream;

public interface DataCloudResultSet extends ResultSet {
    String getQueryId();

    Stream<DataCloudQueryStatus> getQueryStatus() throws DataCloudJDBCException;

    boolean isReady() throws DataCloudJDBCException;
}
