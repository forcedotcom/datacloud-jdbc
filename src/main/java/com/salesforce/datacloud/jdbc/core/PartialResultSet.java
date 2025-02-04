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

import com.salesforce.datacloud.jdbc.core.fsm.Unstable;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.ArrowUtils;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.TimeZone;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Unstable
class PartialResultSet extends AvaticaResultSet implements DataCloudResultSet {
    private static final int ROOT_ALLOCATOR_MB_FROM_V2 = 100 * 1024 * 1024;

    @Getter
    private final String queryId;

    private final HyperGrpcClientExecutor client;

    private Optional<DataCloudQueryStatus> getQueryStatus() {
        return StreamUtilities.toStream(client.getQueryInfo(queryId))
                .map(DataCloudQueryStatus::of)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private PartialResultSet(
            String queryId,
            HyperGrpcClientExecutor client,
            AvaticaStatement statement,
            QueryState queryState,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, queryState, signature, resultSetMetaData, timeZone, firstFrame);
        this.queryId = queryId;
        this.client = client;
    }

    @SneakyThrows
    public static PartialResultSet of(String queryId, HyperGrpcClientExecutor client, Iterator<QueryResult> iterator) {
        try {
            val channel = ExecuteQueryResponseChannel.of(StreamUtilities.toStream(iterator));
            val reader = new ArrowStreamReader(channel, new RootAllocator(ROOT_ALLOCATOR_MB_FROM_V2));
            val schemaRoot = reader.getVectorSchemaRoot();
            val columns = ArrowUtils.toColumnMetaData(schemaRoot.getSchema().getFields());
            val timezone = TimeZone.getDefault();
            val state = new QueryState();
            val signature = new Meta.Signature(
                    columns, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
            val metadata = new AvaticaResultSetMetaData(null, null, signature);
            val result = new PartialResultSet(queryId, client, null, state, signature, metadata, timezone, null);
            val cursor = new ArrowStreamReaderCursor(reader);
            result.execute2(cursor, columns);

            return result;
        } catch (Exception ex) {
            throw QueryExceptionHandler.createException(QUERY_FAILURE + queryId, ex);
        }
    }

    private static final String QUERY_FAILURE = "Failed to acquire partial result set. queryId=";

    @Override
    public DataCloudQueryStatus getStatus() {
        return client.getQueryStatus(queryId).findFirst().orElse(null);
    }
}
