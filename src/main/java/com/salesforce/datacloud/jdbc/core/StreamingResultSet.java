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

import com.salesforce.datacloud.jdbc.core.listener.QueryStatusListener;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.ArrowUtils;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
public class StreamingResultSet extends AvaticaResultSet implements DataCloudResultSet {
    private static final int ROOT_ALLOCATOR_MB_FROM_V2 = 100 * 1024 * 1024;
    private final QueryStatusListener listener;

    private StreamingResultSet(
            QueryStatusListener listener,
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        this.listener = listener;
    }

    @Deprecated
    @SneakyThrows
    public static StreamingResultSet of(String sql, QueryStatusListener listener) {
        try {
            val channel = ExecuteQueryResponseChannel.of(listener.stream());
            val reader = new ArrowStreamReader(channel, new RootAllocator(ROOT_ALLOCATOR_MB_FROM_V2));
            val schemaRoot = reader.getVectorSchemaRoot();
            val columns = ArrowUtils.toColumnMetaData(schemaRoot.getSchema().getFields());
            val timezone = TimeZone.getDefault();
            val state = new QueryState();
            val signature = new Meta.Signature(
                    columns, sql, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
            val metadata = new AvaticaResultSetMetaData(null, null, signature);
            val result = new StreamingResultSet(listener, null, state, signature, metadata, timezone, null);
            val cursor = new ArrowStreamReaderCursor(reader);
            result.execute2(cursor, columns);

            return result;
        } catch (Exception ex) {
            throw QueryExceptionHandler.createQueryException(sql, ex);
        }
    }

    @SneakyThrows
    public static StreamingResultSet of(
            String queryId, HyperGrpcClientExecutor client, Iterator<QueryResult> iterator) {
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
            val listener = new AlreadyReadyNoopListener(queryId);
            val result = new StreamingResultSet(listener, null, state, signature, metadata, timezone, null);
            val cursor = new ArrowStreamReaderCursor(reader);
            result.execute2(cursor, columns);

            return result;
        } catch (Exception ex) {
            throw QueryExceptionHandler.createException(QUERY_FAILURE + queryId, ex);
        }
    }

    @Override
    public String getQueryId() {
        return listener.getQueryId();
    }

    @Override
    public String getStatus() {
        return listener.getStatus();
    }

    @Override
    public boolean isReady() {
        return listener.isReady();
    }
}
