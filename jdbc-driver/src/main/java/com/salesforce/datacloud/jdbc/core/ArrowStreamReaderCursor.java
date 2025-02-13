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

import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.ArrayImpl;

@AllArgsConstructor
@Slf4j
class ArrowStreamReaderCursor extends AbstractCursor {

    private static final int INIT_ROW_NUMBER = -1;

    private final ArrowStreamReader reader;

    private final AtomicInteger currentRow = new AtomicInteger(INIT_ROW_NUMBER);

    private void wasNullConsumer(boolean wasNull) {
        this.wasNull[0] = wasNull;
    }

    @SneakyThrows
    private VectorSchemaRoot getSchemaRoot() {
        return reader.getVectorSchemaRoot();
    }

    @Override
    @SneakyThrows
    public List<Accessor> createAccessors(
            List<ColumnMetaData> types, Calendar localCalendar, ArrayImpl.Factory factory) {
        return getSchemaRoot().getFieldVectors().stream()
                .map(rethrowFunction(this::createAccessor))
                .collect(Collectors.toList());
    }

    private Accessor createAccessor(FieldVector vector) throws SQLException {
        return QueryJDBCAccessorFactory.createAccessor(vector, currentRow::get, this::wasNullConsumer);
    }

    private boolean loadNextBatch() throws SQLException {
        try {
            if (reader.loadNextBatch()) {
                currentRow.set(0);
                return true;
            }
        } catch (IOException e) {
            throw new DataCloudJDBCException(e);
        }
        return false;
    }

    @SneakyThrows
    @Override
    public boolean next() {
        val current = currentRow.incrementAndGet();
        val total = getSchemaRoot().getRowCount();

        try {
            return current < total || loadNextBatch();
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to load next batch", e);
        }
    }

    @Override
    protected Getter createGetter(int i) {
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    @Override
    public void close() {
        reader.close();
    }
}
