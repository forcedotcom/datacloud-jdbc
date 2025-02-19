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
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.ArrayImpl;

public class QueryJDBCCursor extends AbstractCursor {
    private VectorSchemaRoot root;
    private final int rowCount;
    private int currentRow = -1;

    public QueryJDBCCursor(VectorSchemaRoot schemaRoot) {
        this.root = schemaRoot;
        this.rowCount = root.getRowCount();
    }

    @SneakyThrows
    @Override
    public List<Accessor> createAccessors(
            List<ColumnMetaData> types, Calendar localCalendar, ArrayImpl.Factory factory) {
        return root.getFieldVectors().stream()
                .map(rethrowFunction(this::createAccessor))
                .collect(Collectors.toList());
    }

    private Accessor createAccessor(FieldVector vector) throws SQLException {
        return QueryJDBCAccessorFactory.createAccessor(
                vector, this::getCurrentRow, (boolean wasNull) -> this.wasNull[0] = wasNull);
    }

    @Override
    protected Getter createGetter(int i) {
        throw new UnsupportedOperationException("Not allowed.");
    }

    @Override
    public boolean next() {
        currentRow++;
        return currentRow < rowCount;
    }

    @Override
    public void close() {
        try {
            AutoCloseables.close(root);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getCurrentRow() {
        return currentRow;
    }
}
