/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.ArrayImpl;

@RequiredArgsConstructor
@Slf4j
class ArrowStreamReaderCursor extends AbstractCursor {

    private static final int INIT_ROW_NUMBER = -1;

    private final ArrowStreamReader reader;

    @lombok.Getter
    private int rowsSeen = 0;

    private final AtomicInteger currentIndex = new AtomicInteger(INIT_ROW_NUMBER);

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
        return QueryJDBCAccessorFactory.createAccessor(vector, currentIndex::get, this::wasNullConsumer);
    }

    private boolean loadNextBatch() throws SQLException {
        try {
            if (reader.loadNextBatch()) {
                currentIndex.set(0);
                return true;
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return false;
    }

    @SneakyThrows
    @Override
    public boolean next() {
        val current = currentIndex.incrementAndGet();
        val total = getSchemaRoot().getRowCount();

        try {
            val next = current < total || loadNextBatch();
            if (next) {
                rowsSeen++;
            }
            return next;
        } catch (Exception e) {
            throw new SQLException("Failed to load next batch: " + e.getMessage(), e);
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
