/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import org.apache.calcite.avatica.util.AbstractCursor;

public class MetadataCursor extends AbstractCursor {
    private final int rowCount;
    private int currentRow = -1;
    private List<Object> data;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MetadataCursor(@NonNull List<Object> data) {
        this.data = data;
        this.rowCount = data.size();
    }

    protected class ListGetter extends AbstractGetter {
        protected final int index;

        public ListGetter(int index) {
            this.index = index;
        }

        @Override
        public Object getObject() throws SQLException {
            Object o;
            try {
                o = ((List) data.get(currentRow)).get(index);
            } catch (RuntimeException e) {
                throw new SQLException(e);
            }
            wasNull[0] = o == null;
            return o;
        }
    }

    @Override
    protected Getter createGetter(int i) {
        return new ListGetter(i);
    }

    @Override
    public boolean next() {
        currentRow++;
        return currentRow < rowCount;
    }

    @Override
    public void close() {
        try {
            closed.set(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
