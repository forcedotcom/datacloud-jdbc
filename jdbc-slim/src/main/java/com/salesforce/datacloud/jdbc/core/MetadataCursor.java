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
                throw new DataCloudJDBCException(e);
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
