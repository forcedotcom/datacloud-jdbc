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
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.sql.SQLException;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;

public class ListVectorAccessor extends BaseListVectorAccessor {

    private final ListVector vector;

    public ListVectorAccessor(
            ListVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(currentRowSupplier, wasNullConsumer);
        this.vector = vector;
    }

    @Override
    public Object getObject() throws SQLException {
        return getListObject(vector::getObject);
    }

    @Override
    protected long getStartOffset(int index) {
        val offsetBuffer = vector.getOffsetBuffer();
        return offsetBuffer.getInt((long) index * BaseRepeatedValueVector.OFFSET_WIDTH);
    }

    @Override
    protected long getEndOffset(int index) {
        val offsetBuffer = vector.getOffsetBuffer();
        return offsetBuffer.getInt((long) (index + 1) * BaseRepeatedValueVector.OFFSET_WIDTH);
    }

    @Override
    protected FieldVector getDataVector() {
        return vector.getDataVector();
    }

    @Override
    protected boolean isNull(int index) {
        return vector.isNull(index);
    }
}
