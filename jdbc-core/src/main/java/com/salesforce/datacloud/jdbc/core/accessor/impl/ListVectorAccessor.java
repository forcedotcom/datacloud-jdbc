/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
