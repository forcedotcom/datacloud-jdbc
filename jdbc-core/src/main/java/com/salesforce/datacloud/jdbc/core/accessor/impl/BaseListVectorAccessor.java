/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.sql.Array;
import java.sql.SQLException;
import java.util.List;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.FieldVector;

public abstract class BaseListVectorAccessor extends QueryJDBCAccessor {

    protected abstract long getStartOffset(int index);

    protected abstract long getEndOffset(int index);

    protected abstract FieldVector getDataVector();

    protected abstract boolean isNull(int index);

    protected BaseListVectorAccessor(
            IntSupplier currentRowSupplier, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(currentRowSupplier, wasNullConsumer);
    }

    @Override
    public Class<?> getObjectClass() {
        return List.class;
    }

    protected List<?> getListObject(VectorProvider vectorProvider) throws SQLException {
        List<?> object = vectorProvider.getObject(getCurrentRow());
        this.wasNull = object == null;
        this.wasNullConsumer.setWasNull(this.wasNull);
        return object;
    }

    protected interface VectorProvider {
        List<?> getObject(int row) throws SQLException;
    }

    @Override
    public Array getArray() {
        val index = getCurrentRow();
        val dataVector = getDataVector();

        this.wasNull = isNull(index);
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return null;
        }

        val startOffset = getStartOffset(index);
        val endOffset = getEndOffset(index);

        val valuesCount = endOffset - startOffset;
        return new DataCloudArray(dataVector, startOffset, valuesCount);
    }
}
