/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
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

    protected BaseListVectorAccessor(IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
    }

    @Override
    public Class<?> getObjectClass() {
        return List.class;
    }

    protected List<?> getListObject(VectorProvider vectorProvider) throws SQLException {
        // Source wasNull from isNull(int) rather than the getObject return value.
        // ListVector/LargeListVector.getObject is currently validity-correct, but other
        // vector types (e.g. TimeStamp*) gate similar paths on arrow.enable_null_check_for_get;
        // sourcing from isNull keeps null detection independent of any future flag extension.
        final int row = getCurrentRow();
        this.wasNull = isNull(row);
        if (this.wasNull) {
            return null;
        }
        return vectorProvider.getObject(row);
    }

    protected interface VectorProvider {
        List<?> getObject(int row) throws SQLException;
    }

    @Override
    public Array getArray() {
        val index = getCurrentRow();
        val dataVector = getDataVector();

        this.wasNull = isNull(index);
        if (this.wasNull) {
            return null;
        }

        val startOffset = getStartOffset(index);
        val endOffset = getEndOffset(index);

        val valuesCount = endOffset - startOffset;
        return new DataCloudArray(dataVector, startOffset, valuesCount);
    }
}
