/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import java.sql.SQLException;
import lombok.val;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;

final class NumericGetter {

    private static final String INVALID_VECTOR_ERROR_RESPONSE = "Invalid Integer Vector provided";

    private NumericGetter() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static class NumericHolder {
        int isSet;
        long value;
    }

    @FunctionalInterface
    static interface Getter {
        void get(int index, NumericHolder holder);
    }

    static Getter createGetter(BaseIntVector vector) throws SQLException {
        if (vector instanceof TinyIntVector) {
            return createGetter((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return createGetter((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return createGetter((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return createGetter((BigIntVector) vector);
        } else if (vector instanceof UInt4Vector) {
            return createGetter((UInt4Vector) vector);
        }
        val rootCauseException = new UnsupportedOperationException(INVALID_VECTOR_ERROR_RESPONSE);
        throw new SQLException(INVALID_VECTOR_ERROR_RESPONSE, "2200G", rootCauseException);
    }

    private static Getter createGetter(TinyIntVector vector) {
        NullableTinyIntHolder nullableTinyIntHolder = new NullableTinyIntHolder();
        return (index, holder) -> {
            vector.get(index, nullableTinyIntHolder);

            holder.isSet = nullableTinyIntHolder.isSet;
            holder.value = nullableTinyIntHolder.value;
        };
    }

    private static Getter createGetter(SmallIntVector vector) {
        NullableSmallIntHolder nullableSmallIntHolder = new NullableSmallIntHolder();
        return (index, holder) -> {
            vector.get(index, nullableSmallIntHolder);

            holder.isSet = nullableSmallIntHolder.isSet;
            holder.value = nullableSmallIntHolder.value;
        };
    }

    private static Getter createGetter(IntVector vector) {
        NullableIntHolder nullableIntHolder = new NullableIntHolder();
        return (index, holder) -> {
            vector.get(index, nullableIntHolder);

            holder.isSet = nullableIntHolder.isSet;
            holder.value = nullableIntHolder.value;
        };
    }

    private static Getter createGetter(BigIntVector vector) {
        NullableBigIntHolder nullableBigIntHolder = new NullableBigIntHolder();
        return (index, holder) -> {
            vector.get(index, nullableBigIntHolder);

            holder.isSet = nullableBigIntHolder.isSet;
            holder.value = nullableBigIntHolder.value;
        };
    }

    private static Getter createGetter(UInt4Vector vector) {
        NullableUInt4Holder nullableUInt4Holder = new NullableUInt4Holder();
        return (index, holder) -> {
            vector.get(index, nullableUInt4Holder);

            holder.isSet = nullableUInt4Holder.isSet;
            holder.value = nullableUInt4Holder.value;
        };
    }
}
