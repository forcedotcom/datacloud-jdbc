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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
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
        throw new DataCloudJDBCException(INVALID_VECTOR_ERROR_RESPONSE, "2200G", rootCauseException);
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
