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

import static com.salesforce.datacloud.jdbc.core.accessor.impl.NumericGetter.createGetter;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.types.Types.MinorType;

public class BaseIntVectorAccessor extends QueryJDBCAccessor {

    private final MinorType type;
    private final boolean isUnsigned;
    private final NumericGetter.Getter getter;
    private final NumericGetter.NumericHolder holder;

    private static final String INVALID_TYPE_ERROR_RESPONSE = "Invalid Minor Type provided";

    public BaseIntVectorAccessor(
            TinyIntVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        this(vector, currentRowSupplier, false, setCursorWasNull);
    }

    public BaseIntVectorAccessor(
            SmallIntVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        this(vector, currentRowSupplier, false, setCursorWasNull);
    }

    public BaseIntVectorAccessor(
            IntVector vector, IntSupplier currentRowSupplier, QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        this(vector, currentRowSupplier, false, setCursorWasNull);
    }

    public BaseIntVectorAccessor(
            BigIntVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        this(vector, currentRowSupplier, false, setCursorWasNull);
    }

    private BaseIntVectorAccessor(
            BaseIntVector vector,
            IntSupplier currentRowSupplier,
            boolean isUnsigned,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
        this.type = vector.getMinorType();
        this.holder = new NumericGetter.NumericHolder();
        this.getter = createGetter(vector);
        this.isUnsigned = isUnsigned;
    }

    public BaseIntVectorAccessor(
            UInt4Vector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        this(vector, currentRowSupplier, false, setCursorWasNull);
    }

    @Override
    public long getLong() {
        getter.get(getCurrentRow(), holder);

        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return 0;
        }

        return holder.value;
    }

    @Override
    public Class<?> getObjectClass() {
        return Long.class;
    }

    @Override
    public String getString() {
        final long number = getLong();

        if (this.wasNull) {
            return null;
        } else {
            return isUnsigned ? Long.toUnsignedString(number) : Long.toString(number);
        }
    }

    @Override
    public byte getByte() {
        return (byte) getLong();
    }

    @Override
    public short getShort() {
        return (short) getLong();
    }

    @Override
    public int getInt() {
        return (int) getLong();
    }

    @Override
    public float getFloat() {
        return (float) getLong();
    }

    @Override
    public double getDouble() {
        return (double) getLong();
    }

    @Override
    public BigDecimal getBigDecimal() {
        final BigDecimal value = BigDecimal.valueOf(getLong());
        return this.wasNull ? null : value;
    }

    @Override
    public BigDecimal getBigDecimal(int scale) {
        final BigDecimal value = BigDecimal.valueOf(this.getDouble()).setScale(scale, RoundingMode.HALF_UP);
        return this.wasNull ? null : value;
    }

    @Override
    public Number getObject() throws SQLException {
        final Number number;
        switch (type) {
            case TINYINT:
                number = getByte();
                break;
            case SMALLINT:
                number = getShort();
                break;
            case INT:
            case UINT4:
                number = getInt();
                break;
            case BIGINT:
                number = getLong();
                break;
            default:
                val rootCauseException = new UnsupportedOperationException(INVALID_TYPE_ERROR_RESPONSE);
                throw new DataCloudJDBCException(INVALID_TYPE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return wasNull ? null : number;
    }
}
