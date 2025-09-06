/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.math.BigDecimal;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.holders.NullableBitHolder;

public class BooleanVectorAccessor extends QueryJDBCAccessor {

    private final BitVector vector;
    private final NullableBitHolder holder;

    public BooleanVectorAccessor(
            BitVector vector, IntSupplier getCurrentRow, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(getCurrentRow, wasNullConsumer);
        this.vector = vector;
        this.holder = new NullableBitHolder();
    }

    @Override
    public Class<?> getObjectClass() {
        return Boolean.class;
    }

    @Override
    public Object getObject() {
        final boolean value = this.getBoolean();
        return this.wasNull ? null : value;
    }

    @Override
    public byte getByte() {
        return (byte) this.getLong();
    }

    @Override
    public short getShort() {
        return (short) this.getLong();
    }

    @Override
    public int getInt() {
        return (int) this.getLong();
    }

    @Override
    public float getFloat() {
        return this.getLong();
    }

    @Override
    public double getDouble() {
        return this.getLong();
    }

    @Override
    public BigDecimal getBigDecimal() {
        final long value = this.getLong();

        return this.wasNull ? null : BigDecimal.valueOf(value);
    }

    @Override
    public String getString() {
        final boolean value = getBoolean();
        return wasNull ? null : Boolean.toString(value);
    }

    @Override
    public long getLong() {
        vector.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return 0;
        }

        return holder.value;
    }

    @Override
    public boolean getBoolean() {
        return this.getLong() != 0;
    }
}
