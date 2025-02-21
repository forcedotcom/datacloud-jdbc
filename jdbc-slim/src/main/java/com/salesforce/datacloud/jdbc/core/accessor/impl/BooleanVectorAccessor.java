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
