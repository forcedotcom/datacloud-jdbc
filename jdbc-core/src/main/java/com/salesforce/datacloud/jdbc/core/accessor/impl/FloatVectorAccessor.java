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
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.holders.NullableFloat4Holder;

public class FloatVectorAccessor extends QueryJDBCAccessor {

    private final Float4Vector vector;
    private final NullableFloat4Holder holder;

    private static final String INVALID_VALUE_ERROR_RESPONSE = "BigDecimal doesn't support Infinite/NaN";

    public FloatVectorAccessor(
            Float4Vector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull) {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new NullableFloat4Holder();
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return Float.class;
    }

    @Override
    public float getFloat() {
        vector.get(getCurrentRow(), holder);

        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return 0;
        }

        return holder.value;
    }

    @Override
    public Object getObject() {
        final float value = this.getFloat();

        return this.wasNull ? null : value;
    }

    @Override
    public String getString() {
        final float value = this.getFloat();
        return this.wasNull ? null : Float.toString(value);
    }

    @Override
    public boolean getBoolean() {
        return this.getFloat() != 0.0f;
    }

    @Override
    public byte getByte() {
        return (byte) this.getFloat();
    }

    @Override
    public short getShort() {
        return (short) this.getFloat();
    }

    @Override
    public int getInt() {
        return (int) this.getFloat();
    }

    @Override
    public long getLong() {
        return (long) this.getFloat();
    }

    @Override
    public double getDouble() {
        return this.getFloat();
    }

    @Override
    public BigDecimal getBigDecimal() throws SQLException {
        final float value = this.getFloat();
        if (Float.isInfinite(value) || Float.isNaN(value)) {
            val rootCauseException = new UnsupportedOperationException(INVALID_VALUE_ERROR_RESPONSE);
            throw new DataCloudJDBCException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value);
    }

    @Override
    public BigDecimal getBigDecimal(int scale) throws SQLException {
        final float value = this.getFloat();
        if (Float.isInfinite(value) || Float.isNaN(value)) {
            val rootCauseException = new UnsupportedOperationException(INVALID_VALUE_ERROR_RESPONSE);
            throw new DataCloudJDBCException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }
}
