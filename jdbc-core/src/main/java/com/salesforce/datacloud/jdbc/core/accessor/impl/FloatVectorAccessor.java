/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
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

    public FloatVectorAccessor(Float4Vector vector, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.holder = new NullableFloat4Holder();
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return Float.class;
    }

    @Override
    public float getFloat() {
        // Source wasNull from vector.isNull(int) rather than holder.isSet. Arrow's
        // Float4Vector.get(int, holder) currently honors validity unconditionally, but other
        // vector types (e.g. TimeStamp*) gate that path on arrow.enable_null_check_for_get;
        // sourcing from isNull keeps null detection independent of any future flag extension.
        final int row = getCurrentRow();
        this.wasNull = vector.isNull(row);
        if (this.wasNull) {
            return 0;
        }
        vector.get(row, holder);
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
            throw new SQLException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value);
    }

    @Override
    public BigDecimal getBigDecimal(int scale) throws SQLException {
        final float value = this.getFloat();
        if (Float.isInfinite(value) || Float.isNaN(value)) {
            val rootCauseException = new UnsupportedOperationException(INVALID_VALUE_ERROR_RESPONSE);
            throw new SQLException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }
}
