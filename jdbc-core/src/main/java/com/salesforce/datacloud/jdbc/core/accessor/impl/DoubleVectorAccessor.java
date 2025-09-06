/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.holders.NullableFloat8Holder;

public class DoubleVectorAccessor extends QueryJDBCAccessor {

    private final Float8Vector vector;
    private final NullableFloat8Holder holder;

    private static final String INVALID_VALUE_ERROR_RESPONSE = "BigDecimal doesn't support Infinite/NaN";

    public DoubleVectorAccessor(
            Float8Vector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull) {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new NullableFloat8Holder();
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return Double.class;
    }

    @Override
    public double getDouble() {
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
        final double value = this.getDouble();

        return this.wasNull ? null : value;
    }

    @Override
    public String getString() {
        final double value = this.getDouble();
        return this.wasNull ? null : Double.toString(value);
    }

    @Override
    public boolean getBoolean() {
        return this.getDouble() != 0.0;
    }

    @Override
    public byte getByte() {
        return (byte) this.getDouble();
    }

    @Override
    public short getShort() {
        return (short) this.getDouble();
    }

    @Override
    public int getInt() {
        return (int) this.getDouble();
    }

    @Override
    public long getLong() {
        return (long) this.getDouble();
    }

    @Override
    public float getFloat() {
        return (float) this.getDouble();
    }

    @Override
    public BigDecimal getBigDecimal() throws SQLException {
        final double value = this.getDouble();
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            val rootCauseException = new UnsupportedOperationException(INVALID_VALUE_ERROR_RESPONSE);
            throw new DataCloudJDBCException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value);
    }

    @Override
    public BigDecimal getBigDecimal(int scale) throws SQLException {
        final double value = this.getDouble();
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            val rootCauseException = new UnsupportedOperationException(INVALID_VALUE_ERROR_RESPONSE);
            throw new DataCloudJDBCException(INVALID_VALUE_ERROR_RESPONSE, "2200G", rootCauseException);
        }
        return this.wasNull ? null : BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }
}
