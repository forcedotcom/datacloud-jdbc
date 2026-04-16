/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.math.BigDecimal;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.DecimalVector;

public class DecimalVectorAccessor extends QueryJDBCAccessor {
    private final DecimalVector vector;

    public DecimalVectorAccessor(DecimalVector vector, IntSupplier getCurrentRow) {
        super(getCurrentRow);
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return BigDecimal.class;
    }

    @Override
    public BigDecimal getBigDecimal() {
        final BigDecimal value = vector.getObject(getCurrentRow());
        this.wasNull = value == null;
        return value;
    }

    @Override
    public Object getObject() {
        return getBigDecimal();
    }

    @Override
    public String getString() {
        val value = this.getBigDecimal();
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public int getInt() {
        final BigDecimal value = this.getBigDecimal();

        return this.wasNull ? 0 : value.intValue();
    }
}
