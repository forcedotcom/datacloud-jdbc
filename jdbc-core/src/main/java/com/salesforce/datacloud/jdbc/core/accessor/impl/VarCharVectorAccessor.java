/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.nio.charset.StandardCharsets;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

public class VarCharVectorAccessor extends QueryJDBCAccessor {

    @FunctionalInterface
    interface Getter {
        byte[] get(int index);
    }

    private final ValueVector vector;
    private final Getter getter;

    public VarCharVectorAccessor(VarCharVector vector, IntSupplier currenRowSupplier) {
        this(vector, vector::get, currenRowSupplier);
    }

    public VarCharVectorAccessor(LargeVarCharVector vector, IntSupplier currenRowSupplier) {
        this(vector, vector::get, currenRowSupplier);
    }

    VarCharVectorAccessor(ValueVector vector, Getter getter, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.vector = vector;
        this.getter = getter;
    }

    @Override
    public Class<?> getObjectClass() {
        return String.class;
    }

    @Override
    public byte[] getBytes() {
        // Arrow's vector.get(int) skips the isSet check when arrow.enable_null_check_for_get=false
        // (e.g. set by Iceberg on the JVM), so a null entry returns an empty byte[] rather than null.
        // Check the validity buffer explicitly via isNull(int).
        final int row = getCurrentRow();
        this.wasNull = vector.isNull(row);
        if (this.wasNull) {
            return null;
        }
        return getter.get(row);
    }

    @Override
    public String getString() {
        return getObject();
    }

    @Override
    public String getObject() {
        final byte[] bytes = getBytes();
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
