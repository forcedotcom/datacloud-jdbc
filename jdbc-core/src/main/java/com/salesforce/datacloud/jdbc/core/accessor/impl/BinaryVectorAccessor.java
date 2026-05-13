/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.nio.charset.StandardCharsets;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;

public class BinaryVectorAccessor extends QueryJDBCAccessor {

    private interface ByteArrayGetter {
        byte[] get(int index);
    }

    private final ByteArrayGetter getter;
    private final IntPredicate nullChecker;

    public BinaryVectorAccessor(FixedSizeBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, vector::isNull, currentRowSupplier);
    }

    public BinaryVectorAccessor(VarBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, vector::isNull, currentRowSupplier);
    }

    public BinaryVectorAccessor(LargeVarBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, vector::isNull, currentRowSupplier);
    }

    private BinaryVectorAccessor(ByteArrayGetter getter, IntPredicate nullChecker, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.getter = getter;
        this.nullChecker = nullChecker;
    }

    @Override
    public byte[] getBytes() {
        // Arrow's vector.get(int) skips the isSet check when arrow.enable_null_check_for_get=false
        // (e.g. set by Iceberg on the JVM), so a null entry returns garbage rather than null.
        // Check the validity buffer explicitly via isNull(int).
        final int row = getCurrentRow();
        this.wasNull = nullChecker.test(row);
        if (this.wasNull) {
            return null;
        }
        return getter.get(row);
    }

    @Override
    public Object getObject() {
        return this.getBytes();
    }

    @Override
    public Class<?> getObjectClass() {
        return byte[].class;
    }

    @Override
    public String getString() {
        byte[] bytes = this.getBytes();
        if (bytes == null) {
            return null;
        }

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
