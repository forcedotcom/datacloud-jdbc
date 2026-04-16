/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.nio.charset.StandardCharsets;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;

public class BinaryVectorAccessor extends QueryJDBCAccessor {

    private interface ByteArrayGetter {
        byte[] get(int index);
    }

    private final ByteArrayGetter getter;

    public BinaryVectorAccessor(FixedSizeBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, currentRowSupplier);
    }

    public BinaryVectorAccessor(VarBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, currentRowSupplier);
    }

    public BinaryVectorAccessor(LargeVarBinaryVector vector, IntSupplier currentRowSupplier) {
        this(vector::get, currentRowSupplier);
    }

    private BinaryVectorAccessor(ByteArrayGetter getter, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.getter = getter;
    }

    @Override
    public byte[] getBytes() {
        byte[] bytes = getter.get(getCurrentRow());
        this.wasNull = bytes == null;

        return bytes;
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
