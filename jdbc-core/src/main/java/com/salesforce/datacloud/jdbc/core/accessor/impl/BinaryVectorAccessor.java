/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
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

    public BinaryVectorAccessor(
            FixedSizeBinaryVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this(vector::get, currentRowSupplier, wasNullConsumer);
    }

    public BinaryVectorAccessor(
            VarBinaryVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this(vector::get, currentRowSupplier, wasNullConsumer);
    }

    public BinaryVectorAccessor(
            LargeVarBinaryVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this(vector::get, currentRowSupplier, wasNullConsumer);
    }

    private BinaryVectorAccessor(
            ByteArrayGetter getter,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(currentRowSupplier, wasNullConsumer);
        this.getter = getter;
    }

    @Override
    public byte[] getBytes() {
        byte[] bytes = getter.get(getCurrentRow());
        this.wasNull = bytes == null;
        this.wasNullConsumer.setWasNull(this.wasNull);

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
