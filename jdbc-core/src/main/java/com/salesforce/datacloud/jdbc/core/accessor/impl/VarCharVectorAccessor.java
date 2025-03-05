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
import java.nio.charset.StandardCharsets;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;

public class VarCharVectorAccessor extends QueryJDBCAccessor {

    @FunctionalInterface
    interface Getter {
        byte[] get(int index);
    }

    private final Getter getter;

    public VarCharVectorAccessor(
            VarCharVector vector,
            IntSupplier currenRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this(vector::get, currenRowSupplier, wasNullConsumer);
    }

    public VarCharVectorAccessor(
            LargeVarCharVector vector,
            IntSupplier currenRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this(vector::get, currenRowSupplier, wasNullConsumer);
    }

    VarCharVectorAccessor(
            Getter getter, IntSupplier currentRowSupplier, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(currentRowSupplier, wasNullConsumer);
        this.getter = getter;
    }

    @Override
    public Class<?> getObjectClass() {
        return String.class;
    }

    @Override
    public byte[] getBytes() {
        final byte[] bytes = this.getter.get(getCurrentRow());
        this.wasNull = bytes == null;
        this.wasNullConsumer.setWasNull(this.wasNull);
        return this.getter.get(getCurrentRow());
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
