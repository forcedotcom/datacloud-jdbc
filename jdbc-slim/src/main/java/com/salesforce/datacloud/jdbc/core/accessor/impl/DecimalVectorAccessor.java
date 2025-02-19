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
import lombok.val;
import org.apache.arrow.vector.DecimalVector;

public class DecimalVectorAccessor extends QueryJDBCAccessor {
    private final DecimalVector vector;

    public DecimalVectorAccessor(
            DecimalVector vector, IntSupplier getCurrentRow, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        super(getCurrentRow, wasNullConsumer);
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
        this.wasNullConsumer.setWasNull(this.wasNull);
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
