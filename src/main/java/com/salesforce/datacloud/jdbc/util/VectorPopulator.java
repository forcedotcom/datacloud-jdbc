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
package com.salesforce.datacloud.jdbc.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.experimental.UtilityClass;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

/** Populates vectors in a VectorSchemaRoot with values from a list of parameters. */
@UtilityClass
public final class VectorPopulator {

    /**
     * Populates the vectors in the given VectorSchemaRoot.
     *
     * @param root The VectorSchemaRoot to populate.
     */
    public static void populateVectors(VectorSchemaRoot root, List<ParameterBinding> parameters, Calendar calendar) {
        VectorValueSetterFactory factory = new VectorValueSetterFactory(calendar);

        for (int i = 0; i < parameters.size(); i++) {
            Field field = root.getSchema().getFields().get(i);
            ValueVector vector = root.getVector(field.getName());
            Object value = parameters.get(i) == null ? null : parameters.get(i).getValue();

            @SuppressWarnings(value = "unchecked")
            VectorValueSetter<ValueVector> setter =
                    (VectorValueSetter<ValueVector>) factory.getSetter(vector.getClass());

            if (setter != null) {
                setter.setValue(vector, value);
            } else {
                throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass());
            }
        }
        root.setRowCount(1); // Set row count to 1 since we have exactly one row
    }
}

@FunctionalInterface
interface VectorValueSetter<T extends ValueVector> {
    void setValue(T vector, Object value);
}

/** Factory for creating appropriate setter instances based on vector type. */
class VectorValueSetterFactory {
    private final Map<Class<? extends ValueVector>, VectorValueSetter<?>> setterMap;

    VectorValueSetterFactory(Calendar calendar) {
        setterMap = ImmutableMap.ofEntries(
                Maps.immutableEntry(VarCharVector.class, new VarCharVectorSetter()),
                Maps.immutableEntry(Float4Vector.class, new Float4VectorSetter()),
                Maps.immutableEntry(Float8Vector.class, new Float8VectorSetter()),
                Maps.immutableEntry(IntVector.class, new IntVectorSetter()),
                Maps.immutableEntry(SmallIntVector.class, new SmallIntVectorSetter()),
                Maps.immutableEntry(BigIntVector.class, new BigIntVectorSetter()),
                Maps.immutableEntry(BitVector.class, new BitVectorSetter()),
                Maps.immutableEntry(DecimalVector.class, new DecimalVectorSetter()),
                Maps.immutableEntry(DateDayVector.class, new DateDayVectorSetter()),
                Maps.immutableEntry(TimeMicroVector.class, new TimeMicroVectorSetter(calendar)),
                Maps.immutableEntry(TimeStampMicroTZVector.class, new TimeStampMicroTZVectorSetter(calendar)),
                Maps.immutableEntry(TinyIntVector.class, new TinyIntVectorSetter()));
    }

    @SuppressWarnings("unchecked")
    <T extends ValueVector> VectorValueSetter<T> getSetter(Class<T> vectorClass) {
        return (VectorValueSetter<T>) setterMap.get(vectorClass);
    }
}

/** Base setter implementation for ValueVectors that need type validation. */
abstract class BaseVectorSetter<T extends ValueVector, V> implements VectorValueSetter<T> {
    private final Class<V> valueType;

    BaseVectorSetter(Class<V> valueType) {
        this.valueType = valueType;
    }

    @Override
    public void setValue(T vector, Object value) {
        if (value == null) {
            setNullValue(vector);
        } else if (valueType.isInstance(value)) {
            setValueInternal(vector, valueType.cast(value));
        } else {
            throw new IllegalArgumentException(
                    "Value for " + vector.getClass().getSimpleName() + " must be of type " + valueType.getSimpleName());
        }
    }

    protected abstract void setNullValue(T vector);

    protected abstract void setValueInternal(T vector, V value);
}

/** Setter implementation for VarCharVector. */
class VarCharVectorSetter extends BaseVectorSetter<VarCharVector, String> {
    VarCharVectorSetter() {
        super(String.class);
    }

    @Override
    protected void setValueInternal(VarCharVector vector, String value) {
        vector.setSafe(0, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected void setNullValue(VarCharVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for Float4Vector. */
class Float4VectorSetter extends BaseVectorSetter<Float4Vector, Float> {
    Float4VectorSetter() {
        super(Float.class);
    }

    @Override
    protected void setValueInternal(Float4Vector vector, Float value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(Float4Vector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for Float8Vector. */
class Float8VectorSetter extends BaseVectorSetter<Float8Vector, Double> {
    Float8VectorSetter() {
        super(Double.class);
    }

    @Override
    protected void setValueInternal(Float8Vector vector, Double value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(Float8Vector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for IntVector. */
class IntVectorSetter extends BaseVectorSetter<IntVector, Integer> {
    IntVectorSetter() {
        super(Integer.class);
    }

    @Override
    protected void setValueInternal(IntVector vector, Integer value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(IntVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for SmallIntVector. */
class SmallIntVectorSetter extends BaseVectorSetter<SmallIntVector, Short> {
    SmallIntVectorSetter() {
        super(Short.class);
    }

    @Override
    protected void setValueInternal(SmallIntVector vector, Short value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(SmallIntVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for BigIntVector. */
class BigIntVectorSetter extends BaseVectorSetter<BigIntVector, Long> {
    BigIntVectorSetter() {
        super(Long.class);
    }

    @Override
    protected void setValueInternal(BigIntVector vector, Long value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(BigIntVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for BitVector. */
class BitVectorSetter extends BaseVectorSetter<BitVector, Boolean> {
    BitVectorSetter() {
        super(Boolean.class);
    }

    @Override
    protected void setValueInternal(BitVector vector, Boolean value) {
        vector.setSafe(0, Boolean.TRUE.equals(value) ? 1 : 0);
    }

    @Override
    protected void setNullValue(BitVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for DecimalVector. */
class DecimalVectorSetter extends BaseVectorSetter<DecimalVector, BigDecimal> {
    DecimalVectorSetter() {
        super(BigDecimal.class);
    }

    @Override
    protected void setValueInternal(DecimalVector vector, BigDecimal value) {
        vector.setSafe(0, value.unscaledValue().longValue());
    }

    @Override
    protected void setNullValue(DecimalVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for DateDayVector. */
class DateDayVectorSetter extends BaseVectorSetter<DateDayVector, Date> {
    DateDayVectorSetter() {
        super(Date.class);
    }

    @Override
    protected void setValueInternal(DateDayVector vector, Date value) {
        long daysSinceEpoch = value.toLocalDate().toEpochDay();
        vector.setSafe(0, (int) daysSinceEpoch);
    }

    @Override
    protected void setNullValue(DateDayVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for TimeMicroVector. */
class TimeMicroVectorSetter extends BaseVectorSetter<TimeMicroVector, Time> {
    private final Calendar calendar;

    TimeMicroVectorSetter(Calendar calendar) {
        super(Time.class);
        this.calendar = calendar;
    }

    @Override
    protected void setValueInternal(TimeMicroVector vector, Time value) {
        LocalDateTime localDateTime = new Timestamp(value.getTime()).toLocalDateTime();
        localDateTime = DateTimeUtils.adjustForCalendar(localDateTime, calendar, TimeZone.getTimeZone("UTC"));
        long midnightMillis = localDateTime.toLocalTime().toNanoOfDay() / 1_000_000;
        long microsecondsSinceMidnight = DateTimeUtils.millisToMicrosecondsSinceMidnight(midnightMillis);

        vector.setSafe(0, microsecondsSinceMidnight);
    }

    @Override
    protected void setNullValue(TimeMicroVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for TimeStampMicroTZVector. */
class TimeStampMicroTZVectorSetter extends BaseVectorSetter<TimeStampMicroTZVector, Timestamp> {
    private final Calendar calendar;

    TimeStampMicroTZVectorSetter(Calendar calendar) {
        super(Timestamp.class);
        this.calendar = calendar;
    }

    @Override
    protected void setValueInternal(TimeStampMicroTZVector vector, Timestamp value) {
        LocalDateTime localDateTime = value.toLocalDateTime();
        localDateTime = DateTimeUtils.adjustForCalendar(localDateTime, calendar, TimeZone.getTimeZone("UTC"));
        long microsecondsSinceEpoch = DateTimeUtils.localDateTimeToMicrosecondsSinceEpoch(localDateTime);

        vector.setSafe(0, microsecondsSinceEpoch);
    }

    @Override
    protected void setNullValue(TimeStampMicroTZVector vector) {
        vector.setNull(0);
    }
}

/** Setter implementation for TinyIntVectorSetter. */
class TinyIntVectorSetter extends BaseVectorSetter<TinyIntVector, Byte> {
    TinyIntVectorSetter() {
        super(Byte.class);
    }

    @Override
    protected void setValueInternal(TinyIntVector vector, Byte value) {
        vector.setSafe(0, value);
    }

    @Override
    protected void setNullValue(TinyIntVector vector) {
        vector.setNull(0);
    }
}
