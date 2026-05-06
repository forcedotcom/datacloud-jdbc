/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.adjustForCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.millisToMicrosecondsSinceMidnight;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Populates vectors in a VectorSchemaRoot with values from a list of parameters. */
public final class VectorPopulator {

    private VectorPopulator() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Populates the vectors in the given VectorSchemaRoot using the {@link HyperType} of each
     * parameter to decide which typed setter to dispatch to.
     *
     * @param root the VectorSchemaRoot to populate
     */
    public static void populateVectors(VectorSchemaRoot root, List<ParameterBinding> parameters, Calendar calendar) {
        VectorValueSetterFactory factory = new VectorValueSetterFactory(calendar);

        for (int i = 0; i < parameters.size(); i++) {
            ParameterBinding binding = parameters.get(i);
            if (binding == null) {
                // No binding for this parameter — vector stays as created (null values). We cannot
                // invoke a setter without a HyperTypeKind to dispatch on.
                continue;
            }
            HyperTypeKind kind = binding.getType().getKind();
            ValueVector vector =
                    root.getVector(root.getSchema().getFields().get(i).getName());
            Object value = binding.getValue();

            @SuppressWarnings(value = "unchecked")
            VectorValueSetter<ValueVector> setter = (VectorValueSetter<ValueVector>) factory.getSetter(kind);

            if (setter != null) {
                setter.setValue(vector, value);
            } else {
                throw new UnsupportedOperationException("Unsupported HyperTypeKind for parameter binding: " + kind);
            }
        }
        root.setRowCount(1); // Set row count to 1 since we have exactly one row
    }
}

@FunctionalInterface
interface VectorValueSetter<T extends ValueVector> {
    void setValue(T vector, Object value);
}

/** Factory for creating appropriate setter instances based on {@link HyperTypeKind}. */
class VectorValueSetterFactory {
    private final Map<HyperTypeKind, VectorValueSetter<?>> setterMap;

    VectorValueSetterFactory(Calendar calendar) {
        setterMap = ImmutableMap.ofEntries(
                Maps.immutableEntry(HyperTypeKind.VARCHAR, new VarCharVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.CHAR, new VarCharVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.FLOAT4, new Float4VectorSetter()),
                Maps.immutableEntry(HyperTypeKind.FLOAT8, new Float8VectorSetter()),
                Maps.immutableEntry(HyperTypeKind.INT32, new IntVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.INT16, new SmallIntVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.INT64, new BigIntVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.BOOL, new BitVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.DECIMAL, new DecimalVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.DATE, new DateDayVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.TIME, new TimeMicroVectorSetter(calendar)),
                Maps.immutableEntry(HyperTypeKind.TIMESTAMP, new TimeStampMicroVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.TIMESTAMP_TZ, new TimeStampMicroTZVectorSetter()),
                Maps.immutableEntry(HyperTypeKind.INT8, new TinyIntVectorSetter()));
    }

    VectorValueSetter<?> getSetter(HyperTypeKind kind) {
        return setterMap.get(kind);
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
        localDateTime = adjustForCalendar(localDateTime, calendar, TimeZone.getTimeZone("UTC"));
        long midnightMillis = localDateTime.toLocalTime().toNanoOfDay() / 1_000_000;
        long microsecondsSinceMidnight = millisToMicrosecondsSinceMidnight(midnightMillis);

        vector.setSafe(0, microsecondsSinceMidnight);
    }

    @Override
    protected void setNullValue(TimeMicroVector vector) {
        vector.setNull(0);
    }
}

/**
 * Setter for naive TimeStampMicroVector (no timezone metadata).
 *
 * <p>The Timestamp value arriving here has already been normalized by
 * {@code DataCloudPreparedStatement.toWallClockAsUtc}: its UTC instant encodes the
 * wall-clock digits the user intended to store (in their effective timezone). We read
 * those digits back via {@code toInstant()} and store them as the naive epoch, so Hyper
 * receives the literal wall-clock value directly.
 */
class TimeStampMicroVectorSetter extends BaseVectorSetter<TimeStampMicroVector, Timestamp> {

    TimeStampMicroVectorSetter() {
        super(Timestamp.class);
    }

    @Override
    protected void setValueInternal(TimeStampMicroVector vector, Timestamp value) {
        Instant instant = value.toInstant();
        long microsecondsSinceEpoch = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        vector.setSafe(0, microsecondsSinceEpoch);
    }

    @Override
    protected void setNullValue(TimeStampMicroVector vector) {
        vector.setNull(0);
    }
}

/**
 * Setter for TZ-aware TimeStampMicroTZVector (TIMESTAMP WITH TIME ZONE).
 * Stores the Timestamp's true UTC instant — no wall-clock shift applied.
 * Used when the parameter is bound as {@link HyperType#timestampTz(boolean)}.
 */
class TimeStampMicroTZVectorSetter extends BaseVectorSetter<TimeStampMicroTZVector, Timestamp> {

    TimeStampMicroTZVectorSetter() {
        super(Timestamp.class);
    }

    @Override
    protected void setValueInternal(TimeStampMicroTZVector vector, Timestamp value) {
        Instant instant = value.toInstant();
        long microsecondsSinceEpoch = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
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
