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

/**
 * Populates vectors in a {@link VectorSchemaRoot} with Java values, dispatching per column by
 * {@link HyperTypeKind}.
 *
 * <p>The primitive {@link #setCell(ValueVector, HyperTypeKind, int, Object, Calendar)} is the
 * single place the driver converts a Java {@link Object} into the right Arrow setter call. Both
 * the JDBC parameter-encoding path (single row) and the JDBC metadata path (many rows) go
 * through it.
 */
public final class VectorPopulator {

    private VectorPopulator() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Populate a single-row VSR from a list of {@link ParameterBinding}s. Used by the parameter
     * encoding path; the VSR's schema is built from the bindings' {@link HyperType}s.
     */
    public static void populateVectors(VectorSchemaRoot root, List<ParameterBinding> parameters, Calendar calendar) {
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
            setCell(vector, kind, 0, binding.getValue(), calendar);
        }
        root.setRowCount(1);
    }

    /**
     * Populate {@code root} from a row-major list of Java values. {@code columns} supplies the
     * per-column {@link HyperType} used to dispatch the setter; row {@code r}, column {@code c}
     * is taken from {@code rows.get(r).get(c)} (a missing/short row yields a null cell).
     *
     * <p>Row count is set to {@code rows.size()}. Used by the metadata path.
     */
    public static void populateVectors(
            VectorSchemaRoot root, List<ColumnMetadata> columns, List<List<Object>> rows, Calendar calendar) {
        int rowCount = rows == null ? 0 : rows.size();
        for (int c = 0; c < columns.size(); c++) {
            ValueVector vector = root.getVector(columns.get(c).getName());
            HyperTypeKind kind = columns.get(c).getType().getKind();
            for (int r = 0; r < rowCount; r++) {
                List<Object> row = rows.get(r);
                Object value = row == null || c >= row.size() ? null : row.get(c);
                setCell(vector, kind, r, value, calendar);
            }
            vector.setValueCount(rowCount);
        }
        root.setRowCount(rowCount);
    }

    /** Sets cell ({@code vector}, {@code index}) to {@code value}, or null if value is null. */
    static void setCell(ValueVector vector, HyperTypeKind kind, int index, Object value, Calendar calendar) {
        @SuppressWarnings("unchecked")
        VectorValueSetter<ValueVector> setter =
                (VectorValueSetter<ValueVector>) VectorValueSetterFactory.getSetter(kind, calendar);
        if (setter == null) {
            throw new UnsupportedOperationException("Unsupported HyperTypeKind for vector population: " + kind);
        }
        setter.setValue(vector, index, value);
    }
}

@FunctionalInterface
interface VectorValueSetter<T extends ValueVector> {
    void setValue(T vector, int index, Object value);
}

/** Factory for indexed setters keyed by {@link HyperTypeKind}. */
final class VectorValueSetterFactory {
    private VectorValueSetterFactory() {}

    private static final Map<HyperTypeKind, VectorValueSetter<?>> SETTERS_NO_CAL = build(null);

    private static Map<HyperTypeKind, VectorValueSetter<?>> build(Calendar calendar) {
        return ImmutableMap.ofEntries(
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

    static VectorValueSetter<?> getSetter(HyperTypeKind kind, Calendar calendar) {
        if (calendar == null) {
            return SETTERS_NO_CAL.get(kind);
        }
        // Only TIME uses the calendar; build a per-call map rather than caching so tests that
        // pass different Calendars cannot race. This path is cold (parameter-binding only).
        return build(calendar).get(kind);
    }
}

/** Base setter implementation for ValueVectors that need type validation. */
abstract class BaseVectorSetter<T extends ValueVector, V> implements VectorValueSetter<T> {
    private final Class<V> valueType;

    BaseVectorSetter(Class<V> valueType) {
        this.valueType = valueType;
    }

    @Override
    public void setValue(T vector, int index, Object value) {
        if (value == null) {
            setNullValue(vector, index);
        } else if (valueType.isInstance(value)) {
            setValueInternal(vector, index, valueType.cast(value));
        } else {
            throw new IllegalArgumentException(
                    "Value for " + vector.getClass().getSimpleName() + " must be of type " + valueType.getSimpleName());
        }
    }

    protected abstract void setNullValue(T vector, int index);

    protected abstract void setValueInternal(T vector, int index, V value);
}

/** Setter implementation for VarCharVector. */
class VarCharVectorSetter extends BaseVectorSetter<VarCharVector, Object> {
    VarCharVectorSetter() {
        super(Object.class); // accept String, Number, Boolean, byte[] — coerce to UTF-8 bytes
    }

    @Override
    protected void setValueInternal(VarCharVector vector, int index, Object value) {
        byte[] bytes =
                value instanceof byte[] ? (byte[]) value : value.toString().getBytes(StandardCharsets.UTF_8);
        vector.setSafe(index, bytes);
    }

    @Override
    protected void setNullValue(VarCharVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for Float4Vector. */
class Float4VectorSetter extends BaseVectorSetter<Float4Vector, Float> {
    Float4VectorSetter() {
        super(Float.class);
    }

    @Override
    protected void setValueInternal(Float4Vector vector, int index, Float value) {
        vector.setSafe(index, value);
    }

    @Override
    protected void setNullValue(Float4Vector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for Float8Vector. */
class Float8VectorSetter extends BaseVectorSetter<Float8Vector, Double> {
    Float8VectorSetter() {
        super(Double.class);
    }

    @Override
    protected void setValueInternal(Float8Vector vector, int index, Double value) {
        vector.setSafe(index, value);
    }

    @Override
    protected void setNullValue(Float8Vector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for IntVector. Accepts any Number to support metadata rows using long/int. */
class IntVectorSetter extends BaseVectorSetter<IntVector, Number> {
    IntVectorSetter() {
        super(Number.class);
    }

    @Override
    protected void setValueInternal(IntVector vector, int index, Number value) {
        vector.setSafe(index, value.intValue());
    }

    @Override
    protected void setNullValue(IntVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for SmallIntVector. */
class SmallIntVectorSetter extends BaseVectorSetter<SmallIntVector, Number> {
    SmallIntVectorSetter() {
        super(Number.class);
    }

    @Override
    protected void setValueInternal(SmallIntVector vector, int index, Number value) {
        vector.setSafe(index, value.shortValue());
    }

    @Override
    protected void setNullValue(SmallIntVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for BigIntVector. */
class BigIntVectorSetter extends BaseVectorSetter<BigIntVector, Number> {
    BigIntVectorSetter() {
        super(Number.class);
    }

    @Override
    protected void setValueInternal(BigIntVector vector, int index, Number value) {
        vector.setSafe(index, value.longValue());
    }

    @Override
    protected void setNullValue(BigIntVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for BitVector. */
class BitVectorSetter extends BaseVectorSetter<BitVector, Boolean> {
    BitVectorSetter() {
        super(Boolean.class);
    }

    @Override
    protected void setValueInternal(BitVector vector, int index, Boolean value) {
        vector.setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
    }

    @Override
    protected void setNullValue(BitVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for DecimalVector. */
class DecimalVectorSetter extends BaseVectorSetter<DecimalVector, BigDecimal> {
    DecimalVectorSetter() {
        super(BigDecimal.class);
    }

    @Override
    protected void setValueInternal(DecimalVector vector, int index, BigDecimal value) {
        vector.setSafe(index, value.unscaledValue().longValue());
    }

    @Override
    protected void setNullValue(DecimalVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for DateDayVector. */
class DateDayVectorSetter extends BaseVectorSetter<DateDayVector, Date> {
    DateDayVectorSetter() {
        super(Date.class);
    }

    @Override
    protected void setValueInternal(DateDayVector vector, int index, Date value) {
        long daysSinceEpoch = value.toLocalDate().toEpochDay();
        vector.setSafe(index, (int) daysSinceEpoch);
    }

    @Override
    protected void setNullValue(DateDayVector vector, int index) {
        vector.setNull(index);
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
    protected void setValueInternal(TimeMicroVector vector, int index, Time value) {
        LocalDateTime localDateTime = new Timestamp(value.getTime()).toLocalDateTime();
        localDateTime = adjustForCalendar(localDateTime, calendar, TimeZone.getTimeZone("UTC"));
        long midnightMillis = localDateTime.toLocalTime().toNanoOfDay() / 1_000_000;
        long microsecondsSinceMidnight = millisToMicrosecondsSinceMidnight(midnightMillis);

        vector.setSafe(index, microsecondsSinceMidnight);
    }

    @Override
    protected void setNullValue(TimeMicroVector vector, int index) {
        vector.setNull(index);
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
    protected void setValueInternal(TimeStampMicroVector vector, int index, Timestamp value) {
        Instant instant = value.toInstant();
        long microsecondsSinceEpoch = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        vector.setSafe(index, microsecondsSinceEpoch);
    }

    @Override
    protected void setNullValue(TimeStampMicroVector vector, int index) {
        vector.setNull(index);
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
    protected void setValueInternal(TimeStampMicroTZVector vector, int index, Timestamp value) {
        Instant instant = value.toInstant();
        long microsecondsSinceEpoch = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        vector.setSafe(index, microsecondsSinceEpoch);
    }

    @Override
    protected void setNullValue(TimeStampMicroTZVector vector, int index) {
        vector.setNull(index);
    }
}

/** Setter implementation for TinyIntVectorSetter. */
class TinyIntVectorSetter extends BaseVectorSetter<TinyIntVector, Number> {
    TinyIntVectorSetter() {
        super(Number.class);
    }

    @Override
    protected void setValueInternal(TinyIntVector vector, int index, Number value) {
        vector.setSafe(index, value.byteValue());
    }

    @Override
    protected void setNullValue(TinyIntVector vector, int index) {
        vector.setNull(index);
    }
}
