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

import static java.lang.Byte.MAX_VALUE;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.val;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@Getter
public class RootAllocatorTestExtension implements AfterAllCallback, AutoCloseable {

    private final BufferAllocator rootAllocator = new RootAllocator();
    private final Random random = new Random(10);

    @Override
    public void afterAll(ExtensionContext context) {
        try {
            close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.rootAllocator.getChildAllocators().forEach(BufferAllocator::close);
        AutoCloseables.close(this.rootAllocator);
    }

    public Float8Vector createFloat8Vector(List<Double> values) {
        val vector = new Float8Vector("test-double-vector", getRootAllocator());
        vector.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            Double d = values.get(i);
            if (d == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, d);
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }

    public VarCharVector createVarCharVectorFrom(List<String> values) {
        val vector = new VarCharVector("test-varchar-vector", getRootAllocator());
        vector.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            vector.setSafe(i, values.get(i).getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    public LargeVarCharVector createLargeVarCharVectorFrom(List<String> values) {
        val vector = new LargeVarCharVector("test-large-varchar-vector", getRootAllocator());
        vector.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            vector.setSafe(i, values.get(i).getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    public DecimalVector createDecimalVector(List<BigDecimal> values) {
        val vector = new DecimalVector("test-decimal-vector", getRootAllocator(), 39, 0);
        vector.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            vector.setSafe(i, values.get(i));
        }
        return vector;
    }

    public BitVector createBitVector(List<Boolean> values) {
        BitVector vector = new BitVector("Value", this.getRootAllocator());
        vector.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            Boolean b = values.get(i);
            if (b == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, b ? 1 : 0);
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }

    public static <T extends ValueVector> T nulledOutVector(T vector) {
        val original = vector.getValueCount();
        vector.clear();
        vector.setValueCount(original);
        return vector;
    }

    public DateDayVector createDateDayVector() {
        return new DateDayVector("DateDay", this.getRootAllocator());
    }

    public DateMilliVector createDateMilliVector() {
        return new DateMilliVector("DateMilli", this.getRootAllocator());
    }

    public static <T extends DateDayVector> T appendDates(List<Long> values, T vector) {
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.stream()
                .map(TimeUnit.MILLISECONDS::toDays)
                .forEachOrdered(t -> vector.setSafe(i.getAndIncrement(), Math.toIntExact(t)));
        vector.setValueCount(values.size());
        return vector;
    }

    public static <T extends DateMilliVector> T appendDates(List<Long> values, T vector) {
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.forEach(t -> vector.setSafe(i.getAndIncrement(), t));
        vector.setValueCount(values.size());
        return vector;
    }

    public TimeNanoVector createTimeNanoVector(List<Long> values) {
        TimeNanoVector vector = new TimeNanoVector("TimeNano", this.getRootAllocator());
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.forEach(t -> vector.setSafe(i.getAndIncrement(), t));
        vector.setValueCount(values.size());
        return vector;
    }

    public TimeMicroVector createTimeMicroVector(List<Long> values) {
        TimeMicroVector vector = new TimeMicroVector("TimeMicro", this.getRootAllocator());
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.forEach(t -> vector.setSafe(i.getAndIncrement(), t));
        vector.setValueCount(values.size());
        return vector;
    }

    public TimeMilliVector createTimeMilliVector(List<Integer> values) {
        TimeMilliVector vector = new TimeMilliVector("TimeMilli", this.getRootAllocator());
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.forEach(t -> vector.setSafe(i.getAndIncrement(), t));
        vector.setValueCount(values.size());
        return vector;
    }

    public TimeSecVector createTimeSecVector(List<Integer> values) {
        TimeSecVector vector = new TimeSecVector("TimeSec", this.getRootAllocator());
        AtomicInteger i = new AtomicInteger(0);
        vector.allocateNew(values.size());
        values.forEach(t -> vector.setSafe(i.getAndIncrement(), t));
        vector.setValueCount(values.size());
        return vector;
    }

    /**
     * Create a VarBinaryVector to be used in the accessor tests.
     *
     * @return VarBinaryVector
     */
    public VarBinaryVector createVarBinaryVector(List<byte[]> values) {
        VarBinaryVector vector = new VarBinaryVector("test-varbinary-vector", this.getRootAllocator());
        vector.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            byte[] b = values.get(i);
            if (b == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, values.get(i));
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }

    /**
     * Create a LargeVarBinaryVector to be used in the accessor tests.
     *
     * @return LargeVarBinaryVector
     */
    public LargeVarBinaryVector createLargeVarBinaryVector(List<byte[]> values) {
        LargeVarBinaryVector vector = new LargeVarBinaryVector("test-large-varbinary-vector", this.getRootAllocator());
        vector.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            byte[] b = values.get(i);
            if (b == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, values.get(i));
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }

    /**
     * Create a FixedSizeBinaryVector to be used in the accessor tests.
     *
     * @return FixedSizeBinaryVector
     */
    public FixedSizeBinaryVector createFixedSizeBinaryVector(List<byte[]> values) {
        FixedSizeBinaryVector vector =
                new FixedSizeBinaryVector("test-fixedsize-varbinary-vector", this.getRootAllocator(), 16);
        vector.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            byte[] b = values.get(i);
            if (b == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, values.get(i));
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }

    /**
     * Create a TinyIntVector to be used in the accessor tests.
     *
     * @return TinyIntVector
     */
    public TinyIntVector createTinyIntVector(List<Byte> values) {
        TinyIntVector result = new TinyIntVector("test-tinyInt-Vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }

        return result;
    }

    /**
     * Create a SmallIntVector to be used in the accessor tests.
     *
     * @return SmallIntVector
     */
    public SmallIntVector createSmallIntVector(List<Short> values) {

        SmallIntVector result = new SmallIntVector("test-smallInt-Vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }

        return result;
    }

    /**
     * Create a IntVector to be used in the accessor tests.
     *
     * @return IntVector
     */
    public IntVector createIntVector(List<Integer> values) {
        IntVector result = new IntVector("test-int-vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }

        return result;
    }

    /**
     * Create a UInt4Vector to be used in the accessor tests.
     *
     * @return UInt4Vector
     */
    public UInt4Vector createUInt4Vector(List<Integer> values) {
        UInt4Vector result = new UInt4Vector("test-uint4-vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }

        return result;
    }

    /**
     * Create a BigIntVector to be used in the accessor tests.
     *
     * @return BigIntVector
     */
    public BigIntVector createBigIntVector(List<Long> values) {
        BigIntVector result = new BigIntVector("test-bitInt-vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }
        return result;
    }

    public TimeStampNanoVector createTimeStampNanoVector(List<Long> values) {
        TimeStampNanoVector result = new TimeStampNanoVector("", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toNanos(values.get(i)));
        }
        return result;
    }

    public TimeStampNanoTZVector createTimeStampNanoTZVector(List<Long> values, String timeZone) {
        TimeStampNanoTZVector result = new TimeStampNanoTZVector("", this.getRootAllocator(), timeZone);
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toNanos(values.get(i)));
        }
        return result;
    }

    public TimeStampMicroVector createTimeStampMicroVector(List<Long> values) {
        TimeStampMicroVector result = new TimeStampMicroVector("", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toMicros(values.get(i)));
        }
        return result;
    }

    public TimeStampMicroTZVector createTimeStampMicroTZVector(List<Long> values, String timeZone) {
        TimeStampMicroTZVector result = new TimeStampMicroTZVector("", this.getRootAllocator(), timeZone);
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toMicros(values.get(i)));
        }
        return result;
    }

    public TimeStampMilliVector createTimeStampMilliVector(List<Long> values) {
        TimeStampMilliVector result = new TimeStampMilliVector("test-milli-vector", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }
        return result;
    }

    public TimeStampMilliTZVector createTimeStampMilliTZVector(List<Long> values, String timeZone) {
        TimeStampMilliTZVector result = new TimeStampMilliTZVector("", this.getRootAllocator(), timeZone);
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, values.get(i));
        }
        return result;
    }

    public TimeStampSecVector createTimeStampSecVector(List<Long> values) {
        TimeStampSecVector result = new TimeStampSecVector("", this.getRootAllocator());
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toSeconds(values.get(i)));
        }
        return result;
    }

    public TimeStampSecTZVector createTimeStampSecTZVector(List<Long> values, String timeZone) {
        TimeStampSecTZVector result = new TimeStampSecTZVector("", this.getRootAllocator(), timeZone);
        result.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            result.setSafe(i, TimeUnit.MILLISECONDS.toSeconds(values.get(i)));
        }
        return result;
    }

    public ListVector createListVector(String fieldName) {
        ListVector listVector = ListVector.empty(fieldName, this.getRootAllocator());
        listVector.setInitialCapacity(MAX_VALUE);

        UnionListWriter writer = listVector.getWriter();

        IntStream.range(0, MAX_VALUE).forEach(row -> {
            writer.startList();
            writer.setPosition(row);
            IntStream.range(0, 5).map(j -> j * row).forEach(writer::writeInt);
            writer.setValueCount(5);
            writer.endList();
        });

        listVector.setValueCount(MAX_VALUE);
        return listVector;
    }

    public LargeListVector createLargeListVector(String fieldName) {
        LargeListVector largeListVector = LargeListVector.empty(fieldName, this.getRootAllocator());
        largeListVector.setInitialCapacity(MAX_VALUE);

        UnionLargeListWriter writer = largeListVector.getWriter();

        IntStream.range(0, MAX_VALUE).forEach(row -> {
            writer.startList();
            writer.setPosition(row);
            IntStream.range(0, 5).map(j -> j * row).forEach(writer::writeInt);
            writer.setValueCount(5);
            writer.endList();
        });

        largeListVector.setValueCount(MAX_VALUE);
        return largeListVector;
    }
}
