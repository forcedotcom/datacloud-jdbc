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
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.core.accessor.impl.BaseIntVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.BinaryVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.BooleanVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DateVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DecimalVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DoubleVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.LargeListVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.ListVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.VarCharVectorAccessor;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.IntSupplier;
import lombok.SneakyThrows;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class QueryJDBCAccessorFactoryTest {
    public static final IntSupplier GET_CURRENT_ROW = () -> 0;

    List<byte[]> binaryList = ImmutableList.of(
            "BINARY_DATA_0001".getBytes(StandardCharsets.UTF_8),
            "BINARY_DATA_0002".getBytes(StandardCharsets.UTF_8),
            "BINARY_DATA_0003".getBytes(StandardCharsets.UTF_8));

    List<Integer> uint4List = ImmutableList.of(
            0,
            1,
            -1,
            (int) Byte.MIN_VALUE,
            (int) Byte.MAX_VALUE,
            (int) Short.MIN_VALUE,
            (int) Short.MAX_VALUE,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE);

    @RegisterExtension
    public static RootAllocatorTestExtension rootAllocatorTestExtension = new RootAllocatorTestExtension();

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsVarChar() {
        try (ValueVector valueVector = new VarCharVector("VarChar", rootAllocatorTestExtension.getRootAllocator())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(VarCharVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsLargeVarChar() {
        try (ValueVector valueVector =
                new LargeVarCharVector("LargeVarChar", rootAllocatorTestExtension.getRootAllocator())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(VarCharVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsDecimal() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createDecimalVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(DecimalVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsBoolean() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createBitVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BooleanVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsFloat8() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createFloat8Vector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(DoubleVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsDateDay() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createDateDayVector()) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(DateVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsDateMilli() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createDateMilliVector()) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(DateVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeNano() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeNanoVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(TimeVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeMicro() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeMicroVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(TimeVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeMilli() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeMilliVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(TimeVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeSec() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeSecVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            assertThat(accessor).isInstanceOf(TimeVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsUnsupportedVector() {
        try (ValueVector valueVector = new NullVector("Null")) {
            Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> QueryJDBCAccessorFactory.createAccessor(
                            valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {}));
        }
    }

    @Test
    @SneakyThrows
    public void testCreateAccessorCorrectlyDetectsTinyInt() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTinyIntVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BaseIntVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    public void testCreateAccessorCorrectlyDetectsSmallInt() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createSmallIntVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BaseIntVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    public void testCreateAccessorCorrectlyDetectsInt() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createIntVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BaseIntVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    public void testCreateAccessorCorrectlyDetectsBigInt() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createBigIntVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BaseIntVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    public void testCreateAccessorCorrectlyDetectsUInt4() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createUInt4Vector(uint4List)) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});
            Assertions.assertInstanceOf(BaseIntVectorAccessor.class, accessor);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsVarBinaryVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createVarBinaryVector(binaryList)) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(BinaryVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsLargeVarBinaryVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createLargeVarBinaryVector(binaryList)) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(BinaryVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsFixedSizeBinaryVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createFixedSizeBinaryVector(binaryList)) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(BinaryVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampNanoVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeStampNanoVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampNanoTZVector() {
        try (ValueVector valueVector =
                rootAllocatorTestExtension.createTimeStampNanoTZVector(ImmutableList.of(), "UTC")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampMicroVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeStampMicroVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampMicroTZVector() {
        try (ValueVector valueVector =
                rootAllocatorTestExtension.createTimeStampMicroTZVector(ImmutableList.of(), "UTC")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampMilliVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeStampMilliVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampMilliTZVector() {
        try (ValueVector valueVector =
                rootAllocatorTestExtension.createTimeStampMilliTZVector(ImmutableList.of(), "UTC")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampSecVector() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createTimeStampSecVector(ImmutableList.of())) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsTimeStampSecTZVector() {
        try (ValueVector valueVector =
                rootAllocatorTestExtension.createTimeStampSecTZVector(ImmutableList.of(), "UTC")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(TimeStampVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsListVectorAccessor() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createListVector("list-vector")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(ListVectorAccessor.class);
        }
    }

    @Test
    @SneakyThrows
    void testCreateAccessorCorrectlyDetectsLargeListVectorAccessor() {
        try (ValueVector valueVector = rootAllocatorTestExtension.createLargeListVector("large-list-vector")) {
            QueryJDBCAccessor accessor =
                    QueryJDBCAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

            assertThat(accessor).isInstanceOf(LargeListVectorAccessor.class);
        }
    }
}
