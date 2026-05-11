/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pin the range-check behavior on the integer-family vector setters: every narrowing setter
 * (TinyInt/SmallInt/Int) refuses out-of-range Number inputs rather than silently truncating.
 * BigInt accepts the full long range.
 *
 * <p>Both code paths (parameter binding via DataCloudPreparedStatement.setObject and metadata
 * row population via MetadataResultSets) reach these same setters, so strict checks here mean
 * strict checks on both paths.
 */
class IntegerVectorSetterRangeCheckTest {

    private RootAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void intVectorSetterAcceptsValuesInRange() {
        try (val vector = new IntVector("col", allocator)) {
            vector.allocateNew(3);
            val setter = new IntVectorSetter();
            setter.setValueInternal(vector, 0, 0);
            setter.setValueInternal(vector, 1, Integer.MAX_VALUE);
            setter.setValueInternal(vector, 2, Long.valueOf(Integer.MIN_VALUE));
            vector.setValueCount(3);
            assertThat(vector.get(0)).isEqualTo(0);
            assertThat(vector.get(1)).isEqualTo(Integer.MAX_VALUE);
            assertThat(vector.get(2)).isEqualTo(Integer.MIN_VALUE);
        }
    }

    @Test
    void intVectorSetterRejectsLongAboveRange() {
        try (val vector = new IntVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new IntVectorSetter();
            assertThatThrownBy(() -> setter.setValueInternal(vector, 0, Long.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of range for INT32");
        }
    }

    @Test
    void intVectorSetterRejectsLongBelowRange() {
        try (val vector = new IntVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new IntVectorSetter();
            assertThatThrownBy(() -> setter.setValueInternal(vector, 0, Long.MIN_VALUE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of range for INT32");
        }
    }

    @Test
    void smallIntVectorSetterRejectsValueAboveRange() {
        try (val vector = new SmallIntVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new SmallIntVectorSetter();
            assertThatThrownBy(() -> setter.setValueInternal(vector, 0, (long) Short.MAX_VALUE + 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of range for INT16");
        }
    }

    @Test
    void tinyIntVectorSetterRejectsValueAboveRange() {
        try (val vector = new TinyIntVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new TinyIntVectorSetter();
            assertThatThrownBy(() -> setter.setValueInternal(vector, 0, (long) Byte.MAX_VALUE + 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of range for INT8");
        }
    }

    @Test
    void bigIntVectorSetterAcceptsFullLongRange() {
        try (val vector = new BigIntVector("col", allocator)) {
            vector.allocateNew(2);
            val setter = new BigIntVectorSetter();
            setter.setValueInternal(vector, 0, Long.MAX_VALUE);
            setter.setValueInternal(vector, 1, Long.MIN_VALUE);
            vector.setValueCount(2);
            assertThat(vector.get(0)).isEqualTo(Long.MAX_VALUE);
            assertThat(vector.get(1)).isEqualTo(Long.MIN_VALUE);
        }
    }
}
