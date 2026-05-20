/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pin the strict-String contract on VarCharVectorSetter: non-String / non-null payloads are
 * rejected by the BaseVectorSetter type guard rather than silently coerced via toString. Without
 * this test, a future widening of the generic from String back to Object would slip past CI and
 * re-introduce the typeInfoRows Boolean-as-VARCHAR regression that motivated the strict typing.
 */
class VarCharVectorSetterStrictTypeTest {

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
    void varCharSetterAcceptsString() {
        try (val vector = new VarCharVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new VarCharVectorSetter();
            setter.setValue(vector, 0, "hello");
            vector.setValueCount(1);
            assertThat(new String(vector.get(0), StandardCharsets.UTF_8)).isEqualTo("hello");
        }
    }

    @Test
    void varCharSetterRejectsBoolean() {
        try (val vector = new VarCharVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new VarCharVectorSetter();
            assertThatThrownBy(() -> setter.setValue(vector, 0, Boolean.TRUE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be of type String");
        }
    }

    @Test
    void varCharSetterRejectsByteArray() {
        try (val vector = new VarCharVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new VarCharVectorSetter();
            assertThatThrownBy(() -> setter.setValue(vector, 0, new byte[] {1, 2, 3}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be of type String");
        }
    }

    @Test
    void varCharSetterRejectsNumber() {
        try (val vector = new VarCharVector("col", allocator)) {
            vector.allocateNew(1);
            val setter = new VarCharVectorSetter();
            assertThatThrownBy(() -> setter.setValue(vector, 0, 42))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be of type String");
        }
    }
}
