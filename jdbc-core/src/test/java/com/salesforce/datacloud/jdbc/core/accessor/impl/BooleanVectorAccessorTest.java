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

import static com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorAssert.assertThat;

import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class BooleanVectorAccessorTest {
    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    private static final Random random = new Random(10);
    private static final List<Boolean> values = Stream.concat(
                    IntStream.range(0, 15).mapToObj(i -> random.nextBoolean()), Stream.of(null, null, null, null, null))
            .collect(Collectors.toList());
    private static final int expectedNulls =
            (int) values.stream().filter(Objects::isNull).count();
    private static final int expectedNonNulls = values.size() - expectedNulls;

    @BeforeAll
    static void setup() {
        Collections.shuffle(values);
    }

    private void iterate(BuildThrowingConsumer builder) {
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = extension.createBitVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BooleanVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                val s = builder.buildSatisfies(expected);
                collector
                        .assertThat(sut)
                        .hasObjectClass(Boolean.class)
                        .satisfies(b -> s.accept((BooleanVectorAccessor) b));
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls).hasNullSeen(expectedNulls);
    }

    @FunctionalInterface
    private interface BuildThrowingConsumer {
        ThrowingConsumer<BooleanVectorAccessor> buildSatisfies(Boolean expected);
    }

    @Test
    void testShouldGetBooleanMethodFromBitVector() {
        iterate(expected -> sut -> assertThat(sut).hasBoolean(expected != null && expected));
    }

    @Test
    void testShouldGetByteMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasByte(expected == null || !expected ? (byte) 0 : (byte) 1));
    }

    @Test
    void testShouldGetShortMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasShort(expected == null || !expected ? (short) 0 : (short) 1));
    }

    @Test
    void testShouldGetIntMethodFromBitVector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasInt(expected == null || !expected ? 0 : 1));
    }

    @Test
    void testShouldGetFloatMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasFloat(expected == null || !expected ? (float) 0 : (float) 1));
    }

    @Test
    void testShouldGetDoubleMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasDouble(expected == null || !expected ? (double) 0 : (double) 1));
    }

    @Test
    void testShouldGetLongMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasLong(expected == null || !expected ? (long) 0 : (long) 1));
    }

    @Test
    void testShouldGetBigDecimalMethodFromBitVector() {
        iterate(expected -> sut -> collector
                .assertThat(sut)
                .hasBigDecimal(expected == null ? null : (expected ? BigDecimal.ONE : BigDecimal.ZERO)));
    }

    @Test
    void testShouldGetStringMethodFromBitVector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasString(expected == null ? null : (expected ? "true" : "false")));
    }

    @Test
    void testShouldGetBooleanMethodFromBitVectorFromNull() {
        iterate(expected -> sut -> collector.assertThat(sut).hasBoolean(expected != null && expected));
    }

    @Test
    void testShouldGetObjectMethodFromBitVector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasObject(expected));
    }
}
