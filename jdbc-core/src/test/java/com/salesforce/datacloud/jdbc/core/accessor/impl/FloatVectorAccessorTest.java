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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class FloatVectorAccessorTest {
    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    private static final int total = 10;
    private static final Random random = new Random(10);
    private static final List<Float> values = IntStream.range(0, total - 2)
            .mapToDouble(x -> random.nextFloat())
            .filter(Double::isFinite)
            .mapToObj(x -> (float) x)
            .collect(Collectors.toList());

    @BeforeAll
    static void setup() {
        values.add(null);
        values.add(null);
        Collections.shuffle(values);
    }

    private TestWasNullConsumer iterate(List<Float> values, BuildThrowingConsumer builder) {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createFloat4Vector(values)) {
            val i = new AtomicInteger(0);
            val sut = new FloatVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                val s = builder.buildSatisfies(expected);
                collector.assertThat(sut).satisfies(b -> s.accept((FloatVectorAccessor) b));
            }
        }

        return consumer;
    }

    private TestWasNullConsumer iterate(BuildThrowingConsumer builder) {
        val consumer = iterate(values, builder);
        consumer.assertThat().hasNullSeen(2).hasNotNullSeen(values.size() - 2);
        return consumer;
    }

    @FunctionalInterface
    private interface BuildThrowingConsumer {
        ThrowingConsumer<FloatVectorAccessor> buildSatisfies(Float expected);
    }

    @Test
    void testShouldGetFloatMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasFloat(expected == null ? 0.0f : expected));
    }

    @Test
    void testShouldGetObjectMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasObject(expected));
    }

    @Test
    void testShouldGetStringMethodFromFloat4Vector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasString(expected == null ? null : Float.toString(expected)));
    }

    @Test
    void testShouldGetBooleanMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasBoolean(expected != null && (expected != 0.0f)));
    }

    @Test
    void testShouldGetByteMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasByte((byte) (expected == null ? 0.0f : expected)));
    }

    @Test
    void testShouldGetShortMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasShort((short) (expected == null ? 0.0f : expected)));
    }

    @Test
    void testShouldGetIntMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasInt((int) (expected == null ? 0.0f : expected)));
    }

    @Test
    void testShouldGetLongMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasLong((long) (expected == null ? 0.0f : expected)));
    }

    @Test
    void testShouldGetDoubleMethodFromFloat4Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasDouble(expected == null ? 0.0f : expected));
    }

    @Test
    void testGetBigDecimalIllegalFloatsMethodFromFloat4Vector() {
        val consumer = iterate(
                ImmutableList.of(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN),
                expected -> sut -> assertThrows(DataCloudJDBCException.class, sut::getBigDecimal));
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(3);
    }

    @Test
    void testShouldGetBigDecimalWithScaleMethodFromFloat4Vector() {
        val scale = 9;
        val big = Float.MAX_VALUE;
        val expected = BigDecimal.valueOf(big).setScale(scale, RoundingMode.HALF_UP);
        iterate(
                ImmutableList.of(Float.MAX_VALUE),
                e -> sut -> collector.assertThat(sut.getBigDecimal(scale)).isEqualTo(expected));
    }
}
