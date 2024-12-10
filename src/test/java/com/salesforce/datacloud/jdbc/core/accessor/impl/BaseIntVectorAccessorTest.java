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

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class BaseIntVectorAccessorTest {

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @InjectSoftAssertions
    private SoftAssertions collector;

    @SneakyThrows
    @Test
    public void testShouldConvertToTinyIntMethodFromBaseIntVector() {
        val values = getTinyIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTinyIntVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasByte(expected)
                        .hasShort(expected)
                        .hasInt(expected)
                        .hasFloat(expected)
                        .hasDouble(expected)
                        .hasBigDecimal(new BigDecimal(expected))
                        .hasObject(expected)
                        .hasObjectClass(Long.class);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 7);
    }

    @SneakyThrows
    @Test
    public void testShouldConvertToSmallIntMethodFromBaseIntVector() {
        val values = getSmallIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createSmallIntVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasShort(expected)
                        .hasInt((int) expected)
                        .hasFloat(expected)
                        .hasDouble(expected)
                        .hasBigDecimal(new BigDecimal(expected))
                        .hasObject(expected)
                        .hasObjectClass(Long.class);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 6);
    }

    @SneakyThrows
    @Test
    public void testShouldConvertToIntegerMethodFromBaseIntVector() {
        val values = getIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createIntVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasInt(expected)
                        .hasFloat(expected)
                        .hasDouble(expected)
                        .hasBigDecimal(new BigDecimal(expected))
                        .hasObject(expected)
                        .hasObjectClass(Long.class);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 5);
    }

    @SneakyThrows
    @Test
    public void testShouldConvertToBigIntMethodFromBaseIntVector() {
        val values = getBigIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createBigIntVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasFloat(expected)
                        .hasDouble(expected)
                        .hasBigDecimal(new BigDecimal(expected))
                        .hasObject(expected)
                        .hasObjectClass(Long.class);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 4);
    }

    @SneakyThrows
    @Test
    public void testShouldConvertToUInt4MethodFromBaseIntVector() {
        val values = getIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createUInt4Vector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasInt(expected)
                        .hasFloat(expected)
                        .hasDouble(expected)
                        .hasBigDecimal(new BigDecimal(expected))
                        .hasObject(expected)
                        .hasObjectClass(Long.class);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 5);
    }

    @SneakyThrows
    @Test
    public void testGetBigDecimalGetObjectAndGetObjectClassFromNulledDecimalVector() {
        val values = getBigIntValues();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createBigIntVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new BaseIntVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasBigDecimal(null).hasObject(null).hasObjectClass(Long.class);
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size() * 2);
    }

    private List<Byte> getTinyIntValues() {
        return ImmutableList.of((byte) 0, (byte) 1, (byte) -1, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    private List<Short> getSmallIntValues() {
        return ImmutableList.of(
                (short) 0,
                (short) 1,
                (short) -1,
                (short) Byte.MIN_VALUE,
                (short) Byte.MAX_VALUE,
                Short.MIN_VALUE,
                Short.MAX_VALUE);
    }

    private List<Integer> getIntValues() {
        return ImmutableList.of(
                0,
                1,
                -1,
                (int) Byte.MIN_VALUE,
                (int) Byte.MAX_VALUE,
                (int) Short.MIN_VALUE,
                (int) Short.MAX_VALUE,
                Integer.MIN_VALUE,
                Integer.MAX_VALUE);
    }

    private List<Long> getBigIntValues() {
        return ImmutableList.of(
                (long) 0,
                (long) 1,
                (long) -1,
                (long) Byte.MIN_VALUE,
                (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE,
                (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MAX_VALUE);
    }
}
