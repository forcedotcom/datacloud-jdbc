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

import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class DecimalVectorAccessorTest {
    private static final int total = 8;
    private final Random random = new Random(10);

    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @SneakyThrows
    @Test
    void testGetBigDecimalGetObjectAndGetObjectClassFromValidDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasBigDecimal(expected)
                        .hasObject(expected)
                        .hasObjectClass(BigDecimal.class);
            }
        }

        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(values.size() * 2);
    }

    @SneakyThrows
    @Test
    void testGetBigDecimalGetObjectAndGetObjectClassFromNulledDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasBigDecimal(null).hasObject(null).hasObjectClass(BigDecimal.class);
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size() * 2);
    }

    @SneakyThrows
    @Test
    void testGetStringFromDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = values.get(i.get()).toString();
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(values.size()).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetStringFromNullDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                collector.assertThat(stringValue).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size());
    }

    @SneakyThrows
    @Test
    void testGetIntFromDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val intValue = sut.getInt();
                val expected = values.get(i.get()).intValue();
                collector.assertThat(intValue).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(values.size()).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetIntFromNullDecimalVector() {
        val values = getBigDecimals();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val intValue = sut.getInt();
                collector.assertThat(intValue).isZero();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size());
    }

    private List<BigDecimal> getBigDecimals() {
        return IntStream.range(0, total)
                .mapToObj(x -> new BigDecimal(random.nextLong()))
                .collect(Collectors.toList());
    }
}
