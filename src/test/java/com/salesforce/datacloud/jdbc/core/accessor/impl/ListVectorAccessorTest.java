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
import java.util.List;
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

@ExtendWith({SoftAssertionsExtension.class})
public class ListVectorAccessorTest {
    private static final int total = 127;

    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @SneakyThrows
    @Test
    void testGetObjectAndGetObjectClassFromValidListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = extension.createListVector("test-list-vector")) {
            val i = new AtomicInteger(0);
            val sut = new ListVectorAccessor(vector, i::get, consumer);
            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector.assertThat(sut).hasObjectClass(List.class).hasObject(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetArrayFromValidListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = extension.createListVector("test-list-vector")) {
            val i = new AtomicInteger(0);
            val sut = new ListVectorAccessor(vector, i::get, consumer);
            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get()).toArray();
                val actual = (Object[]) sut.getArray().getArray();
                collector.assertThat(actual).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetArrayFromValidLargeListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = extension.createLargeListVector("test-list-vector")) {
            val i = new AtomicInteger(0);
            val sut = new LargeListVectorAccessor(vector, i::get, consumer);
            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get()).toArray();
                val actual = (Object[]) sut.getArray().getArray();
                collector.assertThat(actual).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetArrayFromNulledListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = nulledOutVector(extension.createListVector("test-list-vector"))) {
            val i = new AtomicInteger(0);
            val sut = new ListVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val actual = sut.getArray();
                collector.assertThat(actual).isNull();
            }
        }
        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    @SneakyThrows
    @Test
    void testGetArrayFromNulledLargeListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);
        try (val vector = nulledOutVector(extension.createLargeListVector("test-list-vector"))) {
            val i = new AtomicInteger(0);
            val sut = new LargeListVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val actual = sut.getArray();
                collector.assertThat(actual).isNull();
            }
        }
        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    @SneakyThrows
    @Test
    void testGetObjectAndGetObjectClassFromValidLargeListVector() {
        val values = createListVectors();
        val expectedNullChecks = values.size();
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createLargeListVector("test-large-list-vector")) {
            val i = new AtomicInteger(0);
            val sut = new LargeListVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector.assertThat(sut).hasObjectClass(List.class).hasObject(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    private List<List<Integer>> createListVectors() {
        return IntStream.range(0, total)
                .mapToObj(x -> IntStream.range(0, 5).map(j -> j * x).boxed().collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
}
