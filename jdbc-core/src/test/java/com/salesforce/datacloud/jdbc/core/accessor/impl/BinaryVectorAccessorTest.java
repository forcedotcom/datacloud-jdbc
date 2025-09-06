/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.nio.charset.StandardCharsets;
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
public class BinaryVectorAccessorTest {
    @RegisterExtension
    public static RootAllocatorTestExtension rootAllocatorTestExtension = new RootAllocatorTestExtension();

    @InjectSoftAssertions
    private SoftAssertions collector;

    private final List<byte[]> binaryList = ImmutableList.of(
            "BINARY_DATA_0001".getBytes(StandardCharsets.UTF_8),
            "BINARY_DATA_0002".getBytes(StandardCharsets.UTF_8),
            "BINARY_DATA_0003".getBytes(StandardCharsets.UTF_8));

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromValidVarBinaryVector() {
        val values = binaryList;
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = rootAllocatorTestExtension.createVarBinaryVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasBytes(expected)
                        .hasObject(expected)
                        .hasString(new String(expected, StandardCharsets.UTF_8));
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromNulledVarBinaryVector() {
        val expectedNullChecks = binaryList.size() * 3; // seen thrice since getObject and getString both call
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(rootAllocatorTestExtension.createVarBinaryVector(binaryList))) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasObject(null)
                        .hasString(null);
                collector.assertThat(sut.getBytes()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromValidLargeVarBinaryVector() {
        val values = binaryList;
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = rootAllocatorTestExtension.createLargeVarBinaryVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasBytes(expected)
                        .hasObject(expected)
                        .hasString(new String(expected, StandardCharsets.UTF_8));
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromNulledLargeVarCharVector() {
        val expectedNullChecks = binaryList.size() * 3; // seen thrice since getObject and getString both call
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(rootAllocatorTestExtension.createLargeVarBinaryVector(binaryList))) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasObject(null)
                        .hasString(null);
                collector.assertThat(sut.getBytes()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromValidFixedSizeVarBinaryVector() {
        val values = binaryList;
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = rootAllocatorTestExtension.createFixedSizeBinaryVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasBytes(expected)
                        .hasObject(expected)
                        .hasString(new String(expected, StandardCharsets.UTF_8));
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetBytesGetStringGetObjectAndGetObjectClassFromNulledFixedSizeVarCharVector() {
        val expectedNullChecks = binaryList.size() * 3; // seen thrice since getObject and getString both call
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(rootAllocatorTestExtension.createFixedSizeBinaryVector(binaryList))) {
            val i = new AtomicInteger(0);
            val sut = new BinaryVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector
                        .assertThat(sut)
                        .hasObjectClass(byte[].class)
                        .hasObject(null)
                        .hasString(null);
                collector.assertThat(sut.getBytes()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }
}
