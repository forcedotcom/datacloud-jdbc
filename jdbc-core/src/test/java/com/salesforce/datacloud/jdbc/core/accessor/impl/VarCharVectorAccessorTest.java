/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
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
public class VarCharVectorAccessorTest {
    private static final int total = 8;

    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @SneakyThrows
    @Test
    void testGetStringGetObjectAndGetObjectClassFromValidVarCharVector() {
        val values = getStrings();
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createVarCharVectorFrom(values)) {
            val i = new AtomicInteger(0);
            val sut = new VarCharVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasObjectClass(String.class)
                        .hasBytes(expected.getBytes(StandardCharsets.UTF_8))
                        .hasObject(expected)
                        .hasString(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetStringGetObjectAndGetObjectClassFromNulledVarCharVector() {
        val values = getStrings();
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createVarCharVectorFrom(values))) {
            val i = new AtomicInteger(0);
            val sut = new VarCharVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector
                        .assertThat(sut)
                        .hasObjectClass(String.class)
                        .hasObject(null)
                        .hasString(null);
                collector.assertThat(sut.getBytes()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    @SneakyThrows
    @Test
    void testGetStringGetObjectAndGetObjectClassFromValidLargeVarCharVector() {
        val values = getStrings();
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createLargeVarCharVectorFrom(values)) {
            val i = new AtomicInteger(0);
            val sut = new VarCharVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasObjectClass(String.class)
                        .hasBytes(expected.getBytes(StandardCharsets.UTF_8))
                        .hasObject(expected)
                        .hasString(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNullChecks).hasNullSeen(0);
    }

    @SneakyThrows
    @Test
    void testGetStringGetObjectAndGetObjectClassFromNulledLargeVarCharVector() {
        val values = getStrings();
        val expectedNullChecks = values.size() * 3; // seen thrice since getObject and getString both call getBytes
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createLargeVarCharVectorFrom(values))) {
            val i = new AtomicInteger(0);
            val sut = new VarCharVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector
                        .assertThat(sut)
                        .hasObjectClass(String.class)
                        .hasObject(null)
                        .hasString(null);
                collector.assertThat(sut.getBytes()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(expectedNullChecks);
    }

    private List<String> getStrings() {
        return IntStream.range(0, total)
                .mapToObj(x -> UUID.randomUUID().toString())
                .collect(Collectors.toList());
    }
}
