/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
public class DoubleVectorAccessorTest {
    @InjectSoftAssertions
    private SoftAssertions collector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    private static final int total = 10;
    private static final Random random = new Random(10);
    private static final List<Double> values = IntStream.range(0, total - 2)
            .mapToDouble(x -> random.nextDouble())
            .filter(Double::isFinite)
            .boxed()
            .collect(Collectors.toList());

    @BeforeAll
    static void setup() {
        values.add(null);
        values.add(null);
        Collections.shuffle(values);
    }

    private TestWasNullConsumer iterate(List<Double> values, BuildThrowingConsumer builder) {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createFloat8Vector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DoubleVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                val s = builder.buildSatisfies(expected);
                collector.assertThat(sut).satisfies(b -> s.accept((DoubleVectorAccessor) b));
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
        ThrowingConsumer<DoubleVectorAccessor> buildSatisfies(Double expected);
    }

    @Test
    void testShouldGetDoubleMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasDouble(expected == null ? 0.0 : expected));
    }

    @Test
    void testShouldGetObjectMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasObject(expected));
    }

    @Test
    void testShouldGetStringMethodFromFloat8Vector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasString(expected == null ? null : Double.toString(expected)));
    }

    @Test
    void testShouldGetBooleanMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasBoolean(expected != null && (expected != 0.0)));
    }

    @Test
    void testShouldGetByteMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasByte((byte) (expected == null ? 0.0 : expected)));
    }

    @Test
    void testShouldGetShortMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasShort((short) (expected == null ? 0.0 : expected)));
    }

    @Test
    void testShouldGetIntMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasInt((int) (expected == null ? 0.0 : expected)));
    }

    @Test
    void testShouldGetLongMethodFromFloat8Vector() {
        iterate(expected -> sut -> collector.assertThat(sut).hasLong((long) (expected == null ? 0.0 : expected)));
    }

    @Test
    void testShouldGetFloatMethodFromFloat8Vector() {
        iterate(expected ->
                sut -> collector.assertThat(sut).hasFloat((float) (expected == null ? 0.0 : expected))); // 0.0f
    }

    @Test
    void testGetBigDecimalIllegalDoublesMethodFromFloat8Vector() {
        val consumer = iterate(
                ImmutableList.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN),
                expected -> sut -> assertThrows(DataCloudJDBCException.class, sut::getBigDecimal));
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(3);
    }

    @Test
    void testShouldGetBigDecimalWithScaleMethodFromFloat4Vector() {
        val scale = 9;
        val big = Double.MAX_VALUE;
        val expected = BigDecimal.valueOf(big).setScale(scale, RoundingMode.HALF_UP);
        iterate(
                ImmutableList.of(Double.MAX_VALUE),
                e -> sut -> collector.assertThat(sut.getBigDecimal(scale)).isEqualTo(expected));
    }
}
