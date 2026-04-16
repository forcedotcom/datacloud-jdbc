/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
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

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expected = values.get(i.get());
                collector
                        .assertThat(sut)
                        .hasBigDecimal(expected)
                        .hasObject(expected)
                        .hasObjectClass(BigDecimal.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetBigDecimalGetObjectAndGetObjectClassFromNulledDecimalVector() {
        val values = getBigDecimals();

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasBigDecimal(null).hasObject(null).hasObjectClass(BigDecimal.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetStringFromDecimalVector() {
        val values = getBigDecimals();

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = values.get(i.get()).toString();
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetStringFromNullDecimalVector() {
        val values = getBigDecimals();

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                collector.assertThat(stringValue).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetIntFromDecimalVector() {
        val values = getBigDecimals();

        try (val vector = extension.createDecimalVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val intValue = sut.getInt();
                val expected = values.get(i.get()).intValue();
                collector.assertThat(intValue).isEqualTo(expected);
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetIntFromNullDecimalVector() {
        val values = getBigDecimals();

        try (val vector = nulledOutVector(extension.createDecimalVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new DecimalVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val intValue = sut.getInt();
                collector.assertThat(intValue).isZero();
            }
        }
    }

    private List<BigDecimal> getBigDecimals() {
        return IntStream.range(0, total)
                .mapToObj(x -> new BigDecimal(random.nextLong()))
                .collect(Collectors.toList());
    }
}
