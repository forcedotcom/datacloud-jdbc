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
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.api.Test;

class StreamUtilitiesTest {
    @Test
    void testTakeWhile_Stream_BasicCase() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7);
        Predicate<Integer> predicate = x -> x < 5;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testTakeWhile_Stream_AllMatch() {
        val stream = Stream.of(1, 2, 3, 4);
        Predicate<Integer> predicate = x -> x < 10;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testTakeWhile_Stream_NoMatch() {
        val stream = Stream.of(1, 2, 3);
        Predicate<Integer> predicate = x -> x < 0;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).isEmpty();
    }

    @Test
    void testTakeWhile_Stream_StopsEarly() {
        val stream = Stream.of(2, 4, 6, 7, 8, 10);
        Predicate<Integer> predicate = x -> x % 2 == 0;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).containsExactly(2, 4, 6);
    }

    @Test
    void testTakeWhile_Stream_EmptyStream() {
        Stream<Integer> stream = Stream.empty();
        Predicate<Integer> predicate = x -> x < 5;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).isEmpty();
    }

    @Test
    void testTryTimes_SuccessFirstTry() {
        Optional<Stream<String>> result = StreamUtilities.tryTimes(3, () -> Stream.of("Success"), System.out::println);

        assertThat(result).isPresent();
        assertThat(result.get().collect(Collectors.toList())).containsExactly("Success");
    }

    @Test
    void testTryTimes_SuccessAfterFewFailures() {
        val counter = new AtomicInteger(0);

        Optional<Stream<String>> result = StreamUtilities.tryTimes(
                3,
                () -> {
                    if (counter.incrementAndGet() < 3) {
                        throw new RuntimeException("Failure " + counter.get());
                    }
                    return Stream.of("Success");
                },
                System.out::println);

        assertThat(result).isPresent();
        assertThat(result.get().collect(Collectors.toList())).containsExactly("Success");
    }

    @Test
    void testTryTimes_AllFailures() {
        Optional<Stream<String>> result = StreamUtilities.tryTimes(
                3,
                () -> {
                    throw new RuntimeException("Always fails");
                },
                System.out::println);

        assertThat(result).isEmpty();
    }

    @Test
    void testTryTimes_ConsumerReceivesExceptions() {
        @SuppressWarnings("unchecked")
        Consumer<Throwable> mockConsumer = mock(Consumer.class);

        AtomicInteger counter = new AtomicInteger(0);

        Optional<Stream<String>> result = StreamUtilities.tryTimes(
                3,
                () -> {
                    if (counter.incrementAndGet() < 3) {
                        throw new RuntimeException("Failure " + counter.get());
                    }
                    return Stream.of("Success");
                },
                mockConsumer);

        verify(mockConsumer, times(2)).accept(any(Throwable.class));

        assertThat(result).isPresent();
        assertThat(result.get().collect(Collectors.toList())).containsExactly("Success");
    }

    @Test
    void testTryTimes_NoAttemptsAllowed() {
        Optional<Stream<String>> result =
                StreamUtilities.tryTimes(0, () -> Stream.of("Never runs"), System.out::println);

        assertThat(result).isEmpty();
    }

    @Test
    void testTryTimes_ReturnsEmptyStream_WhenOptionalEmpty() {
        val result = StreamUtilities.tryTimes(
                3,
                () -> {
                    throw new RuntimeException("Always fails");
                },
                System.out::println);

        assertThat(result).isNotPresent();
    }
}
