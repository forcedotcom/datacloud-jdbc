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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

@Slf4j
class StreamUtilitiesTest {
    @Test
    void testTakeWhileSomeMatch() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7);

        val result = StreamUtilities.takeWhile(stream, x -> x < 5).collect(Collectors.toList());

        assertThat(result).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testTakeWhileAllMatch() {
        val stream = Stream.of(1, 2, 3, 4);

        val result = StreamUtilities.takeWhile(stream, x -> x < 10).collect(Collectors.toList());

        assertThat(result).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testTakeWhileNoMatch() {
        val stream = Stream.of(1, 2, 3);

        val result = StreamUtilities.takeWhile(stream, x -> x < 0).collect(Collectors.toList());

        assertThat(result).isEmpty();
    }

    @Test
    void testTakeWhileEmptyStream() {
        Stream<Integer> stream = Stream.empty();
        Predicate<Integer> predicate = x -> x < 5;

        val result = StreamUtilities.takeWhile(stream, predicate).collect(Collectors.toList());

        assertThat(result).isEmpty();
    }

    @Test
    void testTryTimesSuccessFirstTry() {
        val result = StreamUtilities.tryTimes(3, () -> Stream.of("Success"), this::consumer);

        assertThat(result).isPresent();
        assertThat(result.get().collect(Collectors.toList())).containsExactly("Success");
    }

    @Test
    void testTryTimesSomeFailures() {
        Consumer<Throwable> mockConsumer = mock(Consumer.class);

        val counter = new AtomicInteger(0);

        val result = StreamUtilities.tryTimes(
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
    void testTryTimesNoAttemptsAllowed() {
        val result = StreamUtilities.tryTimes(0, () -> Stream.of("Never runs"), this::consumer);

        assertThat(result).isEmpty();
    }

    @Test
    void testTryTimesAlwaysFails() {
        val result = StreamUtilities.tryTimes(
                3,
                () -> {
                    throw new RuntimeException("Always fails");
                },
                this::consumer);

        assertThat(result).isNotPresent();
    }

    private void consumer(Throwable err) {
        log.error("consumed throwable", err);
    }
}
