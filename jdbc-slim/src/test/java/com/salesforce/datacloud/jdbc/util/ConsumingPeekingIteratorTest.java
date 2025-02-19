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

import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConsumingPeekingIteratorTest {
    private final List<Integer> expected = IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());

    private List<Deque<Integer>> getData() {
        return Stream.of(
                        ImmutableList.of(1, 2, 3),
                        ImmutableList.of(4, 5, 6, 7, 8, 9),
                        new ArrayList<Integer>(),
                        new ArrayList<Integer>(),
                        new ArrayList<Integer>(),
                        new ArrayList<Integer>(),
                        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                        ImmutableList.of(20),
                        new ArrayList<Integer>())
                .map(ArrayDeque::new)
                .collect(Collectors.toList());
    }

    ConsumingPeekingIterator<Deque<Integer>> getSut() {
        return ConsumingPeekingIterator.of(getData().stream(), t -> !t.isEmpty());
    }

    @Test
    void returnsSameValueUntilCallerConsumesItem() {
        val sut = getSut();
        assertThat(sut.next()).isSameAs(sut.next());
    }

    @Test
    void consumesAllData() {
        val sut = getSut();
        val actual = new ArrayList<Integer>();
        while (sut.hasNext()) {
            val current = sut.next();
            actual.add(current.removeFirst());
        }

        assertThat(actual).hasSameElementsAs(expected);
    }

    @Test
    void throwsWhenExhausted() {
        val sut = getSut();
        while (sut.hasNext()) {
            sut.next().removeFirst();
        }

        Assertions.assertThrows(NoSuchElementException.class, sut::next);
    }

    @Test
    void removeIsNotSupported() {
        val sut = ConsumingPeekingIterator.of(Stream.empty(), t -> true);
        Assertions.assertThrows(UnsupportedOperationException.class, sut::remove);
    }
}
