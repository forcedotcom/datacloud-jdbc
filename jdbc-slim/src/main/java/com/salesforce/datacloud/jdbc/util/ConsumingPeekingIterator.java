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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConsumingPeekingIterator<T> implements Iterator<T> {
    public static <T> ConsumingPeekingIterator<T> of(Stream<T> stream, Predicate<T> isNotEmpty) {
        return new ConsumingPeekingIterator<>(stream.filter(isNotEmpty).iterator(), isNotEmpty);
    }

    private final Iterator<T> iterator;
    private final Predicate<T> isNotEmpty;
    private final AtomicReference<T> consumable = new AtomicReference<>();

    private boolean consumableHasMore() {
        val head = this.consumable.get();
        return head != null && isNotEmpty.test(this.consumable.get());
    }

    @Override
    public boolean hasNext() {
        return consumableHasMore() || iterator.hasNext();
    }

    @Override
    public T next() {
        if (consumableHasMore()) {
            return this.consumable.get();
        }

        val iteratorHasMore = iterator.hasNext();
        if (iteratorHasMore) {
            this.consumable.set(iterator.next());
            return this.consumable.get();
        }

        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
