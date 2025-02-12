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
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.experimental.UtilityClass;
import lombok.val;

@UtilityClass
public class StreamUtilities {
    public <T> Stream<T> lazyLimitedStream(Supplier<Stream<T>> streamSupplier, LongSupplier limitSupplier) {
        return streamSupplier.get().limit(limitSupplier.getAsLong());
    }

    public <T> Stream<T> toStream(Iterator<T> iterator) {
        val spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    public <T, E extends Exception> Optional<T> tryTimes(
            int times, ThrowingSupplier<T, E> attempt, Consumer<Throwable> consumer) {
        return Stream.iterate(attempt, UnaryOperator.identity())
                .limit(times)
                .map(Result::of)
                .filter(r -> {
                    if (r.getError().isPresent()) {
                        consumer.accept(r.getError().get());
                        return false;
                    }
                    return true;
                })
                .findFirst()
                .flatMap(Result::get);
    }

    public <T> Stream<T> takeWhile(Stream<T> stream, Predicate<T> predicate) {
        val split = stream.spliterator();

        return StreamSupport.stream(
                new Spliterators.AbstractSpliterator<T>(split.estimateSize(), split.characteristics()) {
                    boolean shouldContinue = true;

                    @Override
                    public boolean tryAdvance(Consumer<? super T> action) {
                        return shouldContinue
                                && split.tryAdvance(elem -> {
                                    if (predicate.test(elem)) {
                                        action.accept(elem);
                                    } else {
                                        shouldContinue = false;
                                    }
                                });
                    }
                },
                false);
    }
}
