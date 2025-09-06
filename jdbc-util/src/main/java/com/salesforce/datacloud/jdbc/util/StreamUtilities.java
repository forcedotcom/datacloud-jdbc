/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
import lombok.val;

public final class StreamUtilities {
    private StreamUtilities() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static <T> Stream<T> lazyLimitedStream(Supplier<Stream<T>> streamSupplier, LongSupplier limitSupplier) {
        return streamSupplier.get().limit(limitSupplier.getAsLong());
    }

    public static <T> Stream<T> toStream(Iterator<T> iterator) {
        val spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    public static <T, E extends Exception> Optional<T> tryTimes(
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

    public static <T> Stream<T> takeWhile(Stream<T> stream, Predicate<T> predicate) {
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
