/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

public abstract class Result<T> {
    private Result() {}

    public static <T, E extends Exception> Result<T> of(@NonNull ThrowingSupplier<T, E> supplier) {
        try {
            return new Success<>(supplier.get());
        } catch (Throwable t) {
            return new Failure<>(t);
        }
    }

    abstract Optional<T> get();

    abstract Optional<Throwable> getError();

    @Getter
    @AllArgsConstructor
    public static class Success<T> extends Result<T> {
        private final T value;

        @Override
        Optional<T> get() {
            return Optional.ofNullable(value);
        }

        @Override
        Optional<Throwable> getError() {
            return Optional.empty();
        }
    }

    @Getter
    @AllArgsConstructor
    public static class Failure<T> extends Result<T> {
        private final Throwable error;

        @Override
        Optional<T> get() {
            return Optional.empty();
        }

        @Override
        Optional<Throwable> getError() {
            return Optional.ofNullable(error);
        }
    }
}
