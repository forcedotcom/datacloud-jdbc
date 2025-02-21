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
