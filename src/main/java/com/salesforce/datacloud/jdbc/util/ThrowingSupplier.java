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

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;

    static <T, E extends Exception, R> Supplier<Stream<R>> rethrowSupplier(ThrowingSupplier<T, E> function) throws E {
        return () -> {
            try {
                return (Stream<R>) function.get();
            } catch (Exception exception) {
                throwAsUnchecked(exception);
                return null;
            }
        };
    }

    static <T, E extends Exception> LongSupplier rethrowLongSupplier(ThrowingSupplier<T, E> function) throws E {
        return () -> {
            try {
                return (Long) function.get();
            } catch (Exception exception) {
                throwAsUnchecked(exception);
                return Long.parseLong(null);
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
        throw (E) exception;
    }
}
