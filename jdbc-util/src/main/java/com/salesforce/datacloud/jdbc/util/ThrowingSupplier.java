/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
