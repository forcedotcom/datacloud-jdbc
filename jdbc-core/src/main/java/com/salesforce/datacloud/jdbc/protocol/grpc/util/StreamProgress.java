/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc.util;

import lombok.Getter;

/**
 * Discriminated container for streaming progress: value, error, or completion.
 * Exactly one state is set at a time; use the factory methods to construct the desired state.
 *
 * @param <T> element type when representing a successful value
 */
public class StreamProgress<T> {
    @Getter
    private final T value;

    @Getter
    private final Throwable error;

    @Getter
    private final boolean completed;

    private StreamProgress(T value, Throwable error, boolean completed) {
        this.value = value;
        this.error = error;
        this.completed = completed;
    }

    public static <T> StreamProgress<T> success(T value) {
        return new StreamProgress<>(value, null, false);
    }

    public static <T> StreamProgress<T> failure(Throwable error) {
        return new StreamProgress<>(null, error, false);
    }

    public static <T> StreamProgress<T> completed() {
        return new StreamProgress<>(null, null, true);
    }
}
