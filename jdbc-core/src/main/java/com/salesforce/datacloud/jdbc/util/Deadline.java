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

import java.time.Duration;
import lombok.AccessLevel;
import lombok.Builder;

/**
 * Utility class that handles logic around enforcing timeouts on API calls. It provides simple access
 * to the remaining time based off an initial timeout. This allows to easily enforce a timeout
 * across multiple network calls.
 */
@Builder(access = AccessLevel.PRIVATE)
public class Deadline {
    // The deadline in nanoseconds.
    private final long deadline;

    /**
     * Creates a practically infinite deadline for operations that should not timeout.
     * Returns a deadline set to 10 days from the current time.
     * @return A deadline that effectively never expires for practical purposes.
     */
    public static Deadline infinite() {
        return of(Duration.ZERO);
    }

    /**
     * Initialize a deadline with the given timeout.
     * @param timeout The timeout to enforce. A duration of zero means an infinite deadline and no timeout.
     * @return The deadline.
     */
    public static Deadline of(Duration timeout) {
        // Handle infinite / no timeout case
        if (timeout.isZero()) {
            // We can't use Long.MAX_VALUE here as it results in a remaining time that is too large for netty.
            // Thus for practical pruposes we say that an infitine deadline is 10 days from now.
            timeout = Duration.ofDays(10);
        }
        return Deadline.builder().deadline(currentTime() + timeout.toNanos()).build();
    }

    /**
     * Returns the remaining time until the deadline is reached.
     * @return The remaining time until the deadline is reached.
     */
    public Duration getRemaining() {
        long remaining = deadline - currentTime();
        return Duration.ofNanos(remaining);
    }

    /**
     * Returns true if the deadline has passed.
     * @return True if the deadline has passed, false otherwise.
     */
    public boolean hasPassed() {
        return currentTime() >= deadline;
    }

    /**
     * We are using nano time here because it provides a monotonic clock that never goes backwards or
     * jumps due to system clock adjustments / leap seconds / ...
     * @return The current time in nanoseconds.
     */
    private static long currentTime() {
        return System.nanoTime();
    }

    @Override
    public String toString() {
        return String.format("deadline=%d, remaining=%s", deadline, getRemaining());
    }
}
