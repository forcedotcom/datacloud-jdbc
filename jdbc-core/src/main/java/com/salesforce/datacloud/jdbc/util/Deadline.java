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
    private static final Duration INFINITE = Duration.ofDays(10);

    // The deadline in nanoseconds.
    private final long deadline;

    /**
     * Initialize a deadline with the given timeout.
     * @param timeout The timeout to enforce. A duration of zero means an infinite deadline and no timeout.
     * @return The deadline.
     */
    public static Deadline of(Duration timeout) {
        if (timeout.isZero()) {
            timeout = INFINITE;
        }
        return Deadline.builder().deadline(currentTime() + timeout.toNanos()).build();
    }

    /**
     * Due to limitations with netty we cannot use Long.MAX_VALUE to represent an infinite timeout,
     * therefore we specify a 10-day duration as a practical infinite timeout.
     * @return An "infinite" deadline.
     */
    public static Deadline infinite() {
        return of(Duration.ZERO);
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
}
