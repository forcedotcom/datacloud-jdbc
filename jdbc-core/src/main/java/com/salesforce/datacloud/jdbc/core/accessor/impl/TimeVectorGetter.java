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
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.holders.NullableTimeMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeSecHolder;

public final class TimeVectorGetter {

    private TimeVectorGetter() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static class Holder {
        int isSet;
        long value;
    }

    @FunctionalInterface
    static interface Getter {
        void get(int index, Holder holder);
    }

    static Getter createGetter(TimeNanoVector vector) {
        NullableTimeNanoHolder auxHolder = new NullableTimeNanoHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }

    static Getter createGetter(TimeMicroVector vector) {
        NullableTimeMicroHolder auxHolder = new NullableTimeMicroHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }

    static Getter createGetter(TimeMilliVector vector) {
        NullableTimeMilliHolder auxHolder = new NullableTimeMilliHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }

    static Getter createGetter(TimeSecVector vector) {
        NullableTimeSecHolder auxHolder = new NullableTimeSecHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }
}
