/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
