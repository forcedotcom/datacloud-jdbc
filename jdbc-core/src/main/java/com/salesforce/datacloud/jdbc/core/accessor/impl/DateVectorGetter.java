/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;

final class DateVectorGetter {

    private DateVectorGetter() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static class Holder {
        int isSet;
        long value;
    }

    @FunctionalInterface
    interface Getter {
        void get(int index, Holder holder);
    }

    static Getter createGetter(DateDayVector vector) {
        NullableDateDayHolder auxHolder = new NullableDateDayHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }

    static Getter createGetter(DateMilliVector vector) {
        NullableDateMilliHolder auxHolder = new NullableDateMilliHolder();
        return (index, holder) -> {
            vector.get(index, auxHolder);
            holder.isSet = auxHolder.isSet;
            holder.value = auxHolder.value;
        };
    }
}
