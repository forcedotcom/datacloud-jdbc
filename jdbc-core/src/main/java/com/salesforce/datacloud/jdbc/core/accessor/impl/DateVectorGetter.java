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

import lombok.experimental.UtilityClass;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;

@UtilityClass
final class DateVectorGetter {

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
