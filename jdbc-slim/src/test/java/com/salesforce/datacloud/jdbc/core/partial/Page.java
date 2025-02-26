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
package com.salesforce.datacloud.jdbc.core.partial;

import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.Value;
import lombok.val;

@Value
class Page {
    long offset;
    long limit;

    /**
     * Calculates some number of full pages of some limit with a final page making up the remainder of rows
     * @param rows the total number of rows in the query result
     * @param limit the total number of rows to be acquired in this page
     * @return a stream of pages that can be mapped to
     * {@link com.salesforce.datacloud.jdbc.core.DataCloudConnection#getRowBasedResultSet } calls
     */
    public static Stream<Page> stream(long rows, long limit) {
        long baseSize = Math.min(rows, limit);
        long fullPageCount = rows / baseSize;
        long remainder = rows % baseSize;

        val fullPages = LongStream.range(0, fullPageCount).mapToObj(i -> new Page(i * baseSize, baseSize));
        return Stream.concat(
                fullPages, remainder > 0 ? Stream.of(new Page(fullPageCount * baseSize, remainder)) : Stream.empty());
    }
}
