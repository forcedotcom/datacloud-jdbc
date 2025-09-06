/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
