/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;

/**
 * Utility class for converting protocol-specific iterators to ByteString iterators.
 * This keeps the protocol-specific logic separate from the channel implementation.
 *
 * <p>Note: FluentIterable is used as the reference interface for iterator/stream operations.</p>
 *
 * @see <a href="DEVELOPMENT.md">DEVELOPMENT.md</a> for more guidelines on FluentIterable
 */
public class ProtocolMappers {

    private ProtocolMappers() {
        // Utility class - prevent instantiation
    }

    /**
     * Converts an Iterator<QueryInfo> to an Iterator<ByteString> by extracting binary schema data.
     */
    public static Iterator<ByteString> fromQueryInfo(Iterator<QueryInfo> queryInfos) {
        return FluentIterable.from(() -> queryInfos)
                .transform(input ->
                        input.hasBinarySchema() ? input.getBinarySchema().getData() : null)
                .filter(Predicates.notNull())
                .iterator();
    }

    /**
     * Converts an Iterator<QueryResult> to an Iterator<ByteString> by extracting binary result data.
     */
    public static Iterator<ByteString> fromQueryResult(Iterator<QueryResult> queryResults) {
        return FluentIterable.from(() -> queryResults)
                .transform(
                        input -> input.hasBinaryPart() ? input.getBinaryPart().getData() : null)
                .filter(Predicates.notNull())
                .iterator();
    }
}
