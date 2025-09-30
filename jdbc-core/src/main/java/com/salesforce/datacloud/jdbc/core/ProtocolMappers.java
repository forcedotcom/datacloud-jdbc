/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;

/**
 * Utility class for converting protocol-specific iterators to ByteString iterators.
 * This keeps the protocol-specific logic separate from the channel implementation.
 */
public class ProtocolMappers {

    private ProtocolMappers() {
        // Utility class - prevent instantiation
    }

    /**
     * Converts an Iterator<QueryInfo> to an Iterator<ByteString> by extracting binary schema data.
     */
    public static Iterator<ByteString> fromQueryInfo(Iterator<QueryInfo> queryInfos) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(queryInfos, 0), false)
                .flatMap(input -> input.hasBinarySchema()
                        ? Stream.of(input.getBinarySchema().getData())
                        : Stream.empty())
                .iterator();
    }

    /**
     * Converts an Iterator<QueryResult> to an Iterator<ByteString> by extracting binary result data.
     */
    public static Iterator<ByteString> fromQueryResult(Iterator<QueryResult> queryResults) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(queryResults, 0), false)
                .flatMap(input ->
                        input.hasBinaryPart() ? Stream.of(input.getBinaryPart().getData()) : Stream.empty())
                .iterator();
    }
}
