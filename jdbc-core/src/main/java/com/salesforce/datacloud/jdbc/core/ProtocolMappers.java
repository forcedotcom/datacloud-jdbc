/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.protobuf.ByteString;
import java.util.Iterator;
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
        return new Iterator<ByteString>() {
            @Override
            public boolean hasNext() {
                return queryInfos.hasNext();
            }

            @Override
            public ByteString next() {
                QueryInfo info = queryInfos.next();
                return info.hasBinarySchema() ? info.getBinarySchema().getData() : null;
            }
        };
    }

    /**
     * Converts an Iterator<QueryResult> to an Iterator<ByteString> by extracting binary result data.
     */
    public static Iterator<ByteString> fromQueryResult(Iterator<QueryResult> queryResults) {
        return new Iterator<ByteString>() {
            @Override
            public boolean hasNext() {
                return queryResults.hasNext();
            }

            @Override
            public ByteString next() {
                QueryResult result = queryResults.next();
                return result.hasBinaryPart() ? result.getBinaryPart().getData() : null;
            }
        };
    }
}
