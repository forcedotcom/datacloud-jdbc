/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

class ProtocolMappersTest {

    @Test
    void fromQueryInfo_shouldExtractBinarySchema() {
        ByteString data1 = ByteString.copyFromUtf8("schema1");
        ByteString data2 = ByteString.copyFromUtf8("schema2");

        QueryInfo info1 = QueryInfo.newBuilder()
                .setBinarySchema(
                        QueryResultPartBinary.newBuilder().setData(data1).build())
                .build();
        QueryInfo info2 = QueryInfo.newBuilder()
                .setBinarySchema(
                        QueryResultPartBinary.newBuilder().setData(data2).build())
                .build();

        List<QueryInfo> queryInfos = Arrays.asList(info1, info2);

        Iterator<ByteString> result = ProtocolMappers.fromQueryInfo(queryInfos.iterator());

        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isEqualTo(data1);
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isEqualTo(data2);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void fromQueryInfo_shouldHandleMissingBinarySchema() {
        QueryInfo info = QueryInfo.newBuilder().build(); // No binary schema
        List<QueryInfo> queryInfos = Arrays.asList(info);

        Iterator<ByteString> result = ProtocolMappers.fromQueryInfo(queryInfos.iterator());

        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isNull();
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void fromQueryResult_shouldExtractBinaryPart() {
        ByteString data1 = ByteString.copyFromUtf8("result1");
        ByteString data2 = ByteString.copyFromUtf8("result2");

        QueryResult result1 = QueryResult.newBuilder()
                .setBinaryPart(QueryResultPartBinary.newBuilder().setData(data1).build())
                .build();
        QueryResult result2 = QueryResult.newBuilder()
                .setBinaryPart(QueryResultPartBinary.newBuilder().setData(data2).build())
                .build();

        List<QueryResult> queryResults = Arrays.asList(result1, result2);

        Iterator<ByteString> result = ProtocolMappers.fromQueryResult(queryResults.iterator());

        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isEqualTo(data1);
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isEqualTo(data2);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void fromQueryResult_shouldHandleMissingBinaryPart() {
        QueryResult queryResult = QueryResult.newBuilder().build();
        List<QueryResult> queryResults = Arrays.asList(queryResult);

        Iterator<ByteString> result = ProtocolMappers.fromQueryResult(queryResults.iterator());

        assertThat(result.hasNext()).isTrue();
        assertThat(result.next()).isNull();
        assertThat(result.hasNext()).isFalse();
    }
}
