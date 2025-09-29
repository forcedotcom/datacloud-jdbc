/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

class DirectDataCloudConnectionTest {

    @Test
    void testGetByteBufferWithEmptyIterator() {
        // Test the specific "No schema data available" exception from DirectDataCloudConnection.getByteBuffer
        String queryId = "test-query-id";
        Iterator<QueryInfo> emptyIterator = Collections.emptyIterator();

        assertThat(org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
                    DirectDataCloudConnection.getByteBuffer(queryId, emptyIterator);
                })
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("No schema data available for queryId: " + queryId));
    }

    @SneakyThrows
    @Test
    void testGetByteBufferWithValidData() {
        // Test successful case with valid ByteString data
        String queryId = "test-query-id";
        ByteString testData = ByteString.copyFromUtf8("test-schema-data");

        // Create a mock QueryInfo with binary schema data
        QueryInfo queryInfo = QueryInfo.newBuilder()
                .setBinarySchema(
                        QueryResultPartBinary.newBuilder().setData(testData).build())
                .build();

        Iterator<QueryInfo> iterator = Collections.singletonList(queryInfo).iterator();

        ByteBuffer result = DirectDataCloudConnection.getByteBuffer(queryId, iterator);

        assertThat(result).isNotNull();
        assertThat(result.remaining()).isEqualTo(testData.size());

        // Convert back to string to verify content
        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);
        String resultString = new String(resultBytes);
        assertThat(resultString).isEqualTo("test-schema-data");
    }

    @Test
    void testGetByteBufferWithNullData() {
        // Test case where QueryInfo has no binary schema
        String queryId = "test-query-id";
        QueryInfo queryInfo = QueryInfo.newBuilder().build(); // No binary schema

        Iterator<QueryInfo> iterator = Collections.singletonList(queryInfo).iterator();

        // This should not throw an exception because ProtocolMappers filters out null values
        // But if it does, it should be handled gracefully
        assertThat(org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
                    DirectDataCloudConnection.getByteBuffer(queryId, iterator);
                })
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("No schema data available for queryId: " + queryId));
    }
}
