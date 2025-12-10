/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.apache.arrow.vector.types.pojo.Schema.deserializeMessage;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.jdbc.protocol.grpc.util.BufferingStreamIterator;
import io.grpc.Status;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.types.pojo.Schema;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

@Slf4j
public class QuerySchemaAccessor {
    /**
     * Provides the Arrow schema for a specific query id.
     *
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions</p>
     *
     * @param queryClient The client for a specific query id
     * @return A new QueryInfoIterator instance
     */
    public static Schema getArrowSchema(@NonNull QueryAccessGrpcClient queryClient) {
        val request = queryClient
                .getQueryInfoParamBuilder()
                .setSchemaOutputFormat(QueryResultArrowStream.OUTPUT_FORMAT)
                .build();
        val message = "getQuerySchema queryId=" + queryClient.getQueryId();
        val iterator = new BufferingStreamIterator<QueryInfoParam, QueryInfo>(message, log);
        queryClient.getStub().getQueryInfo(request, iterator.getObserver());

        while (true) {
            // We always expect a schema message as we requested one
            if (!iterator.hasNext()) {
                throw Status.fromCode(Status.Code.INTERNAL)
                        .withDescription("No schema data available. queryId=" + queryClient.getQueryId())
                        .asRuntimeException();
            }
            val info = iterator.next();
            // Identify message with binary schema
            if (info.hasBinarySchema()) {
                iterator.close();
                return deserializeMessage(info.getBinarySchema().getData().asReadOnlyByteBuffer());
            }
        }
    }
}
