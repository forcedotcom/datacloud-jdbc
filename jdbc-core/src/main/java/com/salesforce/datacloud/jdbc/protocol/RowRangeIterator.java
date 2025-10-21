/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValueNonThrowing;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.ResultRange;

/**
 * See {@link RowRangeIterator#of(QueryAccessGrpcClient, long, long, boolean, OutputFormat)}
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RowRangeIterator implements Iterator<QueryResult> {
    // The maximum byte size limit for a row based RPC response. While the server enforces a max as well, also having
    // the constant available on the client side allows to set appropriate default values and also to provide immediate
    // feedback on the
    // ``setResultSetConstraints`` method.
    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = 20971520;
    // The minimal byte size limit for a row based RPC response. The driver enforces this to guard against code that
    // accidentally provides the limit in megabytes.
    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = 1024;

    /**
     * This iterator will fetch the row range that was requested. It'll internally orchestrate all the required RPC calls.
     * It doesn't check the query status or the number of available rows. The creator of this iterator will need to
     * ensure that the requested range is available. No network calls will be done as part of this method call, only once the
     * iterator is used.
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link RowRangeIterator#hasNext()} and {@link RowRangeIterator#next()} calls.</p>
     */
    public static RowRangeIterator of(
            @NonNull QueryAccessGrpcClient queryClient,
            long offset,
            long limit,
            boolean omitSchema,
            @NonNull OutputFormat outputFormat) {
        return new RowRangeIterator(queryClient, offset, offset + limit, outputFormat, false, null);
    }

    private final QueryAccessGrpcClient client;

    private long currentOffset;
    /**
     * The offset of the row that shouldn't be included in the result anymore
     */
    @Getter
    private final long limitRowOffset;

    private final OutputFormat outputFormat;
    private boolean omitSchema;
    private Iterator<QueryResult> iterator;

    @Override
    public boolean hasNext() {
        // No op case where we have more data messages available
        if ((iterator != null) && (iterator.hasNext())) {
            return true;
        }
        // We are finished
        if (currentOffset == limitRowOffset) {
            return false;
        }

        // We need to fetch more rows
        val rowRange = ResultRange.newBuilder()
                .setRowOffset(currentOffset)
                .setRowLimit(limitRowOffset - currentOffset)
                .setByteLimit(RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
        val param = client.getQueryResultParamBuilder()
                .setResultRange(rowRange)
                .setOmitSchema(omitSchema)
                .setOutputFormat(outputFormat)
                .build();

        val message = String.format(
                "getQueryResult queryId=%s, currentOffset=%d, remaining=%d, omitSchema=%s",
                client.getQueryId(), this.currentOffset, limitRowOffset - currentOffset, omitSchema);
        iterator = logTimedValueNonThrowing(() -> client.getStub().getQueryResult(param), message, log);

        // We only need to fetch the schema for the first result
        if (!omitSchema) {
            omitSchema = true;
        }

        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        val next = iterator.next();
        currentOffset += next.getResultPartRowCount();
        return next;
    }
}
