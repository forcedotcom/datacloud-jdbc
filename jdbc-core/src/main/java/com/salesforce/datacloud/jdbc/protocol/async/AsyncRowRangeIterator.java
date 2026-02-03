/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultParam;
import salesforce.cdp.hyperdb.v1.ResultRange;

/**
 * Asynchronous iterator over a row range from a query result.
 *
 * <p>This iterator fetches rows on demand using row-based pagination, providing
 * natural backpressure. It doesn't check the query status or the number of
 * available rows - the creator must ensure the requested range is available.</p>
 *
 * <p>No network calls are made during construction. Calls are only made when
 * {@link #next()} is invoked.</p>
 *
 * <p>This is the async equivalent of
 * {@link com.salesforce.datacloud.jdbc.protocol.RowRangeIterator}.</p>
 */
@Slf4j
public class AsyncRowRangeIterator extends AsyncResultRangeIterator {

    /**
     * The maximum byte size limit for a row based RPC response. While the server enforces a max as well,
     * having the constant available on the client side allows to set appropriate default values.
     */
    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = 20971520;
    /**
     * The minimal byte size limit for a row based RPC response. The driver enforces this to guard against
     * code that accidentally provides the limit in megabytes.
     */
    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = 1024;

    /**
     * Creates an async iterator over the specified row range.
     *
     * @param queryClient  The client for a specific query id
     * @param offset       The starting row offset
     * @param limit        The number of rows to fetch
     * @param omitSchema   Whether to omit schema in responses
     * @param outputFormat The output format for the results
     * @return A new AsyncRowRangeIterator instance
     */
    public static AsyncRowRangeIterator of(
            @NonNull QueryAccessGrpcClient queryClient,
            long offset,
            long limit,
            boolean omitSchema,
            @NonNull OutputFormat outputFormat) {
        return new AsyncRowRangeIterator(queryClient, outputFormat, omitSchema, offset, offset + limit);
    }

    /** The current row offset, updated as results are received. */
    private long currentOffset;

    /** The row offset that marks the end of the range (exclusive). */
    @Getter
    private final long limitRowOffset;

    private AsyncRowRangeIterator(
            QueryAccessGrpcClient client,
            OutputFormat outputFormat,
            boolean omitSchema,
            long offset,
            long limitRowOffset) {
        super(client, outputFormat, omitSchema);
        this.currentOffset = offset;
        this.limitRowOffset = limitRowOffset;
    }

    @Override
    protected boolean hasMoreToFetch() {
        return currentOffset < limitRowOffset;
    }

    @Override
    protected QueryResultParam buildQueryResultParam() {
        ResultRange rowRange = ResultRange.newBuilder()
                .setRowOffset(currentOffset)
                .setRowLimit(limitRowOffset - currentOffset)
                .setByteLimit(HYPER_MAX_ROW_LIMIT_BYTE_SIZE)
                .build();
        return client.getQueryResultParamBuilder()
                .setResultRange(rowRange)
                .setOmitSchema(omitSchema)
                .setOutputFormat(outputFormat)
                .build();
    }

    @Override
    protected String buildLogMessage() {
        return String.format(
                "getQueryResult queryId=%s, currentOffset=%d, remaining=%d, omitSchema=%s",
                client.getQueryId(), currentOffset, limitRowOffset - currentOffset, omitSchema);
    }

    @Override
    protected void onResultReceived(QueryResult result) {
        currentOffset += result.getResultPartRowCount();
    }
}
