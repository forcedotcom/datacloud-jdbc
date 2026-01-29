/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.async.AsyncRowRangeIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.SyncIteratorAdapter;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;

/**
 * Synchronous iterator over a row range from a query result.
 *
 * <p>This extends {@link SyncIteratorAdapter} wrapping {@link AsyncRowRangeIterator} to provide
 * a blocking iterator interface for backward compatibility.</p>
 *
 * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
 * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
 * {@link #hasNext()} and {@link #next()} calls.</p>
 *
 * @see AsyncRowRangeIterator
 */
@Slf4j
public class RowRangeIterator extends SyncIteratorAdapter<QueryResult> {
    // The maximum byte size limit for a row based RPC response. While the server enforces a max as well, also having
    // the constant available on the client side allows to set appropriate default values and also to provide immediate
    // feedback on the
    // ``setResultSetConstraints`` method.
    // TODO: Replace these usages
    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = AsyncRowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE;
    // The minimal byte size limit for a row based RPC response. The driver enforces this to guard against code that
    // accidentally provides the limit in megabytes.
    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = AsyncRowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE;

    /**
     * This iterator will fetch the row range that was requested. It'll internally orchestrate all the required RPC calls.
     * It doesn't check the query status or the number of available rows. The creator of this iterator will need to
     * ensure that the requested range is available. No network calls will be done as part of this method call, only once the
     * iterator is used.
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link #hasNext()} and {@link #next()} calls.</p>
     */
    public static RowRangeIterator of(
            @NonNull QueryAccessGrpcClient queryClient,
            long offset,
            long limit,
            boolean omitSchema,
            @NonNull OutputFormat outputFormat) {
        return new RowRangeIterator(
                AsyncRowRangeIterator.of(queryClient, offset, limit, omitSchema, outputFormat));
    }

    private RowRangeIterator(AsyncRowRangeIterator asyncIterator) {
        super(asyncIterator);
    }
}
