/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.*;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Builder
class RowBasedContext {

    @NonNull private final HyperGrpcClientExecutor client;

    @NonNull private final String queryId;

    private final long offset;

    @Getter
    private final long limit;

    @Getter
    private final AtomicLong seen = new AtomicLong(0);

    public Iterator<QueryResult> getQueryResult(boolean omitSchema) throws SQLException {
        val currentOffset = offset + seen.get();
        val currentLimit = limit - seen.get();
        return client.getQueryResult(
                queryId, currentOffset, currentLimit, RowBased.HYPER_MAX_ROW_LIMIT_BYTE_SIZE, omitSchema);
    }
}

@Builder(access = AccessLevel.PRIVATE)
public class RowBased implements Iterator<QueryResult> {
    // The maximum byte size limit for a row based RPC response. While the server enforces a max as well, also having
    // the constant available on the client side allows to set appropriate default values and also to provide immediate
    // feedback on the
    // ``setResultSetConstraints`` method.
    public static final int HYPER_MAX_ROW_LIMIT_BYTE_SIZE = 20971520;
    // The minimal byte size limit for a row based RPC response. The driver enforces this to guard against code that
    // accidentally provides the limit in megabytes.
    public static final int HYPER_MIN_ROW_LIMIT_BYTE_SIZE = 1024;

    public static RowBased of(@NonNull HyperGrpcClientExecutor client, @NonNull String queryId, long offset, long limit)
            throws SQLException {
        val context = RowBasedContext.builder()
                .client(client)
                .queryId(queryId)
                .offset(offset)
                .limit(limit)
                .build();
        return RowBased.builder().context(context).build();
    }

    private final RowBasedContext context;

    private Iterator<QueryResult> iterator;

    @SneakyThrows
    @Override
    public boolean hasNext() {
        if (iterator == null) {
            iterator = context.getQueryResult(false);
            return iterator.hasNext();
        }

        if (iterator.hasNext()) {
            return true;
        }

        if (context.getSeen().get() < context.getLimit()) {
            iterator = context.getQueryResult(true);
        }

        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        val next = iterator.next();
        context.getSeen().addAndGet(next.getResultPartRowCount());
        return next;
    }
}
