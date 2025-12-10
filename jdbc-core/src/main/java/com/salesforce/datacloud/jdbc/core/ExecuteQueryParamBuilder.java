/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.protocol.QueryResultArrowStream;
import com.salesforce.datacloud.jdbc.util.Unstable;
import java.util.Map;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.ResultRange;

/**
 * Although this class is public, we do not consider it to be part of our API.
 * It is for internal use only until it stabilizes.
 */
@Builder(access = AccessLevel.PRIVATE)
@Slf4j
@Unstable
public class ExecuteQueryParamBuilder {
    private final QueryParam settingsQueryParams;

    private QueryParam additionalQueryParams;

    public static ExecuteQueryParamBuilder of(Map<String, String> querySettings) {
        val builder = ExecuteQueryParamBuilder.builder();
        if (!querySettings.isEmpty()) {
            builder.settingsQueryParams(
                    QueryParam.newBuilder().putAllSettings(querySettings).build());
        }
        return builder.build();
    }

    public ExecuteQueryParamBuilder withQueryParams(QueryParam additionalQueryParams) {
        this.additionalQueryParams = additionalQueryParams;
        return this;
    }

    private QueryParam completeBuilder(QueryParam.Builder builder) {
        if (additionalQueryParams != null) {
            builder.mergeFrom(additionalQueryParams);
        }
        if (settingsQueryParams != null) {
            builder.mergeFrom(settingsQueryParams);
        }
        return builder.build();
    }

    public QueryParam getQueryParams(String sql, QueryParam.TransferMode transferMode) {
        return completeBuilder(QueryParam.newBuilder()
                .setQuery(sql)
                .setOutputFormat(QueryResultArrowStream.OUTPUT_FORMAT)
                .setTransferMode(transferMode));
    }

    public QueryParam getAdaptiveQueryParams(String sql) {
        return getQueryParams(sql, QueryParam.TransferMode.ADAPTIVE);
    }

    public QueryParam getAdaptiveRowLimitQueryParams(String sql, long maxRows, long maxBytes) {
        val builder = QueryParam.newBuilder()
                .setQuery(sql)
                .setOutputFormat(QueryResultArrowStream.OUTPUT_FORMAT)
                .setTransferMode(QueryParam.TransferMode.ADAPTIVE);
        val range = ResultRange.newBuilder().setRowLimit(maxRows).setByteLimit(maxBytes);
        builder.setResultRange(range);
        log.info("setting row limit query. maxRows={}, maxBytes={}", maxRows, maxBytes);
        return completeBuilder(builder);
    }
}
