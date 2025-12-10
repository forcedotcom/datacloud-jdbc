/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.*;

/**
 * This class facilitates protocol access to a specific query id. It bundles the query id together with a stub that is
 * specifically initialized to send the headers for this query id. It also allows to apply a consistent deadline across
 * all calls done with this client by configuring the stub with a deadline. Users can use this class to get a preinitialized
 * cached stub and also to get parameter builders that are preinitialized to the right query id.
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryAccessGrpcClient {
    /**
     * This stub has an interceptor that always contains the query id.
     * The stub is cached as a best practice in gRPC see https://grpc.io/docs/guides/performance/
     */
    @Getter
    private HyperServiceGrpc.HyperServiceStub stub;
    /**
     * The query id that is getting accessed
     */
    @Getter
    private String queryId;

    // The header name for the query id header
    private static final Metadata.Key<String> queryIdHeader =
            Metadata.Key.of("x-hyperdb-query-id", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Initialize the client for the specific query id that was passed in using the passed in stub as a base.
     * @param queryId the query id that should be accessed
     * @param stub the stub that should be used as base
     * @return a new instance of the access client
     */
    public static QueryAccessGrpcClient of(String queryId, HyperServiceGrpc.HyperServiceStub stub) {
        Metadata injectedHeader = new Metadata();
        injectedHeader.put(queryIdHeader, queryId);

        return new QueryAccessGrpcClient(
                stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(injectedHeader)), queryId);
    }

    /**
     * Get the query cancellation param builder with the query id preinitialized
     * @return the builder
     */
    public CancelQueryParam.Builder getCancelQueryParamBuilder() {
        return CancelQueryParam.newBuilder().setQueryId(queryId);
    }

    /**
     * Get QueryInfoParam builder with query id preinitialized
     * @return the builder
     */
    public QueryInfoParam.Builder getQueryInfoParamBuilder() {
        return QueryInfoParam.newBuilder().setQueryId(queryId);
    }

    /**
     * Get QueryResultParam builder with the query id preinitialized
     * @return the builder
     */
    public QueryResultParam.Builder getQueryResultParamBuilder() {
        return QueryResultParam.newBuilder().setQueryId(queryId);
    }

    /**
     * Creates a new QueryAccessGrpcClient with a configured stub. The configurator function receives
     * the current stub and should return a modified stub with additional configuration applied.
     *
     * @param configurator A function that takes the current stub and returns a configured stub
     * @return A new QueryAccessGrpcClient with the configured stub
     */
    public QueryAccessGrpcClient withStubConfiguration(UnaryOperator<HyperServiceGrpc.HyperServiceStub> configurator) {
        return QueryAccessGrpcClient.of(queryId, configurator.apply(stub));
    }
}
