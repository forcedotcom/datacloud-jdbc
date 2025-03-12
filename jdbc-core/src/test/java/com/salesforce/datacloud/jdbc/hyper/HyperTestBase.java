/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.hyper;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.interceptor.AuthorizationHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.QueryIdHeaderInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;
import salesforce.cdp.hyperdb.v1.QueryResultParam;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HyperTestBase {
    public HyperServerProcess instance;

    @SneakyThrows
    public final void assertEachRowIsTheSame(ResultSet rs, AtomicInteger prev) {
        val expected = prev.incrementAndGet();
        val a = rs.getBigDecimal(1).intValue();
        assertThat(expected).isEqualTo(a);
    }

    @SafeVarargs
    @SneakyThrows
    public final void assertWithConnection(
            ThrowingConsumer<DataCloudConnection> assertion, Map.Entry<String, String>... settings) {
        try (val connection =
                getHyperQueryConnection(settings == null ? ImmutableMap.of() : ImmutableMap.ofEntries(settings))) {
            assertion.accept(connection);
        }
    }

    @SafeVarargs
    @SneakyThrows
    public final void assertWithStatement(
            ThrowingConsumer<DataCloudStatement> assertion, Map.Entry<String, String>... settings) {
        try (val connection = getHyperQueryConnection(
                        settings == null ? ImmutableMap.of() : ImmutableMap.ofEntries(settings));
                val result = connection.createStatement().unwrap(DataCloudStatement.class)) {
            assertion.accept(result);
        }
    }

    public DataCloudConnection getHyperQueryConnection() {
        return getHyperQueryConnection(ImmutableMap.of());
    }

    @SneakyThrows
    public DataCloudConnection getHyperQueryConnection(Map<String, String> connectionSettings) {
        val properties = new Properties();
        properties.putAll(connectionSettings);
        log.info("Creating connection to port {}", instance.getPort());
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress("127.0.0.1", instance.getPort())
                .usePlaintext();

        return DataCloudConnection.fromChannel(channel, properties);
    }

    @SneakyThrows
    @AfterAll
    @Timeout(5_000)
    public void afterAll() {
        instance.close();
    }

    @SneakyThrows
    @BeforeAll
    public void beforeAll() {
        instance = new HyperServerProcess();
    }

    @BeforeEach
    public void assumeHyperEnabled() {
        Assertions.assertTrue((instance != null) && instance.isHealthy(), "Hyper wasn't started, failing test");
    }

    static class NoopTokenSupplier implements AuthorizationHeaderInterceptor.TokenSupplier {
        @Override
        public String getToken() {
            return "";
        }
    }

    @SneakyThrows
    protected DataCloudConnection getInterceptedClientConnection() {
        val mocked = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext();

        val auth = AuthorizationHeaderInterceptor.of(new HyperTestBase.NoopTokenSupplier());
        val channel = ManagedChannelBuilder.forAddress("127.0.0.1", instance.getPort())
                .usePlaintext()
                .intercept(auth)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();

        val stub = HyperServiceGrpc.newBlockingStub(channel);

        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getExecuteQueryMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::executeQuery);
        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getGetQueryInfoMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::getQueryInfo);
        proxyStreamingMethod(
                stub,
                HyperServiceGrpc.getGetQueryResultMethod(),
                HyperServiceGrpc.HyperServiceBlockingStub::getQueryResult);

        return DataCloudConnection.fromTokenSupplier(auth, mocked, new Properties());
    }

    public static <ReqT, RespT> void proxyStreamingMethod(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            MethodDescriptor<ReqT, RespT> mock,
            BiFunction<HyperServiceGrpc.HyperServiceBlockingStub, ReqT, Iterator<RespT>> method) {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(mock).willProxyTo((request, observer) -> {
            final String queryId;
            if (request instanceof salesforce.cdp.hyperdb.v1.QueryInfoParam) {
                queryId = ((QueryInfoParam) request).getQueryId();
            } else if (request instanceof salesforce.cdp.hyperdb.v1.QueryResultParam) {
                queryId = ((QueryResultParam) request).getQueryId();
            } else {
                queryId = null;
            }

            val response = method.apply(
                    queryId == null ? stub : stub.withInterceptors(new QueryIdHeaderInterceptor(queryId)), request);
            while (response.hasNext()) {
                observer.onNext(response.next());
            }
            observer.onCompleted();
        }));
    }
}
