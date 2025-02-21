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
package com.salesforce.datacloud.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

public class HyperGrpcClientRetryTest {
    private HyperServiceGrpc.HyperServiceBlockingStub hyperServiceStub;
    private HyperServiceImpl hyperService;
    private ManagedChannel channel;

    @BeforeEach
    public void setUpClient() throws IOException {
        String serverName = InProcessServerBuilder.generateName();
        InProcessServerBuilder serverBuilder =
                InProcessServerBuilder.forName(serverName).directExecutor();

        hyperService = new HyperServiceImpl();
        serverBuilder.addService(hyperService);
        serverBuilder.build().start();

        channel = InProcessChannelBuilder.forName(serverName)
                .usePlaintext()
                .enableRetry()
                .maxRetryAttempts(5)
                .directExecutor()
                .defaultServiceConfig(retryPolicy())
                .build();

        hyperServiceStub = HyperServiceGrpc.newBlockingStub(channel);
    }

    @SneakyThrows
    @AfterEach
    public void cleanupClient() {
        if (channel != null) {
            channel.shutdown();

            try {
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                channel.shutdownNow();
            }
        }
    }

    private Map<String, Object> retryPolicy() {
        return ImmutableMap.of(
                "methodConfig",
                ImmutableList.of(ImmutableMap.of(
                        "name",
                        ImmutableList.of(Collections.EMPTY_MAP),
                        "retryPolicy",
                        ImmutableMap.of(
                                "maxAttempts",
                                String.valueOf(5),
                                "initialBackoff",
                                "0.5s",
                                "maxBackoff",
                                "30s",
                                "backoffMultiplier",
                                2.0,
                                "retryableStatusCodes",
                                ImmutableList.of("UNAVAILABLE")))));
    }

    private final String query = "SELECT * FROM test";
    private static final ExecuteQueryResponse chunk1 = ExecuteQueryResponse.newBuilder()
            .setBinaryPart(QueryResultPartBinary.newBuilder()
                    .setData(ByteString.copyFromUtf8("test 1"))
                    .build())
            .build();

    @Test
    public void testExecuteQueryWithRetry() {
        Iterator<ExecuteQueryResponse> queryResultIterator = hyperServiceStub.executeQuery(
                QueryParam.newBuilder().setQuery(query).build());

        assertDoesNotThrow(() -> {
            boolean responseReceived = false;
            while (queryResultIterator.hasNext()) {
                ExecuteQueryResponse response = queryResultIterator.next();
                if (response.getBinaryPart().getData().toStringUtf8().equals("test 1")) {
                    responseReceived = true;
                }
            }
            assertTrue(responseReceived, "Expected response not received after retries.");
        });

        Assertions.assertThat(hyperService.getRetryCount()).isEqualTo(5);
    }

    @Getter
    @Slf4j
    public static class HyperServiceImpl extends HyperServiceGrpc.HyperServiceImplBase {
        int retryCount = 1;

        @Override
        public void executeQuery(QueryParam request, StreamObserver<ExecuteQueryResponse> responseObserver) {
            log.warn("Executing query attempt #{}", retryCount);
            if (retryCount < 5) {
                retryCount++;
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Service unavailable")
                        .asRuntimeException());
                return;
            }

            responseObserver.onNext(chunk1);
            responseObserver.onCompleted();
        }
    }
}
