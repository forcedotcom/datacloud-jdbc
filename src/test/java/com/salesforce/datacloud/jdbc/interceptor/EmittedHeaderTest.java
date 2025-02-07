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
package com.salesforce.datacloud.jdbc.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.internal.Tracer;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

public class EmittedHeaderTest {
    private static final String WORKLOAD = "x-hyperdb-workload";
    private static final String CONTEXT = "x-hyperdb-external-client-context";

    private static String trim(String str) {
        return str.replace("x-hyperdb-", "");
    }

    @Test
    void traceAndSpanIdsAreSet() {
        val tracer = Tracer.get();
        val actual = getHeadersFor(new Properties());
        assertThat(actual)
                .hasEntrySatisfying("x-b3-traceid", e -> assertThat(tracer.isValidTraceId(e))
                        .isTrue())
                .hasEntrySatisfying(
                        "x-b3-spanid", e -> assertThat(tracer.isValidSpanId(e)).isTrue());
    }

    @Test
    void userAgentHeaderIncludesVersionDetails() {
        val actual = getHeadersFor(new Properties());

        assertThat(actual)
                .hasEntrySatisfying("user-agent", e -> assertThat(e).contains(DriverVersion.formatDriverInfo()));
    }

    private static Stream<Arguments> cases() {
        val workload = UUID.randomUUID().toString();
        val context = UUID.randomUUID().toString();
        val v = DriverVersion.formatDriverInfo();

        return Stream.of(
                argumentSet("workload has a sensible default", WORKLOAD, null, null, "jdbcv3"),
                argumentSet("workload can be overridden", WORKLOAD, trim(WORKLOAD), workload, workload),
                argumentSet("client context is ignored if not provided", CONTEXT, null, null, null),
                argumentSet("client context is set if provided", CONTEXT, trim(CONTEXT), context, context));
    }

    @ParameterizedTest
    @MethodSource("cases")
    void headerIsExpected(String header, String key, String value, String expected) {
        val properties = new Properties();
        if (value != null) {
            properties.put(key, value);
        }

        val actual = getHeadersFor(properties);
        if (expected == null) {
            assertThat(actual).doesNotContainKey(header);
        } else {
            assertThat(actual).containsEntry(header, expected);
        }
    }

    @SneakyThrows
    private static Map<String, String> getHeadersFor(Properties properties) {
        val interceptor = new HeaderCapturingInterceptor();

        val name = InProcessServerBuilder.generateName();
        InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new HeaderCapturingService())
                .intercept(interceptor)
                .build()
                .start();
        val channel = InProcessChannelBuilder.forName(name).usePlaintext();

        try (val connection = DataCloudConnection.fromChannel(channel, properties);
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeAsyncQuery("select 1");
        }

        return interceptor.get();
    }
}

class HeaderCapturingService extends HyperServiceGrpc.HyperServiceImplBase {
    private static final ExecuteQueryResponse chunk1 = ExecuteQueryResponse.newBuilder()
            .setBinaryPart(QueryResultPartBinary.newBuilder()
                    .setData(ByteString.copyFromUtf8("test 1"))
                    .build())
            .build();

    @Override
    public void executeQuery(QueryParam request, StreamObserver<ExecuteQueryResponse> responseObserver) {
        responseObserver.onNext(chunk1);
        responseObserver.onCompleted();
    }
}

class HeaderCapturingInterceptor implements ServerInterceptor {
    private final AtomicReference<Map<String, String>> ref = new AtomicReference<>(ImmutableMap.of());

    public Map<String, String> get() {
        return ref.get();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        val mapped =
                headers.keys().stream().collect(Collectors.toMap(UnaryOperator.identity(), k -> getValue(k, headers)));
        ref.set(mapped);

        return next.startCall(call, headers);
    }

    private static String getValue(String name, Metadata headers) {
        val key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
        return headers.get(key);
    }
}
