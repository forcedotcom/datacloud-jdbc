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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.hyperdb.grpc.QueryParam;
import io.grpc.Metadata;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HeaderMutatingClientInterceptorTest extends HyperGrpcTestBase {
    String query = UUID.randomUUID().toString();
    String queryId = UUID.randomUUID().toString();

    @Test
    @SneakyThrows
    void interceptCallAlwaysCallsMutate() {
        Consumer<Metadata> mockConsumer = mock(Consumer.class);
        val sut = new Sut(mockConsumer);

        try (val client =
                hyperGrpcClient.toBuilder().interceptors(ImmutableList.of(sut)).build()) {
            setupExecuteQuery(queryId, query, QueryParam.TransferMode.SYNC);
            client.executeQuery(query);
        }

        val argumentCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockConsumer).accept(argumentCaptor.capture());
    }

    @Test
    void interceptCallCatchesMutateAndWrapsException() {
        val message = UUID.randomUUID().toString();
        Consumer<Metadata> mockConsumer = mock(Consumer.class);

        doAnswer(invocation -> {
                    throw new RuntimeException(message);
                })
                .when(mockConsumer)
                .accept(any());

        val sut = new Sut(mockConsumer);

        val ex = Assertions.assertThrows(DataCloudJDBCException.class, () -> {
            try (val client = hyperGrpcClient.toBuilder()
                    .interceptors(ImmutableList.of(sut))
                    .build()) {
                setupExecuteQuery(queryId, query, QueryParam.TransferMode.SYNC);
                client.executeQuery(query);
            }
        });

        AssertionsForClassTypes.assertThat(ex)
                .hasRootCauseMessage(message)
                .hasMessage("Caught exception when mutating headers in client interceptor");

        val argumentCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockConsumer).accept(argumentCaptor.capture());
    }

    @AllArgsConstructor
    static class Sut implements HeaderMutatingClientInterceptor {
        private final Consumer<Metadata> headersConsumer;

        @Override
        public void mutate(Metadata headers) {
            headersConsumer.accept(headers);
        }
    }
}
