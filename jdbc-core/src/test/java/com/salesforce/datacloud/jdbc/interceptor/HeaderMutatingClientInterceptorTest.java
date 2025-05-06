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

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.Metadata;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(HyperTestBase.class)
class HeaderMutatingClientInterceptorTest {
    @Test
    @SneakyThrows
    void interceptCallAlwaysCallsMutate() {
        Consumer<Metadata> mockConsumer = mock(Consumer.class);
        val sut = new Sut(mockConsumer);

        try (val conn = getHyperQueryConnection(sut);
                val stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT 1");
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

        assertThatThrownBy(() -> {
                    try (val conn = getHyperQueryConnection(sut);
                            val stmt = conn.createStatement()) {
                        stmt.executeQuery("SELECT 1");
                    }
                })
                .isInstanceOf(DataCloudJDBCException.class)
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
