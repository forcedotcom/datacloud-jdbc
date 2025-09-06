/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
