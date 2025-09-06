/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

class JdbcDriverStubProviderTest {
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ManagedChannelBuilder getMockChannelBuilderWithChannel(ManagedChannel channelMock) {
        val mockChannelBuilder = mock(ManagedChannelBuilder.class);
        when(mockChannelBuilder.maxInboundMessageSize(anyInt())).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.userAgent(anyString())).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.keepAliveTime(anyLong(), any(TimeUnit.class))).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.keepAliveTimeout(anyLong(), any(TimeUnit.class)))
                .thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.idleTimeout(anyLong(), any(TimeUnit.class))).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.keepAliveWithoutCalls(anyBoolean())).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.enableRetry()).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.maxRetryAttempts(anyInt())).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.defaultServiceConfig(anyMap())).thenReturn(mockChannelBuilder);
        when(mockChannelBuilder.build()).thenReturn(channelMock);
        return mockChannelBuilder;
    }

    private ManagedChannelBuilder getMockChannelBuilder() {
        return getMockChannelBuilderWithChannel(mock(ManagedChannel.class));
    }

    @Test
    void shouldSetMaxInboundMessageSizeAndUserAgent() {
        val expectedMaxInboundMessageSize = 64 * 1024 * 1024;
        val expectedUserAgent = DriverVersion.formatDriverInfo();

        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder)) {
            verify(mockChannelBuilder).maxInboundMessageSize(expectedMaxInboundMessageSize);
            verify(mockChannelBuilder).userAgent(expectedUserAgent);
        }
    }

    @Test
    void shouldNotEnableKeepAliveByDefault() {
        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder)) {
            verify(mockChannelBuilder, never()).keepAliveTime(anyLong(), any(TimeUnit.class));
            verify(mockChannelBuilder, never()).keepAliveTimeout(anyLong(), any(TimeUnit.class));
            verify(mockChannelBuilder, never()).idleTimeout(anyLong(), any(TimeUnit.class));
            verify(mockChannelBuilder, never()).keepAliveWithoutCalls(anyBoolean());
        }
    }

    @Test
    void shouldEnableKeepAliveWhenConfiguredWithDefaults() {
        val properties = GrpcChannelProperties.builder().keepAliveEnabled(true).build();
        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder, properties)) {
            verify(mockChannelBuilder).keepAliveTime(60, TimeUnit.SECONDS);
            verify(mockChannelBuilder).keepAliveTimeout(10, TimeUnit.SECONDS);
            verify(mockChannelBuilder).idleTimeout(300, TimeUnit.SECONDS);
            verify(mockChannelBuilder).keepAliveWithoutCalls(false);
        }
    }

    @Test
    void shouldEnableKeepAliveWithCustomValues() {
        // Generate random values for testing
        Random random = ThreadLocalRandom.current();
        val keepAliveTime = random.nextInt(1000) + 1;
        val keepAliveTimeout = random.nextInt(500) + 1;
        val keepAliveWithoutCalls = random.nextBoolean();
        val idleTimeout = random.nextInt(10000) + 1;

        val properties = GrpcChannelProperties.builder()
                .keepAliveEnabled(true)
                .keepAliveTime(keepAliveTime)
                .keepAliveTimeout(keepAliveTimeout)
                .idleTimeoutSeconds(idleTimeout)
                .keepAliveWithoutCalls(keepAliveWithoutCalls)
                .build();
        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder, properties)) {
            verify(mockChannelBuilder).keepAliveTime(keepAliveTime, TimeUnit.SECONDS);
            verify(mockChannelBuilder).keepAliveTimeout(keepAliveTimeout, TimeUnit.SECONDS);
            verify(mockChannelBuilder).idleTimeout(idleTimeout, TimeUnit.SECONDS);
            verify(mockChannelBuilder).keepAliveWithoutCalls(keepAliveWithoutCalls);
        }
    }

    @Test
    void shouldEnableRetriesByDefaultWithDefaults() {
        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder)) {
            verify(mockChannelBuilder).enableRetry();
            verify(mockChannelBuilder).maxRetryAttempts(5);
            verify(mockChannelBuilder).defaultServiceConfig(anyMap());
        }
    }

    @Test
    void shouldEnableRetriesWithCustomValues() {
        Random random = ThreadLocalRandom.current();
        val maxRetryAttempts = random.nextInt(20) + 2;
        val initialBackoff = random.nextDouble() * 5.0;
        val maxBackoff = random.nextInt(200) + 30;
        val backoffMultiplier = random.nextDouble() * 5.0 + 1.0;
        String statusCode1 = Status.UNAVAILABLE.getCode().name();
        String statusCode2 = Status.DEADLINE_EXCEEDED.getCode().name();
        List<String> retryableStatusCodes = Arrays.asList(statusCode1, statusCode2);

        val properties = GrpcChannelProperties.builder()
                .retryEnabled(true)
                .retryMaxAttempts(maxRetryAttempts)
                .retryInitialBackoff(initialBackoff + "s")
                .retryMaxBackoff(maxBackoff + "s")
                .retryBackoffMultiplier(String.valueOf(backoffMultiplier))
                .retryableStatusCodes(retryableStatusCodes)
                .build();
        val mockChannelBuilder = getMockChannelBuilder();
        try (val unused = JdbcDriverStubProvider.of(mockChannelBuilder, properties)) {
            verify(mockChannelBuilder).enableRetry();
            verify(mockChannelBuilder).maxRetryAttempts(maxRetryAttempts);

            verify(mockChannelBuilder).defaultServiceConfig(argThat(config -> {
                String configStr = config.toString();
                return configStr.contains("maxAttempts=" + maxRetryAttempts)
                        && configStr.contains("initialBackoff=" + initialBackoff + "s")
                        && configStr.contains("maxBackoff=" + maxBackoff + "s")
                        && configStr.contains("backoffMultiplier=" + backoffMultiplier)
                        && configStr.contains(statusCode1)
                        && configStr.contains(statusCode2);
            }));
        }
    }

    @Test
    void shouldNotEnableRetriesWhenDisabled() {
        val properties = GrpcChannelProperties.builder().retryEnabled(false).build();
        val mockChannelBuilder = getMockChannelBuilder();
        try (JdbcDriverStubProvider unused = JdbcDriverStubProvider.of(mockChannelBuilder, properties)) {
            verify(mockChannelBuilder, never()).enableRetry();
            verify(mockChannelBuilder, never()).maxRetryAttempts(anyInt());
            verify(mockChannelBuilder, never()).defaultServiceConfig(anyMap());
        }
    }

    @SneakyThrows
    @Test
    void callsManagedChannelCleanup() {
        val mockChannel = mock(ManagedChannel.class);
        val mockChannelBuilder = getMockChannelBuilderWithChannel(mockChannel);
        when(mockChannel.isTerminated()).thenReturn(false, true);

        val stubProvider = JdbcDriverStubProvider.of(mockChannelBuilder);
        stubProvider.close();

        verify(mockChannel).shutdown();
        verify(mockChannel).awaitTermination(5, TimeUnit.SECONDS);
        verify(mockChannel, never()).shutdownNow();
    }

    @SneakyThrows
    @Test
    void callsManagedChannelShutdownNow() {
        val mockChannel = mock(ManagedChannel.class);
        val mockChannelBuilder = getMockChannelBuilderWithChannel(mockChannel);
        when(mockChannel.isTerminated()).thenReturn(false);

        val stubProvider = JdbcDriverStubProvider.of(mockChannelBuilder);
        stubProvider.close();

        verify(mockChannel).shutdown();
        verify(mockChannel).awaitTermination(5, TimeUnit.SECONDS);
        verify(mockChannel).shutdownNow();
    }
}
