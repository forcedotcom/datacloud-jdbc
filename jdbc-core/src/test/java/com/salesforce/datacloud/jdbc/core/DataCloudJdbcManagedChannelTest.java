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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DataCloudJdbcManagedChannelTest {

    private DataCloudJdbcManagedChannel channel;
    private ManagedChannelBuilder<?> channelBuilder;
    private Properties properties;
    private final Random random = ThreadLocalRandom.current();

    @BeforeEach
    void setUp() {
        Mockito.reset();
        channelBuilder = spy(InProcessChannelBuilder.forName("test-channel"));
        properties = new Properties();
    }

    @AfterEach
    void cleanUp() {
        if (channel != null) {
            channel.close();
        }
    }

    @Test
    void shouldSetMaxInboundMessageSizeAndUserAgent() {
        val expectedMaxInboundMessageSize = 64 * 1024 * 1024;
        val expectedUserAgent = DriverVersion.formatDriverInfo();

        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder).maxInboundMessageSize(expectedMaxInboundMessageSize);
        verify(channelBuilder).userAgent(expectedUserAgent);
    }

    @Test
    void shouldNotEnableKeepAliveByDefault() {
        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder, never()).keepAliveTime(anyLong(), any(TimeUnit.class));
        verify(channelBuilder, never()).keepAliveTimeout(anyLong(), any(TimeUnit.class));
        verify(channelBuilder, never()).idleTimeout(anyLong(), any(TimeUnit.class));
        verify(channelBuilder, never()).keepAliveWithoutCalls(anyBoolean());
    }

    @Test
    void shouldEnableKeepAliveWhenConfiguredWithDefaults() {
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_KEEP_ALIVE_ENABLED, "true");

        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder).keepAliveTime(60, TimeUnit.SECONDS);
        verify(channelBuilder).keepAliveTimeout(10, TimeUnit.SECONDS);
        verify(channelBuilder).idleTimeout(300, TimeUnit.SECONDS);
        verify(channelBuilder).keepAliveWithoutCalls(false);
    }

    @Test
    void shouldEnableKeepAliveWithCustomValues() {
        // Generate random values for testing
        val keepAliveTime = random.nextInt(1000) + 1;
        val keepAliveTimeout = random.nextInt(500) + 1;
        val keepAliveWithoutCalls = random.nextBoolean();
        val idleTimeout = random.nextInt(10000) + 1;

        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_KEEP_ALIVE_ENABLED, "true");
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_KEEP_ALIVE_TIME, String.valueOf(keepAliveTime));
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_KEEP_ALIVE_TIMEOUT, String.valueOf(keepAliveTimeout));
        properties.setProperty(
                DataCloudJdbcManagedChannel.GRPC_KEEP_ALIVE_WITHOUT_CALLS, String.valueOf(keepAliveWithoutCalls));
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_IDLE_TIMEOUT_SECONDS, String.valueOf(idleTimeout));

        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder).keepAliveTime(keepAliveTime, TimeUnit.SECONDS);
        verify(channelBuilder).keepAliveTimeout(keepAliveTimeout, TimeUnit.SECONDS);
        verify(channelBuilder).idleTimeout(idleTimeout, TimeUnit.SECONDS);
        verify(channelBuilder).keepAliveWithoutCalls(keepAliveWithoutCalls);
    }

    @Test
    void shouldEnableRetriesByDefaultWithDefaults() {
        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder).enableRetry();
        verify(channelBuilder).maxRetryAttempts(5);
        verify(channelBuilder).defaultServiceConfig(anyMap());
    }

    @Test
    void shouldEnableRetriesWithCustomValues() {
        val maxRetryAttempts = random.nextInt(20) + 2;
        val initialBackoff = random.nextDouble() * 5.0;
        val maxBackoff = random.nextInt(200) + 30;
        val backoffMultiplier = random.nextDouble() * 5.0 + 1.0;
        val statusCode1 = Status.UNAVAILABLE.getCode().name();
        val statusCode2 = Status.DEADLINE_EXCEEDED.getCode().name();
        val retryableStatusCodes = statusCode1 + "," + statusCode2;

        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_RETRY_ENABLED, "true");
        properties.setProperty(
                DataCloudJdbcManagedChannel.GRPC_RETRY_POLICY_MAX_ATTEMPTS, String.valueOf(maxRetryAttempts));
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_RETRY_POLICY_INITIAL_BACKOFF, initialBackoff + "s");
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_RETRY_POLICY_MAX_BACKOFF, maxBackoff + "s");
        properties.setProperty(
                DataCloudJdbcManagedChannel.GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER, String.valueOf(backoffMultiplier));
        properties.setProperty(
                DataCloudJdbcManagedChannel.GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES, retryableStatusCodes);

        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder).enableRetry();
        verify(channelBuilder).maxRetryAttempts(maxRetryAttempts);

        verify(channelBuilder).defaultServiceConfig(argThat(config -> {
            String configStr = config.toString();
            return configStr.contains("maxAttempts=" + maxRetryAttempts)
                    && configStr.contains("initialBackoff=" + initialBackoff + "s")
                    && configStr.contains("maxBackoff=" + maxBackoff + "s")
                    && configStr.contains("backoffMultiplier=" + backoffMultiplier)
                    && configStr.contains(statusCode1)
                    && configStr.contains(statusCode2);
        }));
    }

    @Test
    void shouldNotEnableRetriesWhenDisabled() {
        properties.setProperty(DataCloudJdbcManagedChannel.GRPC_RETRY_ENABLED, "false");

        DataCloudJdbcManagedChannel.of(channelBuilder, properties);

        verify(channelBuilder, never()).enableRetry();
        verify(channelBuilder, never()).maxRetryAttempts(anyInt());
        verify(channelBuilder, never()).defaultServiceConfig(anyMap());
    }

    @SneakyThrows
    @Test
    void callsManagedChannelCleanup() {
        val mocked = mock(ManagedChannel.class);

        when(channelBuilder.build()).thenReturn(mocked);

        when(mocked.isTerminated()).thenReturn(false, true);

        val actual = DataCloudJdbcManagedChannel.of(channelBuilder, properties);
        actual.close();

        verify(mocked).shutdown();
        verify(mocked).awaitTermination(5, TimeUnit.SECONDS);
        verify(mocked, never()).shutdownNow();
    }

    @SneakyThrows
    @Test
    void callsManagedChannelShutdownNow() {
        val mocked = mock(ManagedChannel.class);

        when(channelBuilder.build()).thenReturn(mocked);

        when(mocked.isTerminated()).thenReturn(false);

        val actual = DataCloudJdbcManagedChannel.of(channelBuilder, properties);
        actual.close();

        verify(mocked).shutdown();
        verify(mocked).awaitTermination(5, TimeUnit.SECONDS);
        verify(mocked).shutdownNow();
    }
}
