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

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getIntegerOrDefault;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getListOrDefault;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DataCloudJdbcManagedChannel implements AutoCloseable {
    @Getter()
    public final ManagedChannel channel;

    private static final int GRPC_INBOUND_MESSAGE_MAX_SIZE = 64 * 1024 * 1024;

    public static final String GRPC_KEEP_ALIVE_ENABLED = "grpc.keepAlive";
    public static final String GRPC_KEEP_ALIVE_TIME = "grpc.keepAlive.time";
    public static final String GRPC_KEEP_ALIVE_TIMEOUT = "grpc.keepAlive.timeout";
    public static final String GRPC_IDLE_TIMEOUT_SECONDS = "grpc.idleTimeoutSeconds";
    public static final String GRPC_KEEP_ALIVE_WITHOUT_CALLS = "grpc.keepAlive.withoutCalls";

    public static final String GRPC_RETRY_ENABLED = "grpc.enableRetries";
    public static final String GRPC_RETRY_POLICY_MAX_ATTEMPTS = "grpc.retryPolicy.maxAttempts";
    public static final String GRPC_RETRY_POLICY_INITIAL_BACKOFF = "grpc.retryPolicy.initialBackoff";
    public static final String GRPC_RETRY_POLICY_MAX_BACKOFF = "grpc.retryPolicy.maxBackoff";
    public static final String GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER = "grpc.retryPolicy.backoffMultiplier";
    public static final String GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES = "grpc.retryPolicy.retryableStatusCodes";

    /**
     * Configure only required settings ({@link ManagedChannelBuilder#maxInboundMessageSize(int)} and {@link ManagedChannelBuilder#userAgent(String)}) and immediately builds and stores a {@link ManagedChannel}.
     * After this configuration is made subsequent stubs will have required interceptors wired, but the original {@link ManagedChannel} will be left untouched.
     */
    public static DataCloudJdbcManagedChannel of(ManagedChannelBuilder<?> builder) {
        builder.maxInboundMessageSize(GRPC_INBOUND_MESSAGE_MAX_SIZE);
        builder.userAgent(formatDriverInfo());

        return new DataCloudJdbcManagedChannel(builder.build());
    }

    /**
     * Configure required settings (inbound message size and user agent) in addition to optional keep alive and retry settings based on the provided properties.
     * <br/>
     * See <a href="https://github.com/grpc/proposal/blob/master/A6-client-retries.md">A6-client-retries.md</a> for more details on the retry configuration.
     * See <a href="https://github.com/grpc/proposal/blob/master/A8-client-side-keepalive.md">A8-client-side-keepalive.md</a> for more details on the keep alive configuration.
     * <br/>
     * - grpc.KeepAlive: enable keep alive, default is false
     * - grpc.KeepAlive.Time: setting for {@link ManagedChannelBuilder#keepAliveTime} default is 60 seconds
     * - grpc.KeepAlive.Timeout: setting for {@link ManagedChannelBuilder#keepAliveTimeout} default is 10 seconds
     * - grpc.KeepAlive.WithoutCalls: setting for {@link ManagedChannelBuilder#keepAliveWithoutCalls} default is false; not recommended by gRPC, prefer {@link ManagedChannelBuilder#idleTimeout}
     * - grpc.IdleTimeoutSeconds: setting for {@link ManagedChannelBuilder#idleTimeout} default is 1800 seconds
     * <br/>
     * - grpc.enableRetries: enable retries, default is true
     * - grpc.retryPolicy.maxAttempts: setting for {@link ManagedChannelBuilder#maxRetryAttempts} default is 5
     * - grpc.retryPolicy.initialBackoff: setting for the defaultServiceConfig map's initialBackoff key default is 0.5s
     * - grpc.retryPolicy.maxBackoff: setting for the defaultServiceConfig map's maxBackoff key default is 30s
     * - grpc.retryPolicy.backoffMultiplier: setting for the defaultServiceConfig map's backoffMultiplier key default is 2.0
     * - grpc.retryPolicy.retryableStatusCodes: setting for the defaultServiceConfig map's retryableStatusCodes key default is [UNAVAILABLE]
     * <br/>
     * See <a href="https://github.com/grpc/grpc-java/blob/master/api/src/main/java/io/grpc/ManagedChannelBuilder.java">ManagedChannelBuilder.java</a> for a detailed description of these configuration options.
     */
    public static DataCloudJdbcManagedChannel of(ManagedChannelBuilder<?> builder, Properties properties) {

        configureKeepAlive(builder, properties);
        configureRetries(builder, properties);

        return of(builder);
    }

    private static void configureKeepAlive(ManagedChannelBuilder<?> builder, Properties properties) {
        if (getBooleanOrDefault(properties, GRPC_KEEP_ALIVE_ENABLED, Boolean.FALSE)) {
            val keepAliveTime = getIntegerOrDefault(properties, GRPC_KEEP_ALIVE_TIME, 60);
            val keepAliveTimeout = getIntegerOrDefault(properties, GRPC_KEEP_ALIVE_TIMEOUT, 10);
            val idleTimeoutSeconds = getIntegerOrDefault(properties, GRPC_IDLE_TIMEOUT_SECONDS, 300);
            val keepAliveWithoutCalls = getBooleanOrDefault(properties, GRPC_KEEP_ALIVE_WITHOUT_CALLS, false);

            log.info(
                    "Configuring keep alive, keepAliveTimeSeconds={}, keepAliveTimeoutSeconds={}, keepAliveWithoutCalls={}, idleTimeoutSeconds={}",
                    keepAliveTime,
                    keepAliveTimeout,
                    keepAliveWithoutCalls,
                    idleTimeoutSeconds);

            builder.keepAliveTime(keepAliveTime, TimeUnit.SECONDS)
                    .keepAliveTimeout(keepAliveTimeout, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(keepAliveWithoutCalls)
                    .idleTimeout(idleTimeoutSeconds, TimeUnit.SECONDS);
        } else {
            log.info("Will not enable keep alive, set grpc.KeepAlive=true to enable");
        }
    }

    private static void configureRetries(ManagedChannelBuilder<?> builder, Properties properties) {
        if (getBooleanOrDefault(properties, GRPC_RETRY_ENABLED, Boolean.TRUE)) {
            val maxRetryAttempts = getIntegerOrDefault(properties, GRPC_RETRY_POLICY_MAX_ATTEMPTS, 5);
            val initialBackoff =
                    optional(properties, GRPC_RETRY_POLICY_INITIAL_BACKOFF).orElse("0.5s");
            val maxBackoff = optional(properties, GRPC_RETRY_POLICY_MAX_BACKOFF).orElse("30s");
            val backoffMultiplier =
                    optional(properties, GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER).orElse("2.0");
            val retryableStatusCodes =
                    getListOrDefault(properties, GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES, "UNAVAILABLE");

            val policy =
                    retryPolicy(maxRetryAttempts, initialBackoff, maxBackoff, backoffMultiplier, retryableStatusCodes);

            log.info(
                    "Will enable gRPC's built in retries with: maxRetryAttempts={}, retryPolicy={}",
                    maxRetryAttempts,
                    policy);

            builder.enableRetry().maxRetryAttempts(maxRetryAttempts).defaultServiceConfig(policy);
        } else {
            log.info("Will not enable gRPC's built in retries, set grpc.enableRetries=true to enable");
        }
    }

    private static Map<String, Object> retryPolicy(
            int maxRetryAttempts,
            String initialBackoff,
            String maxBackoff,
            String backoffMultiplier,
            List<String> retryableStatusCodes) {
        return ImmutableMap.of(
                "methodConfig",
                ImmutableList.of(ImmutableMap.of(
                        "name",
                        ImmutableList.of(Collections.EMPTY_MAP),
                        "retryPolicy",
                        ImmutableMap.of(
                                "maxAttempts",
                                String.valueOf(maxRetryAttempts),
                                "initialBackoff",
                                initialBackoff,
                                "maxBackoff",
                                maxBackoff,
                                "backoffMultiplier",
                                backoffMultiplier,
                                "retryableStatusCodes",
                                retryableStatusCodes))));
    }

    @Override
    public void close() {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            return;
        }

        channel.shutdown();

        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Failed to shutdown channel within 5 seconds", e);
        } finally {
            if (!channel.isTerminated()) {
                channel.shutdownNow();
            }
        }
    }
}
