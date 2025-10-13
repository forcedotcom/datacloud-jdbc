/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalBoolean;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalInteger;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Unstable;
import io.grpc.ManagedChannelBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

/**
 * gRPC channel properties that control the gRPC connection behavior.
 *
 * Configures client retries and keep-alives.
 *
 * See [A6-client-retries.md](https://github.com/grpc/proposal/blob/master/A6-client-retries.md) for more details on the retry configuration.
 * See [A8-client-side-keepalive.md](https://github.com/grpc/proposal/blob/master/A8-client-side-keepalive.md) for more details on the keep alive configuration.
 *
 * - grpc.keepAlive: enable keep alive, default is false
 * - grpc.keepAlive.Time: setting for {@link ManagedChannelBuilder#keepAliveTime} default is 60 seconds
 * - grpc.keepAlive.Timeout: setting for {@link ManagedChannelBuilder#keepAliveTimeout} default is 10 seconds
 * - grpc.keepAlive.WithoutCalls: setting for {@link ManagedChannelBuilder#keepAliveWithoutCalls} default is false; not recommended by gRPC, prefer {@link ManagedChannelBuilder#idleTimeout}
 * - grpc.idleTimeoutSeconds: setting for {@link ManagedChannelBuilder#idleTimeout} default is 1800 seconds
 *
 * - grpc.enableRetries: enable retries, default is true
 * - grpc.retryPolicy.maxAttempts: setting for {@link ManagedChannelBuilder#maxRetryAttempts} default is 5
 * - grpc.retryPolicy.initialBackoff: setting for the defaultServiceConfig map's initialBackoff key default is 0.5s
 * - grpc.retryPolicy.maxBackoff: setting for the defaultServiceConfig map's maxBackoff key default is 30s
 * - grpc.retryPolicy.backoffMultiplier: setting for the defaultServiceConfig map's backoffMultiplier key default is 2.0
 * - grpc.retryPolicy.retryableStatusCodes: setting for the defaultServiceConfig map's retryableStatusCodes key default is [UNAVAILABLE]
 */
@Getter
@Builder
@Unstable
public class GrpcChannelProperties {
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

    private static final int GRPC_INBOUND_MESSAGE_MAX_SIZE = 64 * 1024 * 1024;
    private static final int GRPC_INBOUND_METADATA_MAX_SIZE = 1024 * 1024;

    // Keep alive properties
    @Builder.Default
    private final boolean keepAliveEnabled = false;

    @Builder.Default
    private final int keepAliveTime = 60;

    @Builder.Default
    private final int keepAliveTimeout = 10;

    @Builder.Default
    private final int idleTimeoutSeconds = 300;

    @Builder.Default
    private final boolean keepAliveWithoutCalls = false;

    // Retry properties
    @Builder.Default
    private final boolean retryEnabled = true;

    @Builder.Default
    private final int retryMaxAttempts = 5;

    @Builder.Default
    private final String retryInitialBackoff = "0.5s";

    @Builder.Default
    private final String retryMaxBackoff = "30s";

    @Builder.Default
    private final String retryBackoffMultiplier = "2.0";

    @Builder.Default
    private final List<String> retryableStatusCodes = Arrays.asList("UNAVAILABLE");

    public static GrpcChannelProperties defaultProperties() {
        return builder().build();
    }

    /**
     * Parses gRPC channel properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A GrpcChannelProperties instance
     */
    public static GrpcChannelProperties ofDestructive(Properties props) throws DataCloudJDBCException {
        GrpcChannelPropertiesBuilder builder = GrpcChannelProperties.builder();

        // Keep alive properties
        boolean keepAliveEnabled =
                takeOptionalBoolean(props, GRPC_KEEP_ALIVE_ENABLED).orElse(false);
        builder.keepAliveEnabled(keepAliveEnabled);
        if (keepAliveEnabled) {
            takeOptionalInteger(props, GRPC_KEEP_ALIVE_TIME).ifPresent(builder::keepAliveTime);
            takeOptionalInteger(props, GRPC_KEEP_ALIVE_TIMEOUT).ifPresent(builder::keepAliveTimeout);
            takeOptionalInteger(props, GRPC_IDLE_TIMEOUT_SECONDS).ifPresent(builder::idleTimeoutSeconds);
            takeOptionalBoolean(props, GRPC_KEEP_ALIVE_WITHOUT_CALLS).ifPresent(builder::keepAliveWithoutCalls);
        } else if (!Collections.disjoint(
                props.keySet(),
                Arrays.asList(
                        GRPC_KEEP_ALIVE_ENABLED,
                        GRPC_KEEP_ALIVE_TIME,
                        GRPC_KEEP_ALIVE_TIMEOUT,
                        GRPC_IDLE_TIMEOUT_SECONDS,
                        GRPC_KEEP_ALIVE_WITHOUT_CALLS))) {
            throw new DataCloudJDBCException(
                    "grpc.keepAlive must be set to true if grpc.keepAlive.to use any of the grpc.keepAlive.* properties",
                    "HY000");
        }

        // Retry properties
        boolean retryEnabled = takeOptionalBoolean(props, GRPC_RETRY_ENABLED).orElse(true);
        builder.retryEnabled(retryEnabled);
        if (retryEnabled) {
            takeOptionalInteger(props, GRPC_RETRY_POLICY_MAX_ATTEMPTS).ifPresent(builder::retryMaxAttempts);
            takeOptional(props, GRPC_RETRY_POLICY_INITIAL_BACKOFF).ifPresent(builder::retryInitialBackoff);
            takeOptional(props, GRPC_RETRY_POLICY_MAX_BACKOFF).ifPresent(builder::retryMaxBackoff);
            takeOptional(props, GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER).ifPresent(builder::retryBackoffMultiplier);
            takeOptionalList(props, GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES).ifPresent(builder::retryableStatusCodes);
        } else if (!Collections.disjoint(
                props.keySet(),
                Arrays.asList(
                        GRPC_RETRY_ENABLED,
                        GRPC_RETRY_POLICY_MAX_ATTEMPTS,
                        GRPC_RETRY_POLICY_INITIAL_BACKOFF,
                        GRPC_RETRY_POLICY_MAX_BACKOFF,
                        GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER,
                        GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES))) {
            throw new DataCloudJDBCException(
                    "grpc.enableRetries must be set to true if grpc.retryPolicy.* properties are used", "HY000");
        }

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the gRPC channel properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (keepAliveEnabled) {
            props.setProperty(GRPC_KEEP_ALIVE_ENABLED, "true");
        }
        if (keepAliveTime != 60) {
            props.setProperty(GRPC_KEEP_ALIVE_TIME, String.valueOf(keepAliveTime));
        }
        if (keepAliveTimeout != 10) {
            props.setProperty(GRPC_KEEP_ALIVE_TIMEOUT, String.valueOf(keepAliveTimeout));
        }
        if (idleTimeoutSeconds != 300) {
            props.setProperty(GRPC_IDLE_TIMEOUT_SECONDS, String.valueOf(idleTimeoutSeconds));
        }
        if (keepAliveWithoutCalls) {
            props.setProperty(GRPC_KEEP_ALIVE_WITHOUT_CALLS, "true");
        }
        if (!retryEnabled) {
            props.setProperty(GRPC_RETRY_ENABLED, "false");
        }
        if (retryMaxAttempts != 5) {
            props.setProperty(GRPC_RETRY_POLICY_MAX_ATTEMPTS, String.valueOf(retryMaxAttempts));
        }
        if (!"0.5s".equals(retryInitialBackoff)) {
            props.setProperty(GRPC_RETRY_POLICY_INITIAL_BACKOFF, retryInitialBackoff);
        }
        if (!"30s".equals(retryMaxBackoff)) {
            props.setProperty(GRPC_RETRY_POLICY_MAX_BACKOFF, retryMaxBackoff);
        }
        if (!"2.0".equals(retryBackoffMultiplier)) {
            props.setProperty(GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER, retryBackoffMultiplier);
        }
        if (!Arrays.asList("UNAVAILABLE").equals(retryableStatusCodes)) {
            props.setProperty(GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES, String.join(",", retryableStatusCodes));
        }

        return props;
    }

    /**
     * Applies the settings of this instance to a {@link ManagedChannelBuilder}.
     */
    public void applyToChannel(ManagedChannelBuilder<?> builder) {
        // General, setting-independent setup
        builder.maxInboundMessageSize(GRPC_INBOUND_MESSAGE_MAX_SIZE);
        builder.maxInboundMetadataSize(GRPC_INBOUND_METADATA_MAX_SIZE);
        builder.userAgent(formatDriverInfo());

        // Keep alive settings
        if (isKeepAliveEnabled()) {
            builder.keepAliveTime(getKeepAliveTime(), TimeUnit.SECONDS)
                    .keepAliveTimeout(getKeepAliveTimeout(), TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(isKeepAliveWithoutCalls())
                    .idleTimeout(getIdleTimeoutSeconds(), TimeUnit.SECONDS);
        }

        // Retry settings
        if (isRetryEnabled()) {
            val policy = ImmutableMap.of(
                    "methodConfig",
                    ImmutableList.of(ImmutableMap.of(
                            "name",
                            ImmutableList.of(Collections.EMPTY_MAP),
                            "retryPolicy",
                            ImmutableMap.of(
                                    "maxAttempts",
                                    String.valueOf(getRetryMaxAttempts()),
                                    "initialBackoff",
                                    getRetryInitialBackoff(),
                                    "maxBackoff",
                                    getRetryMaxBackoff(),
                                    "backoffMultiplier",
                                    getRetryBackoffMultiplier(),
                                    "retryableStatusCodes",
                                    getRetryableStatusCodes()))));

            builder.enableRetry().maxRetryAttempts(getRetryMaxAttempts()).defaultServiceConfig(policy);
        }
    }
}
