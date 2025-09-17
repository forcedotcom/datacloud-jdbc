/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class GrpcChannelPropertiesTest {
    @Test
    void testParseFromPropertiesWithKeepAliveEnabled() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_ENABLED, "true");
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIME, "90");
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIMEOUT, "15");
        props.setProperty(GrpcChannelProperties.GRPC_IDLE_TIMEOUT_SECONDS, "450");
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_WITHOUT_CALLS, "true");
        props.setProperty("otherSetting", "foobar");

        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(props);

        assertThat(grpcProps.isKeepAliveEnabled()).isTrue();
        assertThat(grpcProps.getKeepAliveTime()).isEqualTo(90);
        assertThat(grpcProps.getKeepAliveTimeout()).isEqualTo(15);
        assertThat(grpcProps.getIdleTimeoutSeconds()).isEqualTo(450);
        assertThat(grpcProps.isKeepAliveWithoutCalls()).isTrue();

        // Properties should be removed after parsing
        assertThat(props).size().isEqualTo(1);
        assertThat(props).containsEntry("otherSetting", "foobar");
    }

    @Test
    void testParseFromPropertiesWithRetryEnabled() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_ENABLED, "true");
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_ATTEMPTS, "7");
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_INITIAL_BACKOFF, "1.0s");
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_BACKOFF, "45s");
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER, "2.5");
        props.setProperty(
                GrpcChannelProperties.GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES,
                "UNAVAILABLE,DEADLINE_EXCEEDED,RESOURCE_EXHAUSTED");

        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(props);

        assertThat(grpcProps.isRetryEnabled()).isTrue();
        assertThat(grpcProps.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(grpcProps.getRetryInitialBackoff()).isEqualTo("1.0s");
        assertThat(grpcProps.getRetryMaxBackoff()).isEqualTo("45s");
        assertThat(grpcProps.getRetryBackoffMultiplier()).isEqualTo("2.5");
        assertThat(grpcProps.getRetryableStatusCodes())
                .containsExactly("UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED");

        // Properties should be removed after parsing
        assertThat(props).isEmpty();
    }

    @Test
    void testParseFromPropertiesWithKeepAliveDisabled() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_ENABLED, "false");

        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(props);

        assertThat(grpcProps.isKeepAliveEnabled()).isFalse();

        // Properties should be removed after parsing
        assertThat(props).isEmpty();
    }

    @Test
    void testParseFromPropertiesWithRetryDisabled() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_ENABLED, "false");

        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(props);

        assertThat(grpcProps.isRetryEnabled()).isFalse();

        // Properties should be removed after parsing
        assertThat(props).isEmpty();
    }

    @Test
    void testParseFromEmptyProperties() throws DataCloudJDBCException {
        Properties props = new Properties();

        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(props);

        // Should use all defaults (just testing a couple of the defaults)
        assertThat(grpcProps.isKeepAliveEnabled()).isFalse();
        assertThat(grpcProps.isRetryEnabled()).isTrue();
        assertThat(grpcProps.getRetryMaxAttempts()).isEqualTo(5);
        assertThat(grpcProps.getRetryableStatusCodes()).containsExactly("UNAVAILABLE");

        // Properties should remain empty
        assertThat(props).isEmpty();
    }

    @Test
    void testErrorWhenKeepAlivePropertiesUsedWithoutEnabling() {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIME, "90");

        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> GrpcChannelProperties.ofDestructive(props));

        assertThat(exception.getMessage())
                .contains(
                        "grpc.keepAlive must be set to true if grpc.keepAlive.to use any of the grpc.keepAlive.* properties");
        assertThat(exception.getSQLState()).isEqualTo("HY000");
    }

    @Test
    void testErrorWhenRetryPropertiesUsedWithoutEnabling() {
        Properties props = new Properties();
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_ENABLED, "false");
        props.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_ATTEMPTS, "7");

        DataCloudJDBCException exception =
                assertThrows(DataCloudJDBCException.class, () -> GrpcChannelProperties.ofDestructive(props));

        assertThat(exception.getMessage())
                .contains("grpc.enableRetries must be set to true if grpc.retryPolicy.* properties are used");
        assertThat(exception.getSQLState()).isEqualTo("HY000");
    }

    @Test
    void testRoundtripConversion() throws DataCloudJDBCException {
        Properties originalProps = new Properties();
        originalProps.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_ENABLED, "true");
        originalProps.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIME, "90");
        originalProps.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIMEOUT, "15");
        originalProps.setProperty(GrpcChannelProperties.GRPC_IDLE_TIMEOUT_SECONDS, "450");
        originalProps.setProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_WITHOUT_CALLS, "true");
        originalProps.setProperty(GrpcChannelProperties.GRPC_RETRY_ENABLED, "true");
        originalProps.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_ATTEMPTS, "7");
        originalProps.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_INITIAL_BACKOFF, "1.0s");
        originalProps.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_BACKOFF, "45s");
        originalProps.setProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER, "2.5");
        originalProps.setProperty(
                GrpcChannelProperties.GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES, "UNAVAILABLE,DEADLINE_EXCEEDED");

        // Parse to GrpcChannelProperties
        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(originalProps);

        // Convert back to Properties
        Properties roundtripProps = grpcProps.toProperties();

        // Verify round-trip preserves non-default values
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_ENABLED))
                .isEqualTo("true");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIME))
                .isEqualTo("90");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_TIMEOUT))
                .isEqualTo("15");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_IDLE_TIMEOUT_SECONDS))
                .isEqualTo("450");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_KEEP_ALIVE_WITHOUT_CALLS))
                .isEqualTo("true");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_ATTEMPTS))
                .isEqualTo("7");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_INITIAL_BACKOFF))
                .isEqualTo("1.0s");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_MAX_BACKOFF))
                .isEqualTo("45s");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_BACKOFF_MULTIPLIER))
                .isEqualTo("2.5");
        assertThat(roundtripProps.getProperty(GrpcChannelProperties.GRPC_RETRY_POLICY_RETRYABLE_STATUS_CODES))
                .isEqualTo("UNAVAILABLE,DEADLINE_EXCEEDED");
    }
}
