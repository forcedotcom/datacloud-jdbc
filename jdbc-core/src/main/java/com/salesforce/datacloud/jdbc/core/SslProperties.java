/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;

import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import javax.net.ssl.TrustManagerFactory;
import lombok.Builder;
import lombok.Getter;

/**
 * SSL/TLS properties for DataCloud JDBC connections.
 * */
@Getter
@Builder
public class SslProperties {
    public enum SslMode {
        DISABLED,
        DEFAULT_TLS,
        ONE_SIDED_TLS,
        MUTUAL_TLS;
    }
    // Property to disable SSL (for testing only, we might change the implementation in future)
    public static final String SSL_DISABLED = "ssl.disabled";

    // JKS truststore properties - for trust verification
    public static final String SSL_TRUSTSTORE_PATH = "ssl.truststore.path";
    public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";

    // PEM certificate properties - for trust verification and client authentication
    public static final String SSL_CLIENT_CERT_PATH = "ssl.client.certPath";
    public static final String SSL_CLIENT_KEY_PATH = "ssl.client.keyPath";
    public static final String SSL_CA_CERT_PATH = "ssl.ca.certPath";

    // Internal constants
    private static final String DEFAULT_TRUSTSTORE_TYPE = "JKS";

    @Builder.Default
    private final SslMode sslMode = SslMode.DEFAULT_TLS;

    // For SslMode.ONE_SIDED_TLS and SslMode.MUTUAL_TLS (optional for both)
    @Builder.Default
    private final String truststorePathValue = null;

    // For SslMode.ONE_SIDED_TLS and SslMode.MUTUAL_TLS (optional for both, only if truststore is used)
    @Builder.Default
    private final String truststorePasswordValue = null;

    // For SslMode.ONE_SIDED_TLS and SslMode.MUTUAL_TLS (optional for both, only if truststore is used)
    @Builder.Default
    private final String truststoreTypeValue = DEFAULT_TRUSTSTORE_TYPE;

    // For SslMode.MUTUAL_TLS (required for mutual TLS)
    @Builder.Default
    private final String clientCertPathValue = null;

    // For SslMode.MUTUAL_TLS (required for mutual TLS)
    @Builder.Default
    private final String clientKeyPathValue = null;

    // For SslMode.ONE_SIDED_TLS and SslMode.MUTUAL_TLS (optional for both, alternative to truststore)
    @Builder.Default
    private final String caCertPathValue = null;

    /**
     * Parses connection properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A SslProperties instance
     * @throws SQLException if parsing of property values fails
     */
    public static SslProperties ofDestructive(Properties props) throws SQLException {
        SslPropertiesBuilder builder = SslProperties.builder();

        // Extract ssl.disabled early (needed for validation and when ssl.disabled is passed explicitly as false)
        boolean sslDisabled =
                Boolean.parseBoolean(takeOptional(props, SSL_DISABLED).orElse("false"));

        if (sslDisabled) {
            // checks if any additional properties are present with sslDisabled, if yes, then it throws error
            if (!Collections.disjoint(
                    props.keySet(),
                    Arrays.asList(
                            SSL_CLIENT_CERT_PATH,
                            SSL_CLIENT_KEY_PATH,
                            SSL_CA_CERT_PATH,
                            SSL_TRUSTSTORE_PATH,
                            SSL_TRUSTSTORE_PASSWORD,
                            SSL_TRUSTSTORE_TYPE))) {
                throw new SQLException("Cannot specify ssl.disabled=true with other SSL properties. ", "HY000");
            }
            builder.sslMode(SslMode.DISABLED);
        } else if (props.containsKey(SSL_CLIENT_CERT_PATH) && props.containsKey(SSL_CLIENT_KEY_PATH)) {
            // if both client cert and client key are present, TLS mode should be mtls
            if (props.containsKey(SSL_CA_CERT_PATH) && props.containsKey(SSL_TRUSTSTORE_PATH)) {
                throw new SQLException("Cannot specify both ssl.ca.certPath and ssl.truststore.path. ", "HY000");
            }
            builder.sslMode(SslMode.MUTUAL_TLS);

            // Extract and validate client certificate properties
            String clientCertPath = takeOptional(props, SSL_CLIENT_CERT_PATH).orElse(null);
            String clientKeyPath = takeOptional(props, SSL_CLIENT_KEY_PATH).orElse(null);

            validateFilePath(SSL_CLIENT_CERT_PATH, clientCertPath);
            validateFilePath(SSL_CLIENT_KEY_PATH, clientKeyPath);

            builder.clientCertPathValue(clientCertPath);
            builder.clientKeyPathValue(clientKeyPath);

            // Either ca_cert or Trust store can be present,not both
            if (props.containsKey(SSL_CA_CERT_PATH)) {
                String caCertPath = takeOptional(props, SSL_CA_CERT_PATH).orElse(null);
                validateFilePath(SSL_CA_CERT_PATH, caCertPath);
                builder.caCertPathValue(caCertPath);

            } else if (props.containsKey(SSL_TRUSTSTORE_PATH)) {
                String truststorePath = takeOptional(props, SSL_TRUSTSTORE_PATH).orElse(null);
                validateFilePath(SSL_TRUSTSTORE_PATH, truststorePath);

                builder.truststorePathValue(truststorePath);
                builder.truststorePasswordValue(
                        takeOptional(props, SSL_TRUSTSTORE_PASSWORD).orElse(null));
                builder.truststoreTypeValue(
                        takeOptional(props, SSL_TRUSTSTORE_TYPE).orElse(DEFAULT_TRUSTSTORE_TYPE));
            }
        } else if (props.containsKey(SSL_CLIENT_CERT_PATH) || props.containsKey(SSL_CLIENT_KEY_PATH)) {
            // Can't have only one of cert or key
            if (props.containsKey(SSL_CLIENT_CERT_PATH)) {
                throw new SQLException(
                        "Client certificate provided but private key is missing. "
                                + "Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            } else {
                throw new SQLException(
                        "Client private key provided but certificate is missing. "
                                + "Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            }
        } else if (props.containsKey(SSL_CA_CERT_PATH) || props.containsKey(SSL_TRUSTSTORE_PATH)) {
            if (props.containsKey(SSL_CA_CERT_PATH) && props.containsKey(SSL_TRUSTSTORE_PATH)) {
                throw new SQLException("Cannot specify both ssl.ca.certPath and ssl.truststore.path. ", "HY000");
            }
            builder.sslMode(SslMode.ONE_SIDED_TLS);

            if (props.containsKey(SSL_CA_CERT_PATH)) {
                String caCertPath = takeOptional(props, SSL_CA_CERT_PATH).orElse(null);
                validateFilePath(SSL_CA_CERT_PATH, caCertPath);
                builder.caCertPathValue(caCertPath);
            } else {
                String truststorePath = takeOptional(props, SSL_TRUSTSTORE_PATH).orElse(null);
                validateFilePath(SSL_TRUSTSTORE_PATH, truststorePath);

                builder.truststorePathValue(truststorePath);
                builder.truststorePasswordValue(
                        takeOptional(props, SSL_TRUSTSTORE_PASSWORD).orElse(null));
                builder.truststoreTypeValue(
                        takeOptional(props, SSL_TRUSTSTORE_TYPE).orElse(DEFAULT_TRUSTSTORE_TYPE));
            }
        } else {
            // DEFAULT TLS mode - no properties to extract
            builder.sslMode(SslMode.DEFAULT_TLS);
        }

        // Check for mixed SSL properties from different modes
        if (!Collections.disjoint(
                props.keySet(),
                Arrays.asList(
                        SSL_DISABLED,
                        SSL_TRUSTSTORE_PATH,
                        SSL_TRUSTSTORE_PASSWORD,
                        SSL_TRUSTSTORE_TYPE,
                        SSL_CLIENT_CERT_PATH,
                        SSL_CLIENT_KEY_PATH,
                        SSL_CA_CERT_PATH))) {
            throw new SQLException("SSL properties from different modes cannot be mixed", "HY000");
        }

        return builder.build();
    }

    public static SslProperties defaultProperties() {
        return builder().build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the SSL properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        // Serialize based on SSL mode
        switch (sslMode) {
            case DISABLED:
                props.setProperty(SSL_DISABLED, "true");
                break;
            case MUTUAL_TLS:
                if (clientCertPathValue != null) {
                    props.setProperty(SSL_CLIENT_CERT_PATH, clientCertPathValue);
                }
                if (clientKeyPathValue != null) {
                    props.setProperty(SSL_CLIENT_KEY_PATH, clientKeyPathValue);
                }
            // intentional fall-through to ONE_SIDED_TLS to serialize common truststore/CA properties
            case ONE_SIDED_TLS:
                if (truststorePathValue != null) {
                    props.setProperty(SSL_TRUSTSTORE_PATH, truststorePathValue);
                }
                if (truststorePasswordValue != null) {
                    props.setProperty(SSL_TRUSTSTORE_PASSWORD, truststorePasswordValue);
                }
                if (!truststoreTypeValue.equals(DEFAULT_TRUSTSTORE_TYPE)) {
                    props.setProperty(SSL_TRUSTSTORE_TYPE, truststoreTypeValue);
                }
                if (caCertPathValue != null) {
                    props.setProperty(SSL_CA_CERT_PATH, caCertPathValue);
                }
                break;
            case DEFAULT_TLS:
                // No additional properties needed for default TLS
                break;
        }

        return props;
    }

    /**
     * Creates a gRPC channel builder with appropriate SSL/TLS configuration.
     *
     * @param host The target host
     * @param port The target port
     * @return Configured ManagedChannelBuilder
     * @throws SQLException if channel creation fails
     */
    public ManagedChannelBuilder<?> createChannelBuilder(String host, int port) throws SQLException {
        switch (sslMode) {
            case DISABLED:
                return ManagedChannelBuilder.forAddress(host, port).usePlaintext();
            case DEFAULT_TLS:
                return NettyChannelBuilder.forAddress(host, port).useTransportSecurity();
            case ONE_SIDED_TLS:
            case MUTUAL_TLS:
                return createCustomSslChannelBuilder(host, port);
            default:
                throw new IllegalStateException("Unsupported SSL mode: " + sslMode);
        }
    }

    /**
     * Creates an SSL channel builder with custom trust configuration.
     * Implements proper SSL context with custom certificates and truststore.
     */
    private ManagedChannelBuilder<?> createCustomSslChannelBuilder(String host, int port) throws SQLException {
        try {
            // Build SSL context with trust manager and optional key manager
            SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

            // Configure trust manager (CA verification)
            if (caCertPathValue != null) {
                // Use PEM CA certificate file
                sslContextBuilder.trustManager(new File(caCertPathValue));
            } else {
                // Use JKS truststore or system default
                sslContextBuilder.trustManager(createTrustManagerFactory());
            }

            // Configure key manager for mutual TLS (if client certs provided)
            if (clientCertPathValue != null && clientKeyPathValue != null) {
                sslContextBuilder.keyManager(new File(clientCertPathValue), new File(clientKeyPathValue));
            }

            SslContext sslContext = sslContextBuilder.build();
            return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
        } catch (Exception e) {
            throw new SQLException("Failed to create SSL configuration: " + e.getMessage(), "HY000", e);
        }
    }

    private TrustManagerFactory createTrustManagerFactory() throws Exception {
        if (truststorePathValue != null) {
            try {
                KeyStore trustStore = KeyStore.getInstance(truststoreTypeValue);
                try (FileInputStream fis = new FileInputStream(truststorePathValue)) {
                    char[] password = truststorePasswordValue != null ? truststorePasswordValue.toCharArray() : null;
                    trustStore.load(fis, password);
                }
                TrustManagerFactory trustManagerFactory =
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
                return trustManagerFactory;
            } catch (Exception e) {
                throw new SQLException("Failed to create trust manager from truststore: " + e.getMessage(), "HY000", e);
            }
        }
        // System truststore (fallback)
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);
        return tmf;
    }

    /**
     * Validates that a file path is non-empty and points to a valid, readable, non-empty file.
     * This validation applies to all SSL-related files (PEM certificates, private keys, truststore files).
     *
     * @param propertyKey The property key (for error messages)
     * @param filePath The file path value to validate
     * @throws SQLException if validation fails (path is empty, file doesn't exist, not readable, or empty file)
     */
    private static void validateFilePath(String propertyKey, String filePath) throws SQLException {
        // Validate non-empty
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new SQLException(propertyKey + " cannot be empty", "HY000");
        }

        // Validate file exists and is readable
        File file = new File(filePath);
        if (!file.exists()) {
            throw new SQLException(
                    "File not found, ensure the file exists and the path is correct. property=" + propertyKey
                            + ", path=" + filePath,
                    "HY000");
        }
        if (!file.canRead()) {
            throw new SQLException(
                    "File is not readable, check file permissions. property=" + propertyKey + ", path=" + filePath,
                    "HY000");
        }
        if (file.length() == 0) {
            throw new SQLException(propertyKey + " file is empty: " + filePath, "HY000");
        }
    }
}
