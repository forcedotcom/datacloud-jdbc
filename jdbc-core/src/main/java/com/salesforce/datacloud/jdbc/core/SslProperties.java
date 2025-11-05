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
import java.util.Optional;
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
        boolean sslDisabled =
                takeOptional(props, SSL_DISABLED).map(Boolean::parseBoolean).orElse(false);

        Optional<String> clientCertPath = takeOptional(props, SSL_CLIENT_CERT_PATH);
        Optional<String> clientKeyPath = takeOptional(props, SSL_CLIENT_KEY_PATH);
        Optional<String> caCertPath = takeOptional(props, SSL_CA_CERT_PATH);
        Optional<String> truststorePath = takeOptional(props, SSL_TRUSTSTORE_PATH);
        Optional<String> truststorePassword = takeOptional(props, SSL_TRUSTSTORE_PASSWORD);
        Optional<String> truststoreType = takeOptional(props, SSL_TRUSTSTORE_TYPE);

        // If SSL is disabled, set mode and return early
        if (sslDisabled) {
            builder.sslMode(SslMode.DISABLED);
            return builder.build();
        }

        // Determine SSL mode based on which properties are present
        boolean hasClientCert =
                clientCertPath.isPresent() && !clientCertPath.get().isEmpty();
        boolean hasClientKey = clientKeyPath.isPresent() && !clientKeyPath.get().isEmpty();
        boolean hasCaCert = caCertPath.isPresent() && !caCertPath.get().isEmpty();
        boolean hasTruststore =
                truststorePath.isPresent() && !truststorePath.get().isEmpty();

        if (hasClientCert && hasClientKey) {
            // Both client cert and key present with non-empty values: MUTUAL_TLS mode
            builder.sslMode(SslMode.MUTUAL_TLS);

            // Validate certificate files early (fail fast)
            validateCertificateFiles(clientCertPath.get(), clientKeyPath.get(), caCertPath.orElse(null));

            builder.clientCertPathValue(clientCertPath.get());
            builder.clientKeyPathValue(clientKeyPath.get());

            // CA cert is optional for mutual TLS (might use truststore instead)
            if (hasCaCert) {
                builder.caCertPathValue(caCertPath.get());
            }

            // Truststore is also optional for mutual TLS
            setTruststoreProperties(truststorePath, truststorePassword, truststoreType, builder);

        } else if (hasClientCert || hasClientKey) {
            // Only one of cert/key present:  Error (both required for mutual TLS)
            if (hasClientCert) {
                throw new SQLException(
                        "Client certificate provided but private key is missing. Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            } else {
                throw new SQLException(
                        "Client private key provided but certificate is missing. Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            }
        } else if (hasTruststore || hasCaCert) {
            // Only truststore or CA cert present (no client certs): ONE_SIDED_TLS mode
            builder.sslMode(SslMode.ONE_SIDED_TLS);

            // Set truststore if provided (already validated non-empty above)
            setTruststoreProperties(truststorePath, truststorePassword, truststoreType, builder);

            // Set CA cert if provided and validate it (already validated non-empty above)
            if (hasCaCert) {
                validatePemFile(caCertPath.get(), "CA certificate");
                builder.caCertPathValue(caCertPath.get());
            }
        } else {
            // No custom SSL configuration provided :  DEFAULT_TLS mode
            // Uses system-wide default certificates for verification
            builder.sslMode(SslMode.DEFAULT_TLS);
        }

        return builder.build();
    }

    /**
     * Helper method to set truststore properties from Optional values to builder.
     * Validates that empty strings are not set as property values (defensive programming).
     *
     * @param truststorePath Optional truststore path
     * @param truststorePassword Optional truststore password
     * @param truststoreType Optional truststore type
     * @param builder The builder to set properties on
     */
    private static void setTruststoreProperties(
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> truststoreType,
            SslPropertiesBuilder builder) {
        if (truststorePath.isPresent() && !truststorePath.get().isEmpty()) {
            builder.truststorePathValue(truststorePath.get());
            builder.truststorePasswordValue(truststorePassword.orElse(null));
            builder.truststoreTypeValue(truststoreType.orElse(DEFAULT_TRUSTSTORE_TYPE));
        }
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

    private static void validateCertificateFiles(String clientCertPath, String clientKeyPath, String caCertPath)
            throws SQLException {
        validatePemFile(clientCertPath, "Client certificate");
        validatePemFile(clientKeyPath, "Client private key");

        // CA certificate is optional (might use JKS truststore instead)
        if (caCertPath != null) {
            validatePemFile(caCertPath, "CA certificate");
        }
    }

    private static void validatePemFile(String path, String description) throws SQLException {
        File file = new File(path);
        if (!file.exists()) {
            throw new SQLException(
                    "File not found, ensure the file exists and the path is correct. description=" + description
                            + ", path=" + path,
                    "HY000");
        }
        if (!file.canRead()) {
            throw new SQLException(
                    "File is not readable, check file permissions. description=" + description + ", path=" + path,
                    "HY000");
        }
        if (file.length() == 0) {
            throw new SQLException(description + " file is empty: " + path, "HY000");
        }
    }
}
