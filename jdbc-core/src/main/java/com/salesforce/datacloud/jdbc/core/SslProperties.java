/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Properties;
import javax.net.ssl.TrustManagerFactory;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * SSL/TLS properties for DataCloud JDBC connections.
 * Supports automatic SSL mode detection based on provided certificate properties.
 */
@Slf4j
@Getter
@Builder
public class SslProperties {
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
    private static final String CA_CERT_ENTRY_NAME = "ca-cert";

    @Builder.Default
    private final SslMode sslMode = SslMode.DEFAULT_TLS;

    @Builder.Default
    private final String truststorePathValue = null;

    @Builder.Default
    private final String truststorePasswordValue = null;

    @Builder.Default
    private final String truststoreTypeValue = DEFAULT_TRUSTSTORE_TYPE;

    @Builder.Default
    private final String clientCertPathValue = null;

    @Builder.Default
    private final String clientKeyPathValue = null;

    @Builder.Default
    private final String caCertPathValue = null;

    /**
     * Parses SSL properties from a Properties object using destructive parsing.
     * This method removes the SSL properties from the input Properties object.
     *
     * @param props The properties to parse (will be modified)
     * @return A SslProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static SslProperties ofDestructive(Properties props) throws DataCloudJDBCException {
        SslPropertiesBuilder builder = SslProperties.builder();

        // Parse SSL disabled flag
        boolean sslDisabled =
                takeOptional(props, SSL_DISABLED).map(Boolean::parseBoolean).orElse(false);

        // If SSL is disabled, no other SSL properties are required
        if (sslDisabled) {
            builder.sslMode(SslMode.DISABLED);
            return builder.build();
        }

        String caCertPath = takeOptional(props, SSL_CA_CERT_PATH).orElse(null);

        // Determine SSL mode and set credentials
        if (props.containsKey(SSL_CLIENT_CERT_PATH) && props.containsKey(SSL_CLIENT_KEY_PATH)) {
            // Client certificates present = mutual TLS
            String clientCertPath = takeRequired(props, SSL_CLIENT_CERT_PATH);
            String clientKeyPath = takeRequired(props, SSL_CLIENT_KEY_PATH);

            validateCertificateFiles(clientCertPath, clientKeyPath, caCertPath);

            builder.sslMode(SslMode.MUTUAL_TLS);
            builder.clientCertPathValue(clientCertPath);
            builder.clientKeyPathValue(clientKeyPath);

            if (caCertPath != null) {
                builder.caCertPathValue(caCertPath);
            }

            // Also set truststore if present
            if (props.containsKey(SSL_TRUSTSTORE_PATH)) {
                String truststorePath = takeRequired(props, SSL_TRUSTSTORE_PATH);
                builder.truststorePathValue(truststorePath);
                builder.truststorePasswordValue(
                        takeOptional(props, SSL_TRUSTSTORE_PASSWORD).orElse(null));
                builder.truststoreTypeValue(
                        takeOptional(props, SSL_TRUSTSTORE_TYPE).orElse(DEFAULT_TRUSTSTORE_TYPE));
            }
        } else if (props.containsKey(SSL_CLIENT_CERT_PATH) || props.containsKey(SSL_CLIENT_KEY_PATH)) {
            // Only one client cert property present - throw error
            if (props.containsKey(SSL_CLIENT_CERT_PATH)) {
                throw new DataCloudJDBCException(
                        "Client certificate provided but private key is missing. Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            } else {
                throw new DataCloudJDBCException(
                        "Client private key provided but certificate is missing. Both ssl.client.certPath and ssl.client.keyPath are required for mutual TLS.",
                        "28000");
            }
        } else if (props.containsKey(SSL_TRUSTSTORE_PATH) || caCertPath != null) {
            // Only truststore/CA cert (no client certs) = one-sided TLS
            builder.sslMode(SslMode.ONE_SIDED_TLS);

            if (props.containsKey(SSL_TRUSTSTORE_PATH)) {
                String truststorePath = takeRequired(props, SSL_TRUSTSTORE_PATH);
                builder.truststorePathValue(truststorePath);
                builder.truststorePasswordValue(
                        takeOptional(props, SSL_TRUSTSTORE_PASSWORD).orElse(null));
                builder.truststoreTypeValue(
                        takeOptional(props, SSL_TRUSTSTORE_TYPE).orElse(DEFAULT_TRUSTSTORE_TYPE));
            }

            if (caCertPath != null) {
                validatePemFile(caCertPath, "CA certificate");
                builder.caCertPathValue(caCertPath);
            }
        } else {
            // No custom configuration = default TLS
            builder.sslMode(SslMode.DEFAULT_TLS);
        }

        return builder.build();
    }

    /**
     * Returns default SSL properties.
     *
     * @return A SslProperties instance with default values
     */
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
     * SSL/TLS connection modes supported by the DataCloud JDBC driver.
     */
    @Getter
    public enum SslMode {
        DISABLED(),
        DEFAULT_TLS(),
        ONE_SIDED_TLS(),
        MUTUAL_TLS();
    }

    /**
     * Checks if client certificates are provided.
     */
    private boolean hasClientCertificates() {
        return clientCertPathValue != null && clientKeyPathValue != null;
    }

    /**
     * Creates a gRPC channel builder with appropriate SSL/TLS configuration.
     *
     * @param host The target host
     * @param port The target port
     * @return Configured ManagedChannelBuilder
     * @throws DataCloudJDBCException if channel creation fails
     */
    public ManagedChannelBuilder<?> createChannelBuilder(String host, int port) throws DataCloudJDBCException {
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
    private ManagedChannelBuilder<?> createCustomSslChannelBuilder(String host, int port)
            throws DataCloudJDBCException {
        try {
            if (hasClientCertificates()) {
                // Mutual TLS: client certificates + truststore/CA
                if (truststorePathValue != null) {
                    // JKS truststore + PEM client certs
                    TrustManagerFactory tmf = createTrustManagerFactory();
                    SslContext sslContext = GrpcSslContexts.forClient()
                            .trustManager(tmf)
                            .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                            .build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                } else if (caCertPathValue != null) {
                    // PEM CA cert + PEM client certs
                    SslContext sslContext = GrpcSslContexts.forClient()
                            .trustManager(new File(caCertPathValue))
                            .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                            .build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                } else {
                    // System truststore + PEM client certs
                    TrustManagerFactory tmf = createTrustManagerFactory();
                    SslContext sslContext = GrpcSslContexts.forClient()
                            .trustManager(tmf)
                            .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                            .build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                }
            } else {
                // One-sided TLS: only truststore/CA (no client certs)
                if (truststorePathValue != null) {
                    TrustManagerFactory tmf = createTrustManagerFactory();
                    SslContext sslContext =
                            GrpcSslContexts.forClient().trustManager(tmf).build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                } else if (caCertPathValue != null) {
                    validatePemFile(caCertPathValue, "CA certificate");
                    SslContext sslContext = GrpcSslContexts.forClient()
                            .trustManager(new File(caCertPathValue))
                            .build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                } else {
                    TrustManagerFactory tmf = createTrustManagerFactory();
                    SslContext sslContext =
                            GrpcSslContexts.forClient().trustManager(tmf).build();
                    return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
                }
            }
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create SSL configuration: " + e.getMessage(), e);
        }
    }

    /**
     * Creates appropriate TrustManagerFactory based on available trust configuration.
     */
    private TrustManagerFactory createTrustManagerFactory() throws Exception {
        if (truststorePathValue != null) {
            // JKS truststore
            return createTrustManagerFactoryFromJksTrustStore();
        }
        // System truststore (fallback)
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);
        return tmf;
    }
    /**
     * Creates TrustManagerFactory from JKS truststore.
     */
    private TrustManagerFactory createTrustManagerFactoryFromJksTrustStore() throws DataCloudJDBCException {
        try {
            KeyStore trustStore = KeyStore.getInstance(truststoreTypeValue);
            try (FileInputStream fis = new FileInputStream(truststorePathValue)) {
                char[] password = truststorePasswordValue != null ? truststorePasswordValue.toCharArray() : null;
                trustStore.load(fis, password);
            }
            return createTrustManagerFactoryFromKeyStore(trustStore);
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create trust manager from truststore: " + e.getMessage(), e);
        }
    }

    /**
     * Creates TrustManagerFactory from a KeyStore.
     * Consolidated method to avoid code duplication.
     */
    private TrustManagerFactory createTrustManagerFactoryFromKeyStore(KeyStore trustStore)
            throws DataCloudJDBCException {
        try {
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create trust manager factory: " + e.getMessage(), e);
        }
    }

    /**
     * Validates that a PEM file exists and is readable.
     */
    private static void validatePemFile(String path, String description) throws DataCloudJDBCException {
        File file = new File(path);
        if (!file.exists()) {
            throw new DataCloudJDBCException(
                    "File not found, ensure the file exists and the path is correct. description=" + description
                            + ", path=" + path);
        }
        if (!file.canRead()) {
            throw new DataCloudJDBCException(
                    "File is not readable, check file permissions. description=" + description + ", path=" + path);
        }
        if (file.length() == 0) {
            throw new DataCloudJDBCException(description + " file is empty: " + path);
        }
    }

    /**
     * Validates that all certificate files for mutual TLS exist and are readable.
     */
    private static void validateCertificateFiles(String clientCertPath, String clientKeyPath, String caCertPath)
            throws DataCloudJDBCException {
        validatePemFile(clientCertPath, "Client certificate");
        validatePemFile(clientKeyPath, "Client private key");

        // CA certificate is optional (might use JKS truststore instead)
        if (caCertPath != null) {
            validatePemFile(caCertPath, "CA certificate");
        }
        log.info("Certificate file validation completed successfully");
    }
}
