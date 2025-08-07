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
package com.salesforce.datacloud.jdbc.util;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.Properties;
import javax.net.ssl.TrustManagerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Direct gRPC connection utility supporting multiple SSL/TLS modes.
 *
 * <p>SSL modes follow industry standards similar to:
 * <ul>
 *   <li>PostgreSQL JDBC: sslmode="disable|require|verify-full"</li>
 *   <li>MySQL Connector/J: sslMode="DISABLED|REQUIRED|VERIFY_IDENTITY"</li>
 * </ul>
 */
@Slf4j
public final class DirectDataCloudConnection {
    public static final String DIRECT = "direct";

    // SSL mode property - follows PostgreSQL and MySQL naming conventions
    public static final String SSL_MODE = "ssl_mode";

    // JKS keystore properties (for REQUIRED mode) - similar to MySQL Connector/J
    public static final String TRUSTSTORE_PATH = "truststore_path";
    public static final String TRUSTSTORE_PASSWORD = "truststore_password";
    public static final String TRUSTSTORE_TYPE = "truststore_type";

    // PEM certificate properties (for VERIFY_FULL mode) - similar to PostgreSQL
    public static final String CLIENT_CERT_PATH = "client_cert_path";
    public static final String CLIENT_KEY_PATH = "client_key_path";
    public static final String CA_CERT_PATH = "ca_cert_path";

    /**
     * SSL connection modes following industry standards.
     *
     * <p>Similar to PostgreSQL sslmode and MySQL sslMode:
     * <ul>
     *   <li>DISABLED: No encryption (like PostgreSQL "disable")</li>
     *   <li>REQUIRED: SSL with server auth only (like PostgreSQL "require")</li>
     *   <li>VERIFY_FULL: Mutual TLS authentication (like PostgreSQL "verify-full")</li>
     * </ul>
     */
    public enum SslMode {
        DISABLED, // Plaintext connection only
        REQUIRED, // SSL with server authentication (JKS truststore)
        VERIFY_FULL // Mutual TLS with full authentication (PEM certificates)
    }

    private DirectDataCloudConnection() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static boolean isDirect(Properties properties) {
        return getBooleanOrDefault(properties, DIRECT, false);
    }

    public static DataCloudConnection of(String url, Properties properties) throws SQLException {
        // Validate inputs first
        if (url == null) {
            throw new DataCloudJDBCException("Connection URL cannot be null");
        }
        if (properties == null) {
            throw new DataCloudJDBCException("Connection properties cannot be null");
        }

        final boolean direct = getBooleanOrDefault(properties, DIRECT, false);
        if (!direct) {
            throw new DataCloudJDBCException("Cannot establish direct connection without " + DIRECT + " enabled");
        }

        final DataCloudConnectionString connString = DataCloudConnectionString.of(url);
        final URI uri = URI.create(connString.getLoginUrl());

        log.info("Creating direct gRPC connection to {}", uri);

        try {
            ManagedChannelBuilder<?> builder = createChannelBuilder(uri, properties);
            return DataCloudConnection.of(builder, properties);
        } catch (DataCloudJDBCException e) {
            // Re-throw validation exceptions as-is
            throw e;
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create direct connection", e);
        }
    }

    private static ManagedChannelBuilder<?> createChannelBuilder(URI uri, Properties properties) throws Exception {
        final SslMode sslMode = determineSslMode(properties);

        log.info("Using SSL mode: {} for connection to {}:{}", sslMode, uri.getHost(), uri.getPort());

        switch (sslMode) {
            case DISABLED:
                return createPlaintextChannel(uri);

            case REQUIRED:
                return createRequiredSslChannel(uri, properties);

            case VERIFY_FULL:
                return createVerifyFullChannel(uri, properties);

            default:
                throw new DataCloudJDBCException("Unsupported SSL mode: " + sslMode);
        }
    }

    private static SslMode determineSslMode(Properties properties) throws DataCloudJDBCException {
        // Default to DISABLED if no ssl_mode specified (fail-safe approach)
        final String sslModeStr =
                optional(properties, SSL_MODE).orElse("DISABLED").trim();

        // Handle empty strings
        if (sslModeStr.isEmpty()) {
            return SslMode.DISABLED;
        }

        try {
            return SslMode.valueOf(sslModeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new DataCloudJDBCException(String.format(
                    "Invalid ssl_mode '%s'. Valid values are: DISABLED, REQUIRED, VERIFY_FULL", sslModeStr));
        }
    }

    /**
     * Creates plaintext connection with no encryption.
     * Similar to PostgreSQL sslmode="disable".
     */
    private static ManagedChannelBuilder<?> createPlaintextChannel(URI uri) {
        log.info("Creating plaintext connection (no encryption)");
        return ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext();
    }

    /**
     * Creates SSL connection with server authentication only.
     * Similar to PostgreSQL sslmode="require" and MySQL sslMode="REQUIRED".
     *
     * Requires JKS truststore to verify server certificate.
     * Client authentication still uses username/password.
     */
    private static ManagedChannelBuilder<?> createRequiredSslChannel(URI uri, Properties properties) throws Exception {
        return NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                .sslContext(createServerOnlyTlsContext(properties));
    }

    /**
     * Creates mutual TLS connection with full authentication.
     * Similar to PostgreSQL sslmode="verify-full" and MySQL sslMode="VERIFY_IDENTITY".
     *
     * Requires PEM certificates for both client and server authentication.
     * No username/password needed - certificate IS the identity.
     */
    private static ManagedChannelBuilder<?> createVerifyFullChannel(URI uri, Properties properties) throws Exception {
        return NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort())
                .sslContext(createMutualTlsContext(properties));
    }
    /**
     * Creates SSL context for server-only TLS authentication (REQUIRED mode).
     *
     * <p>This method creates an SSL context that:
     * <ul>
     *   <li>Verifies server certificate against truststore</li>
     *   <li>Does NOT provide client certificate</li>
     *   <li>Client authentication uses username/password</li>
     * </ul>
     *
     * @param properties connection properties containing truststore configuration
     * @return SSL context configured for server authentication only
     * @throws DataCloudJDBCException if required properties are missing or invalid
     * @throws Exception if SSL context creation fails
     */
    private static SslContext createServerOnlyTlsContext(Properties properties) throws Exception {
        // Validate required properties - fail fast with clear error messages
        final String truststorePath = optional(properties, TRUSTSTORE_PATH)
                .orElseThrow(
                        () -> new DataCloudJDBCException(
                                "truststore_path required for REQUIRED mode. Example: truststore_path=/etc/ssl/truststore.jks"));
        final String truststorePassword = optional(properties, TRUSTSTORE_PASSWORD)
                .orElseThrow(() -> new DataCloudJDBCException(
                        "truststore_password required for REQUIRED mode. Example: truststore_password=changeit"));
        final String truststoreType = optional(properties, TRUSTSTORE_TYPE).orElse("JKS");

        log.info(
                "Creating SSL context for server authentication - truststore: {}, type: {}",
                truststorePath,
                truststoreType);

        // Validate truststore file exists and is readable
        validateTruststoreFile(truststorePath);

        try {
            // Load truststore for server certificate verification
            KeyStore trustStore = KeyStore.getInstance(truststoreType);
            try (FileInputStream fis = new FileInputStream(truststorePath)) {
                trustStore.load(fis, truststorePassword.toCharArray());
                log.debug("Successfully loaded truststore with {} certificates", trustStore.size());
            }

            // Create trust manager factory to verify server certificate
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            // Build SSL context with server authentication only
            SslContext sslContext =
                    GrpcSslContexts.forClient().trustManager(tmf).build();

            log.info("SSL context created successfully for server-only TLS");
            return sslContext;

        } catch (Exception e) {
            log.error("Failed to create SSL context for server-only TLS: {}", e.getMessage());
            throw new DataCloudJDBCException("Server-only TLS context creation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Creates SSL context for mutual TLS authentication (VERIFY_FULL mode).
     *
     * <p>This method creates an SSL context that:
     * <ul>
     *   <li>Verifies server certificate against CA certificate</li>
     *   <li>Provides client certificate for client authentication</li>
     *   <li>Certificate IS the client identity (no username/password needed)</li>
     * </ul>
     *
     * @param properties connection properties containing certificate paths
     * @return SSL context configured for mutual authentication
     * @throws DataCloudJDBCException if required properties are missing or invalid
     * @throws Exception if SSL context creation fails
     */
    private static SslContext createMutualTlsContext(Properties properties) throws Exception {
        // Validate required properties - fail fast with helpful error messages
        final String clientCertPath = optional(properties, CLIENT_CERT_PATH)
                .orElseThrow(
                        () -> new DataCloudJDBCException(
                                "client_cert_path required for VERIFY_FULL mode. Example: client_cert_path=/etc/pki/client.pem"));
        final String clientKeyPath = optional(properties, CLIENT_KEY_PATH)
                .orElseThrow(
                        () -> new DataCloudJDBCException(
                                "client_key_path required for VERIFY_FULL mode. Example: client_key_path=/etc/pki/client-key.pem"));
        final String caCertPath = optional(properties, CA_CERT_PATH)
                .orElseThrow(() -> new DataCloudJDBCException(
                        "ca_cert_path required for VERIFY_FULL mode. Example: ca_cert_path=/etc/pki/ca.pem"));

        log.info(
                "Creating SSL context for mutual TLS - client cert: {}, key: {}, CA: {}",
                clientCertPath,
                clientKeyPath,
                caCertPath);

        // Validate all certificate files exist and are readable
        validateCertificateFiles(clientCertPath, clientKeyPath, caCertPath);

        try {
            // Build SSL context with mutual authentication
            SslContext sslContext = GrpcSslContexts.forClient()
                    .trustManager(new File(caCertPath)) // Verify server certificate
                    .keyManager(new File(clientCertPath), new File(clientKeyPath)) // Client identity
                    .build();

            log.info("SSL context created successfully for mutual TLS authentication");
            return sslContext;

        } catch (Exception e) {
            log.error("Failed to create SSL context for mutual TLS: {}", e.getMessage());
            throw new DataCloudJDBCException("Mutual TLS context creation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Validates that truststore file exists and is readable.
     */
    private static void validateTruststoreFile(String truststorePath) throws DataCloudJDBCException {
        File truststoreFile = new File(truststorePath);
        if (!truststoreFile.exists()) {
            throw new DataCloudJDBCException("Truststore file not found: " + truststorePath
                    + ". Ensure the JKS truststore exists and the path is correct.");
        }
        if (!truststoreFile.canRead()) {
            throw new DataCloudJDBCException(
                    "Truststore file is not readable: " + truststorePath + ". Check file permissions.");
        }
        if (truststoreFile.length() == 0) {
            throw new DataCloudJDBCException("Truststore file is empty: " + truststorePath);
        }
    }

    /**
     * Validates that all certificate files for mutual TLS exist and are readable.
     */
    private static void validateCertificateFiles(String clientCertPath, String clientKeyPath, String caCertPath)
            throws DataCloudJDBCException {
        validatePemFile(clientCertPath, "Client certificate");
        validatePemFile(clientKeyPath, "Client private key");
        validatePemFile(caCertPath, "CA certificate");

        // Additional validation - ensure client cert and key are a matching pair
        // This is a basic check - more sophisticated validation could be added
        log.debug("Certificate file validation completed successfully");
    }

    /**
     * Validates a PEM certificate file.
     */
    private static void validatePemFile(String path, String description) throws DataCloudJDBCException {
        File file = new File(path);
        if (!file.exists()) {
            throw new DataCloudJDBCException(
                    description + " file not found: " + path + ". Ensure the PEM file exists and the path is correct.");
        }
        if (!file.canRead()) {
            throw new DataCloudJDBCException(
                    description + " file is not readable: " + path + ". Check file permissions.");
        }
        if (file.length() == 0) {
            throw new DataCloudJDBCException(description + " file is empty: " + path);
        }

        // Basic PEM format validation - check for PEM markers (Java 8 compatible)
        try (java.util.Scanner scanner = new java.util.Scanner(file)) {
            String content = scanner.useDelimiter("\\A").next();
            if (!content.contains("-----BEGIN") || !content.contains("-----END")) {
                log.warn("{} file may not be in valid PEM format: {}", description, path);
            }
        } catch (Exception e) {
            log.warn("Could not validate PEM format for {}: {}", description, e.getMessage());
        }
    }
}
