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
 * Direct gRPC connection utility with automatic SSL/TLS detection.
 *
 * <p>SSL configuration is detected based on provided properties:
 * <ul>
 *   <li>No SSL properties: SSL with system truststore (default)</li>
 *   <li>Trust properties only: One-sided TLS (server authentication)</li>
 *   <li>Trust + client cert properties: Two-sided TLS (mutual authentication)</li>
 * </ul>
 *
 * <p>Supported trust verification formats:
 * <ul>
 *   <li>JKS truststore: truststore_path + truststore_password</li>
 *   <li>PEM CA certificate: ca_cert_path</li>
 *   <li>System truststore: automatic fallback</li>
 * </ul>
 *
 * <p>Supported property combinations:
 * <ul>
 *   <li>truststore_* → one-sided TLS</li>
 *   <li>ca_cert_path → one-sided TLS</li>
 *   <li>truststore_* + client_cert_* → two-sided TLS</li>
 *   <li>ca_cert_path + client_cert_* → two-sided TLS</li>
 * </ul>
 */
@Slf4j
public final class DirectDataCloudConnection {
    public static final String DIRECT = "direct";

    // property to disable SSL (for testing only, we might change the implementation in future)
    private static final String SSL_DISABLED = "ssl_disabled";

    // JKS truststore properties - for trust verification
    public static final String TRUSTSTORE_PATH = "truststore_path";
    public static final String TRUSTSTORE_PASSWORD = "truststore_password";
    public static final String TRUSTSTORE_TYPE = "truststore_type";

    // PEM certificate properties - for trust verification and client authentication
    public static final String CLIENT_CERT_PATH = "client_cert_path";
    public static final String CLIENT_KEY_PATH = "client_key_path";
    public static final String CA_CERT_PATH = "ca_cert_path";

    private DirectDataCloudConnection() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static boolean isDirect(Properties properties) {
        return getBooleanOrDefault(properties, DIRECT, false);
    }

    public static DataCloudConnection of(String url, Properties properties) throws SQLException {
        // Basic input validation - DataCloudConnectionString.of(url) provides comprehensive URL validation
        // including protocol validation, URL format checking, and parameter parsing
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
        SslMode sslMode = determineSslMode(properties);
        String endpoint = uri.getHost() + ":" + uri.getPort();

        log.info("Creating {} connection to {}", sslMode.getDescription(), endpoint);

        switch (sslMode) {
            case DISABLED:
                return createPlaintextChannel(uri);
            case SYSTEM_TRUSTSTORE:
                return createSslWithSystemTrust(uri);
            case CUSTOM_TRUST:
            case MUTUAL_TLS:
                return createCustomSslChannel(uri, properties);
            default:
                throw new IllegalStateException("Unsupported SSL mode: " + sslMode);
        }
    }

    /**
     * Determines the appropriate SSL mode based on provided properties.
     *
     * @param properties Connection properties
     * @return The SSL mode to use
     * @throws IllegalArgumentException if SSL configuration is invalid
     */
    private static SslMode determineSslMode(Properties properties) {
        // Check for explicit SSL disable (testing only)
        if (getBooleanOrDefault(properties, SSL_DISABLED, false)) {
            return SslMode.DISABLED;
        }

        boolean hasTrustConfig = hasTrustConfiguration(properties);
        boolean hasClientCert = hasClientCertificates(properties);

        if (hasClientCert) {
            return SslMode.MUTUAL_TLS;
        } else if (hasTrustConfig) {
            return SslMode.CUSTOM_TRUST;
        } else {
            return SslMode.SYSTEM_TRUSTSTORE;
        }
    }

    /**
     * SSL/TLS connection modes supported by the DataCloud JDBC driver.
     */
    private enum SslMode {
        /** SSL disabled - plaintext connection (testing only) */
        DISABLED("plaintext"),

        /** SSL with system truststore - default secure mode */
        SYSTEM_TRUSTSTORE("SSL with system truststore (one-sided TLS)"),

        /** SSL with custom trust configuration - custom CA or truststore */
        CUSTOM_TRUST("SSL with custom trust configuration (one-sided TLS)"),

        /** SSL with mutual authentication - client certificates required */
        MUTUAL_TLS("SSL with client certificates (two-sided TLS)");

        private final String description;

        SslMode(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Checks if trust configuration properties are provided.
     * Trust configuration determines how server certificates are verified.
     *
     * @param properties Connection properties to check
     * @return true if truststore or CA certificate path is provided
     */
    private static boolean hasTrustConfiguration(Properties properties) {
        return optional(properties, TRUSTSTORE_PATH).isPresent()
                || optional(properties, CA_CERT_PATH).isPresent();
    }

    /**
     * Checks if client certificate properties are provided.
     * Client certificates enable two-sided TLS (mutual authentication).
     *
     * @param properties Connection properties to check
     * @return true if both client certificate and key paths are provided
     * @throws IllegalArgumentException if client cert is provided without key or vice versa
     */
    private static boolean hasClientCertificates(Properties properties) {
        boolean hasClientCert = optional(properties, CLIENT_CERT_PATH).isPresent();
        boolean hasClientKey = optional(properties, CLIENT_KEY_PATH).isPresent();

        // Validate that both cert and key are provided together
        if (hasClientCert && !hasClientKey) {
            throw new IllegalArgumentException("Client certificate path provided without client key path. "
                    + "For mutual TLS, both '" + CLIENT_CERT_PATH + "' and '" + CLIENT_KEY_PATH + "' are required.");
        }
        if (hasClientKey && !hasClientCert) {
            throw new IllegalArgumentException("Client key path provided without client certificate path. "
                    + "For mutual TLS, both '" + CLIENT_CERT_PATH + "' and '" + CLIENT_KEY_PATH + "' are required.");
        }

        return hasClientCert && hasClientKey;
    }

    /**
     * Creates plaintext connection with no encryption.
     * Only used when SSL is explicitly disabled via internal flag.
     */
    private static ManagedChannelBuilder<?> createPlaintextChannel(URI uri) {
        log.info("Creating plaintext connection (no encryption)");
        return ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext();
    }

    /**
     * Creates SSL connection with system truststore (default behavior).
     * Uses Java's default truststore for server certificate verification.
     * No client certificate provided (one-sided TLS).
     */
    private static ManagedChannelBuilder<?> createSslWithSystemTrust(URI uri) throws Exception {
        log.info("Creating SSL connection with system truststore");

        // Use system default truststore
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // null uses system default truststore

        SslContext sslContext = GrpcSslContexts.forClient().trustManager(tmf).build();

        return NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort()).sslContext(sslContext);
    }

    /**
     * Creates SSL connection with custom configuration.
     * Supports both one-sided and two-sided TLS based on provided properties.
     * Supports all combinations of JKS/PEM trust verification with PEM client certificates.
     */
    private static ManagedChannelBuilder<?> createCustomSslChannel(URI uri, Properties properties) throws Exception {
        log.info("Creating SSL connection with custom configuration");

        SslContext sslContext;

        // Check if client certificates are provided (determines one-sided vs two-sided TLS)
        if (hasClientCertificates(properties)) {
            // Two-sided TLS: trust verification + client authentication
            sslContext = createTwoSidedTlsContext(properties);
            log.info("Client certificates configured - using two-sided TLS");
        } else {
            // One-sided TLS: trust verification only
            sslContext = createOneSidedTlsContext(properties);
            log.info("No client certificates - using one-sided TLS");
        }

        return NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort()).sslContext(sslContext);
    }

    /**
     * Creates SSL context for one-sided TLS (server authentication only).
     * Supports both JKS truststore and PEM CA certificate for trust verification.
     */
    private static SslContext createOneSidedTlsContext(Properties properties) throws Exception {
        String truststorePath = optional(properties, TRUSTSTORE_PATH).orElse(null);
        String caCertPath = optional(properties, CA_CERT_PATH).orElse(null);

        if (truststorePath != null) {
            // JKS truststore
            log.info("Using JKS truststore for server verification: {}", truststorePath);
            TrustManagerFactory tmf = createJksTrustManager(properties);
            return GrpcSslContexts.forClient().trustManager(tmf).build();
        } else if (caCertPath != null) {
            // PEM CA certificate
            log.info("Using PEM CA certificate for server verification: {}", caCertPath);
            validatePemFile(caCertPath, "CA certificate");
            return GrpcSslContexts.forClient()
                    .trustManager(new File(caCertPath))
                    .build();
        } else {
            // System truststore (fallback)
            log.info("Using system truststore for server verification");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            return GrpcSslContexts.forClient().trustManager(tmf).build();
        }
    }

    /**
     * Creates SSL context for two-sided TLS (mutual authentication).
     * Supports JKS truststore + PEM client certs OR PEM CA cert + PEM client certs.
     */
    private static SslContext createTwoSidedTlsContext(Properties properties) throws Exception {
        String clientCertPath = properties.getProperty(CLIENT_CERT_PATH);
        String clientKeyPath = properties.getProperty(CLIENT_KEY_PATH);
        String truststorePath = optional(properties, TRUSTSTORE_PATH).orElse(null);
        String caCertPath = optional(properties, CA_CERT_PATH).orElse(null);

        // Validate client certificate files
        validateCertificateFiles(clientCertPath, clientKeyPath, caCertPath);

        if (truststorePath != null) {
            // JKS truststore + PEM client certs
            log.info("Using JKS truststore + PEM client certificates for mutual TLS");
            TrustManagerFactory tmf = createJksTrustManager(properties);
            return GrpcSslContexts.forClient()
                    .trustManager(tmf)
                    .keyManager(new File(clientCertPath), new File(clientKeyPath))
                    .build();
        } else if (caCertPath != null) {
            // PEM CA cert + PEM client certs
            log.info("Using PEM CA certificate + PEM client certificates for mutual TLS");
            return GrpcSslContexts.forClient()
                    .trustManager(new File(caCertPath))
                    .keyManager(new File(clientCertPath), new File(clientKeyPath))
                    .build();
        } else {
            // System truststore + PEM client certs
            log.info("Using system truststore + PEM client certificates for mutual TLS");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            return GrpcSslContexts.forClient()
                    .trustManager(tmf)
                    .keyManager(new File(clientCertPath), new File(clientKeyPath))
                    .build();
        }
    }

    /**
     * Creates trust manager from JKS truststore.
     */
    private static TrustManagerFactory createJksTrustManager(Properties properties) throws Exception {
        String truststorePath = properties.getProperty(TRUSTSTORE_PATH);
        String truststorePassword = optional(properties, TRUSTSTORE_PASSWORD).orElse("");
        String truststoreType = optional(properties, TRUSTSTORE_TYPE).orElse("JKS");

        // Validate truststore file
        validateTruststoreFile(truststorePath);

        // Load truststore
        KeyStore trustStore = KeyStore.getInstance(truststoreType);
        try (FileInputStream fis = new FileInputStream(truststorePath)) {
            trustStore.load(fis, truststorePassword.toCharArray());
            log.debug("Successfully loaded JKS truststore with {} certificates", trustStore.size());
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf;
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

        // CA certificate is optional (might use JKS truststore instead)
        if (caCertPath != null) {
            validatePemFile(caCertPath, "CA certificate");
        }

        // TODO: Add advanced certificate validation for production use:
        //  1. Cryptographic pair validation - verify client cert and private key match
        //  2. Certificate expiration validation - check validity dates and warn about expiring certs
        //  3. Certificate chain validation - verify client cert is signed by provided CA
        //  4. Key algorithm validation - ensure supported algorithms (RSA, EC)
        //  5. Certificate purpose validation - verify cert is valid for client authentication
        //  This would catch configuration errors early with clear error messages instead of
        //  runtime SSL handshake failures. Could be enabled via 'validate_certificates' property.
        log.debug("Certificate file validation completed successfully");
    }

    private static void validatePemFile(String path, String description) throws DataCloudJDBCException {
        File file = new File(path);
        if (!file.exists()) {
            throw new DataCloudJDBCException(
                    "File not found, ensure the file exists and the path is correct. description={}, path={}");
        }
        if (!file.canRead()) {
            throw new DataCloudJDBCException("File is not readable, check file permissions. description={}, path={}");
        }
        if (file.length() == 0) {
            throw new DataCloudJDBCException(description + " file is empty: " + path);
        }

        basicPemValidation(path, description, file);
    }

    private static void basicPemValidation(String path, String description, File file) {
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
