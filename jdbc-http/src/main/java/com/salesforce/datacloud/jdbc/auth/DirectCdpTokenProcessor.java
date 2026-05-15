/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectCdpTokenProcessor {
    static final String CDP_TOKEN_KEY = "cdpToken";
    static final String TENANT_URL_KEY = "tenantUrl";
    static final String DATASPACE_KEY = "dataspace";
    static final int FALLBACK_EXPIRES_IN_SECONDS = 3600;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String cdpToken;
    private final String tenantUrl;
    private final String dataspace;
    private DataCloudToken cachedDataCloudToken;

    private DirectCdpTokenProcessor(String cdpToken, String tenantUrl, String dataspace) {
        this.cdpToken = cdpToken;
        this.tenantUrl = tenantUrl;
        this.dataspace = dataspace;
    }

    public static boolean hasCdpToken(Properties properties) {
        return properties != null && properties.containsKey(CDP_TOKEN_KEY) && properties.containsKey(TENANT_URL_KEY);
    }

    public static DirectCdpTokenProcessor ofDestructive(Properties properties) throws SQLException {
        try {
            String cdpToken = takeRequired(properties, CDP_TOKEN_KEY);
            String tenantUrl = validateTenantHost(takeRequired(properties, TENANT_URL_KEY));
            String dataspace = takeOptional(properties, DATASPACE_KEY).orElse(null);
            DirectCdpTokenProcessor processor = new DirectCdpTokenProcessor(cdpToken, tenantUrl, dataspace);
            processor.validateToken();
            return processor;
        } catch (IllegalArgumentException ex) {
            throw new SQLException(ex.getMessage(), "28000", ex);
        }
    }

    /**
     * tenantUrl must be a bare hostname — gRPC's {@code ManagedChannelBuilder.forAddress} requires it.
     * Reject schemes, ports, paths, and whitespace so users get a clear error rather than a confusing
     * connection failure later.
     */
    static String validateTenantHost(String tenantUrl) {
        if (tenantUrl.contains("://") || tenantUrl.contains("/") || tenantUrl.contains(":")) {
            throw new IllegalArgumentException(
                    "tenantUrl must be a bare hostname (e.g. 'tenant.c360a.salesforce.com'), got: '" + tenantUrl + "'");
        }
        String trimmed = tenantUrl.trim();
        if (trimmed.isEmpty() || trimmed.length() != tenantUrl.length()) {
            throw new IllegalArgumentException(
                    "tenantUrl must be a non-empty hostname with no whitespace, got: '" + tenantUrl + "'");
        }
        return trimmed;
    }

    private void validateToken() throws SQLException {
        try {
            DataCloudToken token = buildDataCloudToken();
            token.getTenantId();
            cachedDataCloudToken = token;
        } catch (Exception ex) {
            throw new SQLException(
                    "Invalid CDP token: unable to parse JWT or extract tenant ID. " + ex.getMessage(), "28000", ex);
        }
    }

    public DataCloudToken getDataCloudToken() throws SQLException {
        if (cachedDataCloudToken == null || !cachedDataCloudToken.isAlive()) {
            cachedDataCloudToken = buildDataCloudToken();
        }
        return cachedDataCloudToken;
    }

    public String getLakehouse() throws SQLException {
        String tenantId = getDataCloudToken().getTenantId();
        String response =
                "lakehouse:" + tenantId + ";" + Optional.ofNullable(dataspace).orElse("");
        log.info("Lakehouse: {}", response);
        return response;
    }

    private DataCloudToken buildDataCloudToken() throws SQLException {
        DataCloudTokenResponse response = new DataCloudTokenResponse();
        response.setToken(cdpToken);
        response.setInstanceUrl(tenantUrl);
        response.setTokenType("Bearer");
        response.setExpiresIn(secondsUntilJwtExpiry(cdpToken));
        return DataCloudToken.of(response);
    }

    /**
     * Returns seconds remaining until the JWT's `exp` claim, clamped to a minimum of 0.
     * If the JWT cannot be parsed or has no `exp` claim, falls back to {@link #FALLBACK_EXPIRES_IN_SECONDS}
     * so callers behave identically to the previous fixed-TTL behavior.
     */
    static int secondsUntilJwtExpiry(String jwt) {
        try {
            String[] chunks = jwt.split("\\.", -1);
            if (chunks.length < 2) {
                return FALLBACK_EXPIRES_IN_SECONDS;
            }
            byte[] decoded = Base64.getUrlDecoder().decode(chunks[1]);
            JsonNode payload = JSON.readTree(decoded);
            JsonNode exp = payload.get("exp");
            if (exp == null || !exp.canConvertToLong()) {
                return FALLBACK_EXPIRES_IN_SECONDS;
            }
            long remaining = exp.asLong() - Instant.now().getEpochSecond();
            return remaining > 0 ? (int) Math.min(remaining, Integer.MAX_VALUE) : 0;
        } catch (Exception ex) {
            log.debug("Could not derive exp from CDP token, using default TTL", ex);
            return FALLBACK_EXPIRES_IN_SECONDS;
        }
    }
}
