/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectCdpTokenProcessor {
    static final String CDP_TOKEN_KEY = "cdpToken";
    static final String TENANT_URL_KEY = "tenantUrl";
    static final String DATASPACE_KEY = "dataspace";
    static final int DEFAULT_EXPIRES_IN = 3600;

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
            String tenantUrl = takeRequired(properties, TENANT_URL_KEY);
            String dataspace = takeOptional(properties, DATASPACE_KEY).orElse(null);
            DirectCdpTokenProcessor processor = new DirectCdpTokenProcessor(cdpToken, tenantUrl, dataspace);
            processor.validateToken();
            return processor;
        } catch (IllegalArgumentException ex) {
            throw new SQLException(ex.getMessage(), "28000", ex);
        }
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
        if (cachedDataCloudToken != null && cachedDataCloudToken.isAlive()) {
            return cachedDataCloudToken;
        }

        try {
            DataCloudToken token = buildDataCloudToken();
            cachedDataCloudToken = token;
            return token;
        } catch (Exception ex) {
            cachedDataCloudToken = null;
            throw new SQLException(ex.getMessage(), "28000", ex);
        }
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
        response.setExpiresIn(DEFAULT_EXPIRES_IN);
        return DataCloudToken.of(response);
    }
}
