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
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.required;

import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DirectCdpTokenProcessor implements TokenProcessor {
    static final String CDP_TOKEN_KEY = "cdpToken";
    static final String TENANT_URL_KEY = "tenantUrl";
    static final String DATASPACE_KEY = "dataspace";
    static final int DEFAULT_EXPIRES_IN = 3600;

    private final String cdpToken;
    private final String tenantUrl;
    private final String dataspace;
    private final TokenCache cache;

    public static boolean hasCdpToken(Properties properties) {
        return optional(properties, CDP_TOKEN_KEY).isPresent()
                && optional(properties, TENANT_URL_KEY).isPresent();
    }

    public static DirectCdpTokenProcessor of(Properties properties) throws DataCloudJDBCException {
        try {
            val cdpToken = required(properties, CDP_TOKEN_KEY);
            val tenantUrl = required(properties, TENANT_URL_KEY);
            val dataspace = optional(properties, DATASPACE_KEY).orElse(null);
            val processor = new DirectCdpTokenProcessor(cdpToken, tenantUrl, dataspace, new TokenCacheImpl());
            processor.validateToken();
            return processor;
        } catch (IllegalArgumentException ex) {
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    private void validateToken() throws DataCloudJDBCException {
        try {
            val token = buildDataCloudToken();
            token.getTenantId();
            cache.setDataCloudToken(token);
        } catch (Exception ex) {
            throw new DataCloudJDBCException(
                    "Invalid CDP token: unable to parse JWT or extract tenant ID. " + ex.getMessage(), "28000", ex);
        }
    }

    @Override
    public AuthenticationSettings getSettings() {
        return null;
    }

    @Override
    public OAuthToken getOAuthToken() throws SQLException {
        throw new DataCloudJDBCException(
                "OAuth token is not available when using direct CDP token authentication", "28000");
    }

    @Override
    public DataCloudToken getDataCloudToken() throws SQLException {
        val cachedToken = cache.getDataCloudToken();
        if (cachedToken != null && cachedToken.isAlive()) {
            return cachedToken;
        }

        try {
            val token = buildDataCloudToken();
            cache.setDataCloudToken(token);
            return token;
        } catch (Exception ex) {
            cache.clearDataCloudToken();
            throw new DataCloudJDBCException(ex.getMessage(), "28000", ex);
        }
    }

    @Override
    public String getLakehouse() throws SQLException {
        val tenantId = getDataCloudToken().getTenantId();
        val response = "lakehouse:" + tenantId + ";" + Optional.ofNullable(dataspace).orElse("");
        log.info("Lakehouse: {}", response);
        return response;
    }

    private DataCloudToken buildDataCloudToken() throws SQLException {
        val response = new DataCloudTokenResponse();
        response.setToken(cdpToken);
        response.setInstanceUrl(tenantUrl);
        response.setTokenType("Bearer");
        response.setExpiresIn(DEFAULT_EXPIRES_IN);
        return DataCloudToken.of(response);
    }
}
