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

import static com.salesforce.datacloud.jdbc.util.Require.requireNotNullOrBlank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Messages;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Calendar;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DataCloudToken {
    private static final int JWT_PAYLOAD_INDEX = 1;
    private static final String JWT_DELIMITER = "\\.";
    private static final String AUDIENCE_TENANT_ID = "audienceTenantId";

    private final String type;
    private final String token;
    private final URI tenant;
    private final Calendar expiresIn;

    private static final String TENANT_IO_ERROR_RESPONSE = "Error while decoding tenantId.";

    public static DataCloudToken of(DataCloudTokenResponse model) throws SQLException {
        val type = model.getTokenType();
        val token = model.getToken();
        val tenantUrl = model.getInstanceUrl();

        requireNotNullOrBlank(type, "token_type");
        requireNotNullOrBlank(token, "access_token");
        requireNotNullOrBlank(tenantUrl, "instance_url");

        val expiresIn = Calendar.getInstance();
        expiresIn.add(Calendar.SECOND, model.getExpiresIn());

        try {
            val tenant = URI.create(tenantUrl);

            return new DataCloudToken(type, token, tenant, expiresIn);
        } catch (IllegalArgumentException ex) {
            val rootCauseException = new IllegalArgumentException(
                    "Failed to parse the provided tenantUrl: '" + tenantUrl + "'. " + ex.getMessage(), ex.getCause());
            throw new DataCloudJDBCException(Messages.FAILED_LOGIN, "28000", rootCauseException);
        }
    }

    public boolean isAlive() {
        val now = Calendar.getInstance();
        return now.compareTo(expiresIn) <= 0;
    }

    public String getTenantUrl() {
        return this.tenant.toString();
    }

    public String getTenantId() throws SQLException {
        return getTenantId(this.token);
    }

    public String getAccessToken() {
        return this.type + StringUtils.SPACE + this.token;
    }

    private static String getTenantId(String token) throws SQLException {
        String[] chunks = token.split(JWT_DELIMITER, -1);
        Base64.Decoder decoder = Base64.getUrlDecoder();
        try {
            val chunk = chunks[JWT_PAYLOAD_INDEX];
            val decodedChunk = decoder.decode(chunk);

            return new ObjectMapper()
                    .readTree(decodedChunk)
                    .get(AUDIENCE_TENANT_ID)
                    .asText();
        } catch (IOException e) {
            throw new DataCloudJDBCException(TENANT_IO_ERROR_RESPONSE, "58030", e);
        }
    }
}
