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

import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.net.URI;
import java.sql.SQLException;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Value
@Builder(access = AccessLevel.PRIVATE)
public class OAuthToken {
    public static final String FAILED_LOGIN = "Failed to login. Please check credentials";
    private static final String BEARER_PREFIX = "Bearer ";

    String token;
    URI instanceUrl;

    public static OAuthToken of(OAuthTokenResponse response) throws SQLException {
        val accessToken = response.getToken();

        if (StringUtils.isBlank(accessToken)) {
            throw new DataCloudJDBCException(FAILED_LOGIN, "28000");
        }

        try {
            val instanceUrl = new URI(response.getInstanceUrl());

            return OAuthToken.builder()
                    .token(accessToken)
                    .instanceUrl(instanceUrl)
                    .build();
        } catch (Exception ex) {
            throw new DataCloudJDBCException(FAILED_LOGIN, "28000", ex);
        }
    }

    public String getBearerToken() {
        return BEARER_PREFIX + getToken();
    }
}
