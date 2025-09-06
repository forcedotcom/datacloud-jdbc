/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import java.net.URI;
import java.sql.SQLException;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;
import lombok.val;

@Value
@Builder(access = AccessLevel.PRIVATE)
public class OAuthToken {
    public static final String FAILED_LOGIN = "Failed to login. Please check credentials";
    private static final String BEARER_PREFIX = "Bearer ";

    String token;
    URI instanceUrl;

    public static OAuthToken of(OAuthTokenResponse response) throws SQLException {
        val accessToken = response.getToken();

        if (StringCompatibility.isNullOrBlank(accessToken)) {
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
