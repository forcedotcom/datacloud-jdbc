/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import java.sql.SQLException;

public interface TokenProcessor {
    AuthenticationSettings getSettings();

    OAuthToken getOAuthToken() throws SQLException;

    DataCloudToken getDataCloudToken() throws SQLException;

    String getLakehouse() throws SQLException;
}
