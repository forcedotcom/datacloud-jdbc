/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Connection properties that the driver advertises via {@link java.sql.Driver#getPropertyInfo}, e.g., for DBeaver's
 * driver properties tab. The property parsing uses the names defined here, so the advertised list cannot drift.
 * None are flagged as required, since which ones are needed depends on the authentication mode.
 */
@Getter
@RequiredArgsConstructor
public enum DriverProperty {
    CLIENT_ID("clientId", "OAuth consumer key of the External Client App. Required for all authentication modes."),
    CLIENT_SECRET(
            "clientSecret",
            "OAuth consumer secret, for client credentials, password and refresh token authentication."
                    + " Must not be set for JWT (privateKey) authentication."),
    USER_NAME("userName", "Salesforce username, for JWT (privateKey) and password authentication."),
    PASSWORD("password", "Salesforce password, for password authentication."),
    PRIVATE_KEY("privateKey", "RSA private key in PEM format, for JWT authentication together with userName."),
    REFRESH_TOKEN("refreshToken", "OAuth refresh token, for refresh token authentication together with clientSecret."),
    DATASPACE("dataspace", "Name of the Data Cloud data space to query. Defaults to the default data space."),
    QUERY_TIMEOUT("queryTimeout", "Query timeout in seconds. Defaults to 0, which means no timeout.");

    private final String name;
    private final String description;
}
