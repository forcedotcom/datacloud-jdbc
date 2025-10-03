/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parse JDBC URLs into host, port, path, and parameters.
 *
 * You can't directly use {@link java.net.URL} because it doesn't support the JDBC URL format
 * and will choke on `jdbc:salesforce-datacloud:...` URLs due to the two `:`s.
 */
@Builder(access = AccessLevel.PRIVATE)
public class JdbcURL {
    /// The URL without the query string (`?param=value`).
    /// Returned by DatabaseMetadata.getURL().
    @Getter
    private final String urlWithoutQuery;

    @Getter
    private final String host;

    @Getter
    private final int port;

    @Getter
    private final Map<String, String> parameters;

    public static JdbcURL of(@NonNull String url) throws DataCloudJDBCException {
        // Strip the trailing `jdbc:`
        if (!url.startsWith("jdbc:")) {
            throw new IllegalArgumentException("All JDBC URLs must start with 'jdbc:'");
        }
        url = url.substring("jdbc:".length());

        // Now we can use the standard URL parser
        URI uriObj;
        String urlWithoutQuery;
        try {
            uriObj = new URI(url);
            urlWithoutQuery = "jdbc:"
                    + (new URI(uriObj.getScheme(), uriObj.getAuthority(), uriObj.getPath(), null, null).toString());
        } catch (URISyntaxException e) {
            throw new DataCloudJDBCException("Invalid URI syntax: " + e.getReason(), e);
        }

        if (!uriObj.getPath().equals("")) {
            throw new DataCloudJDBCException("JDBC URLs must not contain a path");
        }
        if (uriObj.getFragment() != null) {
            throw new DataCloudJDBCException("JDBC URLs must not contain a fragment");
        }
        if (uriObj.getUserInfo() != null) {
            throw new DataCloudJDBCException("JDBC URLs must not contain a user info");
        }
        if (uriObj.getHost() == null || uriObj.getHost().isEmpty()) {
            throw new DataCloudJDBCException("JDBC URLs must contain a host");
        }

        return JdbcURL.builder()
                .urlWithoutQuery(urlWithoutQuery)
                .host(uriObj.getHost())
                .port(uriObj.getPort())
                .parameters(parseQueryString(uriObj.getQuery()))
                .build();
    }

    /**
     * Parse query string `key=value&key2=value2` into parameters, following Javascript's URLSearchParams behavior.
     * Handles URL decoding, empty values, and throws SqlException for duplicate keys.
     */
    private static Map<String, String> parseQueryString(String query) throws DataCloudJDBCException {
        Map<String, String> parameters = new HashMap<>();

        if (query == null || query.isEmpty()) {
            return parameters;
        }

        String[] pairs = query.split("&");
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                // It is valid to have empty pairs, i.e. `a=1&&b=2` is valid.
                // This is in sync with Javascript's URLSearchParams.
                continue;
            }

            // Noteworthy edge cases:
            // * `a=b=c&d=e` is valid, and we will decode it to a="b=c" and d="e". This is in sync with Javascript's
            // URLSearchParams.
            // * `a&b=c` is invalid - parameters must have values. This is different from Javascript's URLSearchParams.
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 1) {
                throw new DataCloudJDBCException("Parameter without value in JDBC URL: " + keyValue[0], "HY000");
            }

            String key, value;
            try {
                key = URLDecoder.decode(keyValue[0], "UTF-8");
                value = URLDecoder.decode(keyValue[1], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCloudJDBCException("Unsupported encoding: UTF-8", e);
            }

            if (parameters.containsKey(key)) {
                throw new DataCloudJDBCException("Duplicate parameter key in JDBC URL: " + key, "HY000");
            }

            parameters.put(key, value);
        }

        return parameters;
    }

    /*
     * Add the URL parameters to the properties.
     */
    public void addParametersToProperties(Properties properties) throws DataCloudJDBCException {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            if (properties.containsKey(entry.getKey())) {
                throw new DataCloudJDBCException(
                        "Parameter `" + entry.getKey() + "` is set both in the URL and the properties", "HY000");
            }
            properties.put(entry.getKey(), entry.getValue());
        }
    }
}
