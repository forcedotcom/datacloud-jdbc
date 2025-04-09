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
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

@Builder(access = AccessLevel.PRIVATE)
public class DataCloudConnectionString {
    public static final String ILLEGAL_CONNECTION_PROTOCOL =
            "URL is specified with invalid datasource, expected jdbc:salesforce-datacloud";

    public static final String CONNECTION_PROTOCOL = "jdbc:salesforce-datacloud:";

    public static DataCloudConnectionString of(String url) throws SQLException {
        if (!acceptsUrl(url)) {
            throw new DataCloudJDBCException(ILLEGAL_CONNECTION_PROTOCOL);
        }

        val database = getDatabaseUrl(url);
        val login = getAuthenticationUrl(url);
        val parameters = parseParameters(url);

        return DataCloudConnectionString.builder()
                .databaseUrl(database)
                .loginUrl(login)
                .parameters(parameters)
                .build();
    }

    @Getter
    private final String databaseUrl;

    @Getter
    private final String loginUrl;

    private final Map<String, String> parameters;

    public static boolean acceptsUrl(String url) {
        return url != null && url.startsWith(CONNECTION_PROTOCOL) && urlDoesNotContainScheme(url);
    }

    private static boolean urlDoesNotContainScheme(String url) {
        val suffix = url.substring(CONNECTION_PROTOCOL.length());
        return !suffix.startsWith("http://") && !suffix.startsWith("https://");
    }

    /**
     * Returns the extracted service url from given jdbc endpoint
     *
     * @param url of the form jdbc:salesforce-datacloud://login.salesforce.com
     * @return service url
     * @throws SQLException when given url doesn't belong with required datasource
     */
    static String getAuthenticationUrl(String url) throws SQLException {
        if (!acceptsUrl(url)) {
            throw new DataCloudJDBCException(ILLEGAL_CONNECTION_PROTOCOL);
        }

        val withoutParameters = withoutParameters(url);
        val serviceRootUrl = withoutParameters.substring(CONNECTION_PROTOCOL.length());
        val noTrailingSlash = StringUtils.removeEnd(serviceRootUrl, "/");
        val host = StringUtils.removeStart(noTrailingSlash, "//");

        return StringCompatibility.isBlank(host) ? host : createURI(host).toString();
    }

    static String getDatabaseUrl(String url) throws SQLException {
        if (!acceptsUrl(url)) {
            throw new DataCloudJDBCException(ILLEGAL_CONNECTION_PROTOCOL);
        }

        return withoutParameters(url);
    }

    private static String withoutParameters(String url) {
        return url.split(";")[0];
    }

    private static Map<String, String> parseParameters(String connectionString) {
        val parts = connectionString.split(";");

        return Arrays.stream(parts)
                .skip(1)
                .filter(pair -> pair.contains("="))
                .map(pair -> {
                    val split = pair.split("=");
                    return split.length != 2 ? null : split;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(p -> p[0], p -> p[1]));
    }

    private static URI createURI(String host) throws SQLException {
        try {
            return URI.create("https://" + host);
        } catch (IllegalArgumentException e) {
            throw new DataCloudJDBCException(ILLEGAL_CONNECTION_PROTOCOL, e);
        }
    }

    public void withParameters(Properties properties) {
        properties.putAll(parameters);
    }
}
