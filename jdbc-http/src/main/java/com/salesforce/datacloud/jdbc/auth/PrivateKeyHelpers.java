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

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Validates authentication urls against patterns described in
 * <a href="https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_endpoints.htm&type=5">OAuth documentation</a>
 */
@Slf4j
final class Audience {
    private static final Pattern PROD = Pattern.compile("^login\\.salesforce\\.com$");
    private static final Pattern MY_DOMAIN = Pattern.compile("^.+\\.my\\.salesforce\\.com$");
    private static final Pattern EXPERIENCE = Pattern.compile("^.+\\.my\\.site\\.com$");
    private static final Pattern SANDBOX = Pattern.compile("^test\\.salesforce\\.com$");
    private static final Pattern TEST = Pattern.compile("^login\\.test\\d+\\.pc-rnd\\.salesforce\\.com$");
    private static final Pattern MY_SANDBOX = Pattern.compile("^.+--.+\\.sandbox\\.my\\.salesforce\\.com$");

    private static final List<Pattern> PATTERNS =
            ImmutableList.of(PROD, MY_DOMAIN, EXPERIENCE, SANDBOX, TEST, MY_SANDBOX);

    private Audience() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String getAudience(String url) throws SQLException {
        if (url == null) {
            throw new DataCloudJDBCException("Cannot determine audience for login url because it was null.");
        }

        final URI uri;
        try {
            uri = url.startsWith("https://") ? URI.create(url) : URI.create("https://" + url);
        } catch (NullPointerException | IllegalArgumentException ex) {
            throw new DataCloudJDBCException("The specified url was not a valid uri. url=" + url, "28000", ex);
        }

        val host = uri.getHost();

        if (PATTERNS.stream().map(Pattern::asPredicate).noneMatch(p -> p.test(host))) {
            log.warn(
                    "The specified url does not match any known hosts, but salesforce allows custom urls. host={}, url={}",
                    host,
                    url);
        }

        return host;
    }
}

final class JwtParts {
    private JwtParts() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String buildJwt(PrivateKeyAuthenticationSettings settings) throws DataCloudJDBCException {
        try {
            val now = Instant.now();
            val audience = Audience.getAudience(settings.getLoginUrl());
            val privateKey = asPrivateKey(settings.getPrivateKey());
            return Jwts.builder()
                    .setIssuer(settings.getClientId())
                    .setSubject(settings.getUserName())
                    .setAudience(audience)
                    .setIssuedAt(Date.from(now))
                    .setExpiration(Date.from(now.plus(2L, ChronoUnit.MINUTES)))
                    .signWith(privateKey, SignatureAlgorithm.RS256)
                    .compact();
        } catch (Exception ex) {
            throw new DataCloudJDBCException(JWT_CREATION_FAILURE, "28000", ex);
        }
    }

    private static RSAPrivateKey asPrivateKey(String privateKey) throws SQLException {
        String rsaPrivateKey = privateKey
                .replaceFirst(BEGIN_PRIVATE_KEY, "")
                .replaceFirst(END_PRIVATE_KEY, "")
                .replaceAll("\\s", "");

        val bytes = decodeBase64(rsaPrivateKey);

        try {
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
            val factory = KeyFactory.getInstance("RSA");
            return (RSAPrivateKey) factory.generatePrivate(keySpec);

        } catch (Exception ex) {
            throw new DataCloudJDBCException(JWT_CREATION_FAILURE, "28000", ex);
        }
    }

    private static byte[] decodeBase64(String input) {
        return Base64.getDecoder().decode(input);
    }

    private static final String JWT_CREATION_FAILURE =
            "JWT assertion creation failed. Please check Username, Client Id, Private key and try again.";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
}
