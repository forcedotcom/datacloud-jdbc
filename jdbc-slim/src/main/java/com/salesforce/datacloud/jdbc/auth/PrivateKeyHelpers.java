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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.val;

@Getter
enum Audience {
    DEV("login.test1.pc-rnd.salesforce.com"),
    PROD("login.salesforce.com");

    public final String url;

    Audience(String audience) {
        this.url = audience;
    }

    public static Audience of(String url) throws SQLException {
        if (url.contains(TEST_SUFFIX)) {
            return Audience.DEV;
        } else if (url.endsWith(PROD_SUFFIX)) {
            return Audience.PROD;
        } else {
            val errorMessage = "The specified url: '" + url + "' didn't match any known environments";
            val rootCauseException = new IllegalArgumentException(errorMessage);
            throw new DataCloudJDBCException(errorMessage, "28000", rootCauseException);
        }
    }

    private static final String PROD_SUFFIX = ".salesforce.com";
    private static final String TEST_SUFFIX = ".test1.pc-rnd.salesforce.com";
}

@UtilityClass
class JwtParts {
    public static String buildJwt(PrivateKeyAuthenticationSettings settings) throws SQLException {
        try {
            Instant now = Instant.now();
            Audience audience = Audience.of(settings.getLoginUrl());
            RSAPrivateKey privateKey = asPrivateKey(settings.getPrivateKey());
            return Jwts.builder()
                    .setIssuer(settings.getClientId())
                    .setSubject(settings.getUserName())
                    .setAudience(audience.url)
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

    private byte[] decodeBase64(String input) {
        return Base64.getDecoder().decode(input);
    }

    private static final String JWT_CREATION_FAILURE =
            "JWT assertion creation failed. Please check Username, Client Id, Private key and try again.";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
}
