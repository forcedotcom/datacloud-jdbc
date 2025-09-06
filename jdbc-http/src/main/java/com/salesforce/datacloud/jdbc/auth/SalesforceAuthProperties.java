/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.net.URI;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Authentication properties that control the Salesforce authentication behavior.
 *
 * Configures authentication mode, credentials, and connection settings.
 *
 * To create a SalesforceAuthProperties instance, in a type-safe way, use
 * the `builderUsingPassword`, `builderUsingPrivateKey`, or `builderUsingRefreshToken`.
 *
 * Accepts the following properties:
 * - userName: Username for password/private key authentication
 * - password: Password for password authentication
 * - privateKey: Private key for JWT authentication
 * - clientSecret: OAuth client secret (required)
 * - clientId: OAuth client ID (required)
 * - dataspace: Data space identifier, default is null
 * - refreshToken: Refresh token for token-based authentication
 */
@Slf4j
@Getter
@Builder(access = AccessLevel.PRIVATE)
public class SalesforceAuthProperties {
    public enum AuthenticationMode {
        PASSWORD,
        PRIVATE_KEY,
        REFRESH_TOKEN
    }

    static final String AUTH_USER_NAME = "userName";
    static final String AUTH_PASSWORD = "password";
    static final String AUTH_PRIVATE_KEY = "privateKey";
    static final String AUTH_CLIENT_ID = "clientId";
    static final String AUTH_CLIENT_SECRET = "clientSecret";
    static final String AUTH_REFRESH_TOKEN = "refreshToken";
    static final String AUTH_DATASPACE = "dataspace";

    // Required fields
    private final URI loginUrl;

    @Builder.Default
    private final AuthenticationMode authenticationMode = AuthenticationMode.PASSWORD;

    private final String clientId;
    private final String clientSecret;

    // For `AuthenticationMode.PASSWORD` and `AuthenticationMode.PRIVATE_KEY`
    private final String userName;

    // For `AuthenticationMode.PASSWORD`
    private final String password;

    // For `AuthenticationMode.PRIVATE_KEY`
    private final RSAPrivateKey privateKey;

    // For `AuthenticationMode.REFRESH_TOKEN`
    private final String refreshToken;

    // Optional fields
    @Builder.Default
    private final String dataspace = null;

    public static SalesforceAuthPropertiesBuilder builderUsingPassword(URI loginUrl, String userName, String password) {
        if (!isKnownLoginUrl(loginUrl.getHost())) {
            log.warn("The specified url `{}` does not match any known Salesforce hosts.", loginUrl);
        }
        return builder()
                .loginUrl(loginUrl)
                .authenticationMode(AuthenticationMode.PASSWORD)
                .userName(userName)
                .password(password);
    }

    public static SalesforceAuthPropertiesBuilder builderUsingPrivateKey(
            URI loginUrl, String userName, RSAPrivateKey privateKey) {
        if (!isKnownLoginUrl(loginUrl.getHost())) {
            log.warn("The specified url `{}` does not match any known Salesforce hosts.", loginUrl);
        }
        return builder()
                .loginUrl(loginUrl)
                .authenticationMode(AuthenticationMode.PRIVATE_KEY)
                .userName(userName)
                .privateKey(privateKey);
    }

    public static SalesforceAuthPropertiesBuilder builderUsingRefreshToken(URI loginUrl, String refreshToken) {
        if (!isKnownLoginUrl(loginUrl.getHost())) {
            log.warn("The specified url `{}` does not match any known Salesforce hosts.", loginUrl);
        }
        return builder()
                .loginUrl(loginUrl)
                .authenticationMode(AuthenticationMode.REFRESH_TOKEN)
                .refreshToken(refreshToken);
    }

    /**
     * Parses authentication properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A SalesforceAuthProperties instance
     */
    public static SalesforceAuthProperties ofDestructive(URI loginUrl, Properties props) throws DataCloudJDBCException {
        if (!isKnownLoginUrl(loginUrl.getHost())) {
            log.warn("The specified url `{}` does not match any known Salesforce hosts.", loginUrl);
        }

        SalesforceAuthPropertiesBuilder builder = SalesforceAuthProperties.builder();
        builder.loginUrl(loginUrl);

        // Required fields
        builder.clientId(takeRequired(props, AUTH_CLIENT_ID));
        builder.clientSecret(takeRequired(props, AUTH_CLIENT_SECRET));

        // Optional fields
        builder.dataspace(takeOptional(props, AUTH_DATASPACE).orElse(null));

        // Determine authentication mode and set credentials
        if (props.containsKey(AUTH_USER_NAME) && props.containsKey(AUTH_PASSWORD)) {
            builder.authenticationMode(AuthenticationMode.PASSWORD)
                    .userName(takeRequired(props, AUTH_USER_NAME))
                    .password(takeRequired(props, AUTH_PASSWORD));
        } else if (props.containsKey(AUTH_USER_NAME) && props.containsKey(AUTH_PRIVATE_KEY)) {
            builder.authenticationMode(AuthenticationMode.PRIVATE_KEY);
            builder.userName(takeRequired(props, AUTH_USER_NAME));
            builder.privateKey(parsePrivateKey(takeRequired(props, AUTH_PRIVATE_KEY)));
        } else if (props.containsKey(AUTH_REFRESH_TOKEN)) {
            builder.authenticationMode(AuthenticationMode.REFRESH_TOKEN);
            builder.refreshToken(takeRequired(props, AUTH_REFRESH_TOKEN));
            // We still accept an optional userName. This might show up
            // in the `DatabaseMetadata.getUserName` call.
            builder.userName(takeOptional(props, AUTH_USER_NAME).orElse(null));
        } else {
            throw new DataCloudJDBCException(
                    "Properties must contain either (userName + password), (userName + privateKey), or refreshToken",
                    "28000");
        }

        // Check for mixed authentication modes
        if (!Collections.disjoint(
                props.keySet(), Arrays.asList(AUTH_USER_NAME, AUTH_PASSWORD, AUTH_PRIVATE_KEY, AUTH_REFRESH_TOKEN))) {
            throw new DataCloudJDBCException("Properties from different authentication modes cannot be mixed", "28000");
        }

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the authentication properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        props.setProperty(AUTH_CLIENT_ID, clientId);
        props.setProperty(AUTH_CLIENT_SECRET, clientSecret);

        if (dataspace != null) {
            props.setProperty(AUTH_DATASPACE, dataspace);
        }
        if (userName != null) {
            props.setProperty(AUTH_USER_NAME, userName);
        }

        switch (authenticationMode) {
            case PASSWORD:
                props.setProperty(AUTH_PASSWORD, password);
                break;
            case PRIVATE_KEY:
                props.setProperty(AUTH_PRIVATE_KEY, serializePrivateKey(privateKey));
                break;
            case REFRESH_TOKEN:
                props.setProperty(AUTH_REFRESH_TOKEN, refreshToken);
                break;
        }

        return props;
    }

    private static RSAPrivateKey parsePrivateKey(String privateKey) throws DataCloudJDBCException {
        privateKey = privateKey.trim();
        if (!privateKey.startsWith("-----BEGIN PRIVATE KEY-----")
                || !privateKey.endsWith("-----END PRIVATE KEY-----")) {
            throw new DataCloudJDBCException("Private key must be in PEM format", "28000");
        }

        try {
            String rsaPrivateKey = privateKey
                    .replaceFirst("-----BEGIN PRIVATE KEY-----", "")
                    .replaceFirst("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] bytes = Base64.getDecoder().decode(rsaPrivateKey);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
            KeyFactory factory = KeyFactory.getInstance("RSA");
            return (RSAPrivateKey) factory.generatePrivate(keySpec);
        } catch (Exception ex) {
            throw new DataCloudJDBCException("Failed to parse private key", "28000", ex);
        }
    }

    private static String serializePrivateKey(RSAPrivateKey privateKey) {
        String base64Key = Base64.getEncoder().encodeToString(privateKey.getEncoded());
        String formattedKey = base64Key.replaceAll("(.{64})", "$1\n");
        return "-----BEGIN PRIVATE KEY-----\n" + formattedKey + "\n-----END PRIVATE KEY-----";
    }

    // Known login url patterns
    // See https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_endpoints.htm
    private static final List<Pattern> KNOWN_LOGIN_URL_PATTERNS = ImmutableList.of(
            Pattern.compile("^login\\.salesforce\\.com$"),
            Pattern.compile("^.+\\.my\\.salesforce\\.com$"),
            Pattern.compile("^.+\\.my\\.site\\.com$"),
            Pattern.compile("^test\\.salesforce\\.com$"),
            Pattern.compile("^login\\.test\\d+\\.pc-rnd\\.salesforce\\.com$"),
            Pattern.compile("^.+--.+\\.sandbox\\.my\\.salesforce\\.com$"));

    public static boolean isKnownLoginUrl(String host) {
        return KNOWN_LOGIN_URL_PATTERNS.stream().map(Pattern::asPredicate).anyMatch(p -> p.test(host));
    }
}
