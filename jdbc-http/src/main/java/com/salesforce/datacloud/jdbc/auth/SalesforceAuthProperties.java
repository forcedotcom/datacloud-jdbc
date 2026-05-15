/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalInteger;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeRequired;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
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
 * - clientSecret: OAuth client secret (required for PASSWORD and REFRESH_TOKEN modes, not allowed for PRIVATE_KEY/JWT mode,
 *                 optional for AUTH_CODE_PKCE mode for confidential clients)
 * - clientId: OAuth client ID (required)
 * - dataspace: Data space identifier, default is null
 * - refreshToken: Refresh token for token-based authentication
 * - authMode: Selects AUTH_CODE_PKCE explicitly when no other credentials are present
 * - oauthScope: OAuth scope to request during the authorization-code flow (default: cdp_query_api api refresh_token)
 * - redirectPort: Port for the loopback OAuth callback (default 0 = pick a free ephemeral port)
 * - browserAuthTimeoutSeconds: How long to wait for the user to complete the browser flow (default 120)
 */
@Slf4j
@Getter
@Builder(access = AccessLevel.PRIVATE)
public class SalesforceAuthProperties {
    public enum AuthenticationMode {
        PASSWORD,
        PRIVATE_KEY,
        REFRESH_TOKEN,
        AUTH_CODE_PKCE
    }

    static final String AUTH_USER_NAME = "userName";
    static final String AUTH_PASSWORD = "password";
    static final String AUTH_PRIVATE_KEY = "privateKey";
    static final String AUTH_CLIENT_ID = "clientId";
    static final String AUTH_CLIENT_SECRET = "clientSecret";
    static final String AUTH_REFRESH_TOKEN = "refreshToken";
    static final String AUTH_DATASPACE = "dataspace";
    static final String AUTH_MODE = "authMode";
    static final String AUTH_OAUTH_SCOPE = "oauthScope";
    static final String AUTH_REDIRECT_PORT = "redirectPort";
    static final String AUTH_BROWSER_TIMEOUT_SECONDS = "browserAuthTimeoutSeconds";

    static final String DEFAULT_OAUTH_SCOPE = "cdp_query_api api refresh_token";
    static final int DEFAULT_BROWSER_TIMEOUT_SECONDS = 120;

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

    // For `AuthenticationMode.AUTH_CODE_PKCE`
    @Builder.Default
    private final String oauthScope = DEFAULT_OAUTH_SCOPE;

    @Builder.Default
    private final int redirectPort = 0;

    @Builder.Default
    private final int browserAuthTimeoutSeconds = DEFAULT_BROWSER_TIMEOUT_SECONDS;

    // Optional fields
    @Builder.Default
    private final String dataspace = null;

    /**
     * Parses authentication properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return A SalesforceAuthProperties instance
     */
    public static SalesforceAuthProperties ofDestructive(@NonNull URI loginUrl, Properties props) throws SQLException {
        if (!isKnownLoginUrl(loginUrl.getHost())) {
            log.warn("The specified url `{}` does not match any known Salesforce hosts.", loginUrl);
        }

        SalesforceAuthPropertiesBuilder builder = SalesforceAuthProperties.builder();
        builder.loginUrl(loginUrl);

        // Required fields
        builder.clientId(takeRequired(props, AUTH_CLIENT_ID));

        // Optional fields
        builder.dataspace(takeOptional(props, AUTH_DATASPACE).orElse(null));

        // The caller can request the AUTH_CODE_PKCE flow explicitly, in case the connection
        // properties are otherwise empty (no userName/password/privateKey/refreshToken).
        boolean explicitPkce = "AUTH_CODE_PKCE".equals(takeOptional(props, AUTH_MODE).orElse(null));

        // Determine authentication mode and set credentials
        if (props.containsKey(AUTH_USER_NAME) && props.containsKey(AUTH_PASSWORD)) {
            builder.authenticationMode(AuthenticationMode.PASSWORD);
            builder.userName(takeRequired(props, AUTH_USER_NAME));
            builder.password(takeRequired(props, AUTH_PASSWORD));
            builder.clientSecret(takeRequired(props, AUTH_CLIENT_SECRET));
        } else if (props.containsKey(AUTH_USER_NAME) && props.containsKey(AUTH_PRIVATE_KEY)) {
            // JWT Bearer Token Flow does not require clientSecret and it should not be provided
            if (props.containsKey(AUTH_CLIENT_SECRET)) {
                throw new SQLException(
                        "clientSecret is not allowed for PRIVATE_KEY/JWT authentication mode. "
                                + "JWT Bearer Token Flow does not require clientSecret.",
                        "28000");
            }
            builder.authenticationMode(AuthenticationMode.PRIVATE_KEY);
            builder.userName(takeRequired(props, AUTH_USER_NAME));
            builder.privateKey(parsePrivateKey(takeRequired(props, AUTH_PRIVATE_KEY)));
        } else if (props.containsKey(AUTH_REFRESH_TOKEN)) {
            builder.authenticationMode(AuthenticationMode.REFRESH_TOKEN);
            builder.refreshToken(takeRequired(props, AUTH_REFRESH_TOKEN));
            builder.clientSecret(takeRequired(props, AUTH_CLIENT_SECRET));
            // We still accept an optional userName. This might show up
            // in the `DatabaseMetadata.getUserName` call.
            builder.userName(takeOptional(props, AUTH_USER_NAME).orElse(null));
        } else if (explicitPkce) {
            builder.authenticationMode(AuthenticationMode.AUTH_CODE_PKCE);
            // clientSecret is optional: ECAs may be configured as confidential or public clients.
            builder.clientSecret(takeOptional(props, AUTH_CLIENT_SECRET).orElse(null));
            builder.oauthScope(takeOptional(props, AUTH_OAUTH_SCOPE).orElse(DEFAULT_OAUTH_SCOPE));
            builder.redirectPort(takeOptionalInteger(props, AUTH_REDIRECT_PORT).orElse(0));
            int timeout = takeOptionalInteger(props, AUTH_BROWSER_TIMEOUT_SECONDS)
                    .orElse(DEFAULT_BROWSER_TIMEOUT_SECONDS);
            if (timeout <= 0) {
                throw new SQLException(AUTH_BROWSER_TIMEOUT_SECONDS + " must be a positive number of seconds", "28000");
            }
            builder.browserAuthTimeoutSeconds(timeout);
        } else {
            throw new SQLException(
                    "Properties must contain either (userName + password), (userName + privateKey), refreshToken, "
                            + "or authMode=AUTH_CODE_PKCE",
                    "28000");
        }

        // Check for mixed authentication modes
        if (!Collections.disjoint(
                props.keySet(), Arrays.asList(AUTH_USER_NAME, AUTH_PASSWORD, AUTH_PRIVATE_KEY, AUTH_REFRESH_TOKEN))) {
            throw new SQLException("Properties from different authentication modes cannot be mixed", "28000");
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

        if (dataspace != null) {
            props.setProperty(AUTH_DATASPACE, dataspace);
        }
        if (userName != null) {
            props.setProperty(AUTH_USER_NAME, userName);
        }

        switch (authenticationMode) {
            case PASSWORD:
                props.setProperty(AUTH_PASSWORD, password);
                props.setProperty(AUTH_CLIENT_SECRET, clientSecret);
                break;
            case PRIVATE_KEY:
                props.setProperty(AUTH_PRIVATE_KEY, serializePrivateKey(privateKey));
                break;
            case REFRESH_TOKEN:
                props.setProperty(AUTH_REFRESH_TOKEN, refreshToken);
                props.setProperty(AUTH_CLIENT_SECRET, clientSecret);
                break;
            case AUTH_CODE_PKCE:
                props.setProperty(AUTH_MODE, AuthenticationMode.AUTH_CODE_PKCE.name());
                if (clientSecret != null) {
                    props.setProperty(AUTH_CLIENT_SECRET, clientSecret);
                }
                if (!DEFAULT_OAUTH_SCOPE.equals(oauthScope)) {
                    props.setProperty(AUTH_OAUTH_SCOPE, oauthScope);
                }
                if (redirectPort != 0) {
                    props.setProperty(AUTH_REDIRECT_PORT, Integer.toString(redirectPort));
                }
                if (browserAuthTimeoutSeconds != DEFAULT_BROWSER_TIMEOUT_SECONDS) {
                    props.setProperty(AUTH_BROWSER_TIMEOUT_SECONDS, Integer.toString(browserAuthTimeoutSeconds));
                }
                break;
        }

        return props;
    }

    private static RSAPrivateKey parsePrivateKey(String privateKey) throws SQLException {
        privateKey = privateKey.trim();
        if (!privateKey.startsWith("-----BEGIN PRIVATE KEY-----")
                || !privateKey.endsWith("-----END PRIVATE KEY-----")) {
            throw new SQLException("Private key must be in PEM format", "28000");
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
            throw new SQLException("Failed to parse private key", "28000", ex);
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

    public static boolean isKnownLoginUrl(@NonNull String host) {
        return KNOWN_LOGIN_URL_PATTERNS.stream().map(Pattern::asPredicate).anyMatch(p -> p.test(host));
    }
}
