/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class SalesforceAuthPropertiesTest {

    private static final URI TEST_LOGIN_URL = URI.create("https://login.salesforce.com");
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_USER_NAME = "test-user";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_REFRESH_TOKEN = "test-refresh-token";
    private static final String TEST_DATASPACE = "test-dataspace";

    // Valid RSA private key in PEM format for testing
    static final String FAKE_PRIVATE_KEY =
            "-----BEGIN PRIVATE KEY-----\n" + "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC915CqkMpBMFEv\n"
                    + "sP7USNFkDaxf7Mf+RO+cskGH15xp270k9Y9l6QVnwqKKUVAkOPiji3vZL7yCS5X/\n"
                    + "LviGJ/GFzPLrKxTPnPHEGs8G9U5hfJF3/4Y5KGXB4FO1B/P0DEmQF0JZ8l0LYsw5\n"
                    + "lNIh3zdxIvmx+xb0jpV/PCTj3iBDc4baq/ye+L3bk+joyJVSbHJXObCNc3AYkYh/\n"
                    + "pUcWVPhnr9cIUNNykKi0pKuN4QE5oear7j289JMWdR3xovvAE8x1p2YcFqcKBikI\n"
                    + "pHxJTwQzitVprLZId+T7YaqyPKqDjCEvXb5VDO7HC/3UFguAD3J3fOG3HnEWQ0+U\n"
                    + "Dlj/Gg6hAgMBAAECggEABgEg7WbPTC4z4H8dIugQs1+ZVvQIGVCN3EC+a4ZUHKcG\n"
                    + "EvqvUpOtskFI3yNrllG0+jCUYLiuoi0I1/2Lf8FuAAOM9Vzd93kTzoDjO77w1TWE\n"
                    + "QQLfIxoA1JfR80pb8BlJZFolT5n8+7oKvTwp70dTm2ugm9KrawhteGJAb8fxRkt9\n"
                    + "qt7rr53FOkni7RF9DwNQtm/goP3dMJvVd7PA+lBjsY1Yxx1V8psKLzrFeYQiyKLe\n"
                    + "JhQtYm9RV8pqxHgg8CV82UJ0SSC773XQ2CA+265x1B1/Bm+YmL7SwKUNUGDCH3Qx\n"
                    + "Kbixjj7EeyrgQ/pWrD3qSNQTf0658GcXrvPxRgMKzwKBgQDmPObkDD5qMki3jdsg\n"
                    + "zdnaWZPQdgj04ZBAA4/T77iRGtM3lfqeI3oRmSQ5NbDZAhnZhubigHVOhFShVEWH\n"
                    + "XNXBV52oHpRA//onc/iTExZ49Ahw9tCEru6iAOTlY7sw3xqXzY0nNQydkKMMKgAl\n"
                    + "NNA07qVOrjOuc492AEB3YbW/lwKBgQDTFYkrcaJyxtE6auk9iduOSQj6WhZqXDJo\n"
                    + "OGsTl7wpzaDzdCJUW0E5etng3T+UFVK5j4eozXbLUbV2990wLiqCk06xLVKte2Qg\n"
                    + "9SLtmuuZxdcD8Jygi4yTbHaPPegF9MN3UfuokSa9XheqxX1/4UAfSZr/UdaUDyeF\n"
                    + "GJhQpi7qhwKBgBYdbXQkNO6Qa+mtp4msHCvcBNW2MMAM1oU/klYiJZFSiU1Ci7Ma\n"
                    + "50O/ePpBzP3bNM5vJchF3H6xOUvRw2fwI9wRZWRbo5Pmwol9mzfNUpBFqHXpTzgf\n"
                    + "cW6ANXxPKxDTrUM9jDxi3RZZaT/m3OK2AvXCooA/PVmZYgNMnOSarxF7AoGATk1D\n"
                    + "2AjMQRV9kdUM9pYICw0OtofqnsPaswySs+7qjvtHhGgFQBOl62OmJSMNraoHSOx0\n"
                    + "X2rwcVW7IgmqVHAbttpcK59rdw05xHwK2+dHFIDuVPjRsUBzAlEO3e4R/vuf8shH\n"
                    + "uW022LJpxyCBgImXVsPgKmw6VCwW9el8vxYWe0sCgYBUltzErOk4RAJbzh5cdo8l\n"
                    + "SkdtM1Xm3AWea1exZ4dwA3zQzr1Ukac92SvIVT8gmuaop9w6vv9R9p6fzdnSzf6E\n"
                    + "UzQv/ylNI0C1yDrxcruTu3SmA//rowtpo+cIXB+1Zf8UNz4QSvLw/sD/Uh1qxjUt\n"
                    + "FB4+AIToJBMFzG8K+jEs7Q==\n"
                    + "-----END PRIVATE KEY-----";

    @Test
    void parsesPasswordAuthenticationProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("password", TEST_PASSWORD);
        props.setProperty("dataspace", TEST_DATASPACE);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);

        assertThat(authProps.getLoginUrl()).isEqualTo(TEST_LOGIN_URL);
        assertThat(authProps.getAuthenticationMode()).isEqualTo(SalesforceAuthProperties.AuthenticationMode.PASSWORD);
        assertThat(authProps.getClientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(authProps.getClientSecret()).isEqualTo(TEST_CLIENT_SECRET);
        assertThat(authProps.getUserName()).isEqualTo(TEST_USER_NAME);
        assertThat(authProps.getPassword()).isEqualTo(TEST_PASSWORD);
        assertThat(authProps.getDataspace()).isEqualTo(TEST_DATASPACE);
    }

    @Test
    void parsesPrivateKeyAuthenticationProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("privateKey", FAKE_PRIVATE_KEY);
        props.setProperty("dataspace", TEST_DATASPACE);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);

        assertThat(authProps.getLoginUrl()).isEqualTo(TEST_LOGIN_URL);
        assertThat(authProps.getAuthenticationMode())
                .isEqualTo(SalesforceAuthProperties.AuthenticationMode.PRIVATE_KEY);
        assertThat(authProps.getClientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(authProps.getClientSecret()).isEqualTo(TEST_CLIENT_SECRET);
        assertThat(authProps.getUserName()).isEqualTo(TEST_USER_NAME);
        assertThat(authProps.getPrivateKey()).isNotNull();
        assertThat(authProps.getDataspace()).isEqualTo(TEST_DATASPACE);
    }

    @Test
    void parsesRefreshTokenAuthenticationProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("refreshToken", TEST_REFRESH_TOKEN);
        props.setProperty("dataspace", TEST_DATASPACE);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);

        assertThat(authProps.getLoginUrl()).isEqualTo(TEST_LOGIN_URL);
        assertThat(authProps.getAuthenticationMode())
                .isEqualTo(SalesforceAuthProperties.AuthenticationMode.REFRESH_TOKEN);
        assertThat(authProps.getClientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(authProps.getClientSecret()).isEqualTo(TEST_CLIENT_SECRET);
        assertThat(authProps.getRefreshToken()).isEqualTo(TEST_REFRESH_TOKEN);
        assertThat(authProps.getDataspace()).isEqualTo(TEST_DATASPACE);
    }

    @Test
    void parsesRefreshTokenWithOptionalUserName() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("refreshToken", TEST_REFRESH_TOKEN);
        props.setProperty("userName", TEST_USER_NAME);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);

        assertThat(authProps.getAuthenticationMode())
                .isEqualTo(SalesforceAuthProperties.AuthenticationMode.REFRESH_TOKEN);
        assertThat(authProps.getRefreshToken()).isEqualTo(TEST_REFRESH_TOKEN);
        assertThat(authProps.getUserName()).isEqualTo(TEST_USER_NAME);
    }

    @Test
    void parsesPropertiesWithoutDataspace() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("password", TEST_PASSWORD);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);

        assertThat(authProps.getDataspace()).isNull();
    }

    @Test
    void toPropertiesRoundtripPasswordAuthentication() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("password", TEST_PASSWORD);
        props.setProperty("dataspace", TEST_DATASPACE);

        Properties originalProps = (Properties) props.clone();
        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);
        Properties roundtripProps = authProps.toProperties();

        assertThat(roundtripProps).isEqualTo(originalProps);
    }

    @Test
    void toPropertiesRoundtripPrivateKeyAuthentication() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("privateKey", FAKE_PRIVATE_KEY);
        props.setProperty("dataspace", TEST_DATASPACE);

        Properties originalProps = (Properties) props.clone();
        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);
        Properties roundtripProps = authProps.toProperties();

        assertThat(roundtripProps).isEqualTo(originalProps);
    }

    @Test
    void toPropertiesRoundtripRefreshTokenAuthentication() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("refreshToken", TEST_REFRESH_TOKEN);
        props.setProperty("dataspace", TEST_DATASPACE);

        Properties originalProps = (Properties) props.clone();
        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props);
        Properties roundtripProps = authProps.toProperties();

        assertThat(roundtripProps).isEqualTo(originalProps);
    }

    @Test
    void rejectsInvalidAuthenticationMode() {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        // Missing all authentication credentials

        assertThatThrownBy(() -> SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining(
                        "Properties must contain either (userName + password), (userName + privateKey), or refreshToken");
    }

    @Test
    void rejectsMixedAuthenticationModes() {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("password", TEST_PASSWORD);
        props.setProperty("privateKey", FAKE_PRIVATE_KEY); // Mixed with password

        assertThatThrownBy(() -> SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Properties from different authentication modes cannot be mixed");
    }

    @Test
    void rejectsInvalidPrivateKeyFormat() {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("privateKey", "invalid-key-format");

        assertThatThrownBy(() -> SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Private key must be in PEM format");
    }

    @Test
    void rejectsCorruptedPrivateKey() {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty(
                "privateKey", "-----BEGIN PRIVATE KEY-----\ninvalid-base64-content\n-----END PRIVATE KEY-----");

        assertThatThrownBy(() -> SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Failed to parse private key");
    }

    @Test
    void acceptsKnownLoginUrlPatterns() throws SQLException {
        String[] knownHosts = {
            "login.salesforce.com",
            "test.salesforce.com",
            "mycompany.my.salesforce.com",
            "mycompany.my.site.com",
            "login.test1.pc-rnd.salesforce.com",
            "mycompany--sandbox.my.salesforce.com"
        };

        for (String host : knownHosts) {
            assertTrue(SalesforceAuthProperties.isKnownLoginUrl(host));
        }
    }

    @Test
    void warnsOnUnknownLoginUrl() throws SQLException {
        Properties props = new Properties();
        props.setProperty("clientId", TEST_CLIENT_ID);
        props.setProperty("clientSecret", TEST_CLIENT_SECRET);
        props.setProperty("userName", TEST_USER_NAME);
        props.setProperty("password", TEST_PASSWORD);

        URI unknownUrl = URI.create("https://unknown.example.com");
        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(unknownUrl, props);

        // Should not throw, but should log a warning (we can't easily test the log message)
        assertThat(authProps.getLoginUrl()).isEqualTo(unknownUrl);
    }

    @Test
    void privateKeySerializationRoundtrip() throws Exception {
        Properties originalProps = new Properties();
        originalProps.setProperty("clientId", TEST_CLIENT_ID);
        originalProps.setProperty("clientSecret", TEST_CLIENT_SECRET);
        originalProps.setProperty("userName", TEST_USER_NAME);
        originalProps.setProperty("privateKey", FAKE_PRIVATE_KEY);

        SalesforceAuthProperties authProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, originalProps);
        Properties serialized = authProps.toProperties();
        SalesforceAuthProperties deserializedProps = SalesforceAuthProperties.ofDestructive(TEST_LOGIN_URL, serialized);

        assertThat(deserializedProps.getPrivateKey()).isNotNull();
        assertThat(deserializedProps.getPrivateKey().getAlgorithm())
                .isEqualTo(authProps.getPrivateKey().getAlgorithm());
        assertThat(deserializedProps.getPrivateKey().getFormat())
                .isEqualTo(authProps.getPrivateKey().getFormat());
    }
}
