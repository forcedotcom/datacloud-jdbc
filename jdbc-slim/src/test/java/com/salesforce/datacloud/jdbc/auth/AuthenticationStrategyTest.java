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

import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPassword;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPrivateKey;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForRefreshToken;
import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
class AuthenticationStrategyTest {
    private static final URI LOGIN;
    private static final URI INSTANCE;

    static {
        try {
            LOGIN = new URI("login.test1.pc-rnd.salesforce.com");
            INSTANCE = new URI("https://valid-instance.salesforce.com");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String JWT_GRANT = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    @InjectSoftAssertions
    SoftAssertions softly;

    @SneakyThrows
    static Stream<Arguments> settings() {
        Function<Properties, AuthenticationSettings> from = rethrowFunction(AuthenticationSettings::of);

        return Stream.of(
                Arguments.of(
                        from.apply(propertiesForPrivateKey(PrivateKeyHelpersTest.fakePrivateKey)),
                        PrivateKeyAuthenticationStrategy.class),
                Arguments.of(from.apply(propertiesForPassword("un", "pw")), PasswordAuthenticationStrategy.class),
                Arguments.of(from.apply(propertiesForRefreshToken("rt")), RefreshTokenAuthenticationStrategy.class));
    }

    @ParameterizedTest
    @MethodSource("settings")
    @SneakyThrows
    void ofSettingsProperlyMapsTypes(AuthenticationSettings settings, Class<AuthenticationStrategy> type) {
        assertThat(AuthenticationStrategy.of(settings)).isInstanceOf(type);
    }

    @Test
    @SneakyThrows
    void ofSettingsProperlyThrowsOnUnknown() {
        val anonymous = new AuthenticationSettings(propertiesForPassword("un", "pw")) {};
        val e = assertThrows(DataCloudJDBCException.class, () -> AuthenticationStrategy.of(anonymous));
        assertThat((Throwable) e)
                .hasMessage(AuthenticationStrategy.Messages.UNKNOWN_SETTINGS_TYPE)
                .hasCause(new IllegalArgumentException(AuthenticationStrategy.Messages.UNKNOWN_SETTINGS_TYPE));
    }

    @Test
    @SneakyThrows
    void givenTokenSharedStrategyCreatesCorrectRevokeCommand() {
        val passwordProperties = propertiesForPassword("un", "pw");
        val privateKeyProperties = propertiesForPrivateKey("pw");
        val refreshTokenProperties = propertiesForRefreshToken("rt");

        val props = new Properties[] {passwordProperties, privateKeyProperties, refreshTokenProperties};
        Arrays.stream(props).forEach(p -> {
            val token = new OAuthToken("token", INSTANCE);

            FormCommand revokeCommand = null;
            try {
                revokeCommand = RevokeTokenAuthenticationStrategy.of(AuthenticationSettings.of(p), token)
                        .toCommand();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            assertThat(revokeCommand.getUrl()).isEqualTo(INSTANCE);
            assertThat(revokeCommand.getSuffix()).isEqualTo(URI.create("services/oauth2/revoke"));
            assertThat(revokeCommand.getBodyEntries()).containsKeys("token").containsEntry("token", token.getToken());
        });
    }

    @Test
    @SneakyThrows
    void givenTokenSharedStrategyCreatesCorrectExchangeCommand() {
        val passwordProperties = propertiesForPassword("un", "pw");
        val privateKeyProperties = propertiesForPrivateKey("pw");
        val refreshTokenProperties = propertiesForRefreshToken("rt");

        val props = new Properties[] {passwordProperties, privateKeyProperties, refreshTokenProperties};
        Arrays.stream(props).forEach(p -> {
            val token = new OAuthToken("token", INSTANCE);

            FormCommand exchangeCommand = null;
            try {
                exchangeCommand = ExchangeTokenAuthenticationStrategy.of(AuthenticationSettings.of(p), token)
                        .toCommand();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            assertThat(exchangeCommand.getUrl()).isEqualTo(INSTANCE);
            assertThat(exchangeCommand.getSuffix()).isEqualTo(URI.create("services/a360/token"));
            assertThat(exchangeCommand.getBodyEntries())
                    .containsKeys("grant_type", "subject_token_type", "subject_token")
                    .containsEntry("grant_type", "urn:salesforce:grant-type:external:cdp")
                    .containsEntry("subject_token_type", "urn:ietf:params:oauth:token-type:access_token")
                    .containsEntry("subject_token", token.getToken());
        });
    }

    @SneakyThrows
    @Test
    void privateKeyCreatesCorrectCommand() {
        val p = propertiesForPrivateKey(PrivateKeyHelpersTest.fakePrivateKey);

        val actual = AuthenticationStrategy.of(p).buildAuthenticate();

        assertThat(actual.getUrl()).isEqualTo(LOGIN);
        assertThat(actual.getSuffix()).isEqualTo(URI.create("services/oauth2/token"));
        assertThat(actual.getBodyEntries())
                .containsKeys("grant_type", "assertion")
                .containsEntry("grant_type", JWT_GRANT);
        PrivateKeyHelpersTest.shouldYieldJwt(actual.getBodyEntries().get("assertion"), PrivateKeyHelpersTest.fakeJwt);
    }

    @SneakyThrows
    @Test
    void passwordCreatesCorrectCommand() {
        val password = UUID.randomUUID().toString();
        val userName = UUID.randomUUID().toString();
        val p = propertiesForPassword(userName, password);
        val actual = AuthenticationStrategy.of(p).buildAuthenticate();

        assertThat(actual.getUrl()).isEqualTo(LOGIN);
        assertThat(actual.getSuffix()).isEqualTo(URI.create("services/oauth2/token"));
        assertThat(actual.getBodyEntries())
                .containsKeys("grant_type", "username", "password", "client_id", "client_secret")
                .containsEntry("grant_type", "password")
                .containsEntry("username", p.getProperty("userName"))
                .containsEntry("password", password);
    }

    @SneakyThrows
    @Test
    void refreshTokenCreatesCorrectCommand() {
        val refreshToken = UUID.randomUUID().toString();
        val p = propertiesForRefreshToken(refreshToken);
        val actual = AuthenticationStrategy.of(p).buildAuthenticate();

        assertThat(actual.getUrl()).isEqualTo(LOGIN);
        assertThat(actual.getSuffix()).isEqualTo(URI.create("services/oauth2/token"));
        assertThat(actual.getBodyEntries())
                .containsKeys("grant_type", "refresh_token", "client_id", "client_secret")
                .containsEntry("grant_type", "refresh_token")
                .containsEntry("refresh_token", refreshToken);
    }

    static Stream<Arguments> grantTypeExpectations() {
        return Stream.of(
                Arguments.of(JWT_GRANT, propertiesForPrivateKey(PrivateKeyHelpersTest.fakePrivateKey)),
                Arguments.of("password", propertiesForPassword("un", "pw")),
                Arguments.of("refresh_token", propertiesForRefreshToken("rt")));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("grantTypeExpectations")
    void allIncludeGrantType(String expectedGrantType, Properties properties) {
        val actual = AuthenticationStrategy.of(properties).buildAuthenticate().getBodyEntries();
        assertThat(actual).containsEntry("grant_type", expectedGrantType);
    }

    static Stream<Properties> sharedAuthenticationSettings() {
        return Stream.of(propertiesForPassword("un", "pw"), propertiesForRefreshToken("rt"));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("sharedAuthenticationSettings")
    void allIncludeSharedSettings(Properties properties) {

        val clientId = properties.getProperty(AuthenticationSettings.Keys.CLIENT_ID);
        val clientSecret = properties.getProperty(AuthenticationSettings.Keys.CLIENT_SECRET);

        val actual = AuthenticationStrategy.of(properties).buildAuthenticate().getBodyEntries();
        softly.assertThat(actual)
                .containsEntry(AuthenticationStrategy.Keys.CLIENT_ID, clientId)
                .containsEntry(AuthenticationStrategy.Keys.CLIENT_SECRET, clientSecret);
    }

    static Stream<Properties> allAuthenticationSettings() {
        return Stream.of(propertiesForPassword("un", "pw"), propertiesForRefreshToken("rt"));
    }

    @SneakyThrows
    @Test
    void exchangeTokenAuthenticationStrategyIncludesDataspaceOptionally() {
        val properties = propertiesForPassword("un", "pw");
        val key = AuthenticationSettings.Keys.DATASPACE;
        properties.remove(key);

        val token = new OAuthToken("token", INSTANCE);

        val none = ExchangeTokenAuthenticationStrategy.of(AuthenticationSettings.of(properties), token)
                .toCommand();
        softly.assertThat(none.getBodyEntries()).doesNotContainKey(key);

        val dataspace = UUID.randomUUID().toString();
        properties.put(key, dataspace);

        val some = ExchangeTokenAuthenticationStrategy.of(AuthenticationSettings.of(properties), token)
                .toCommand();
        softly.assertThat(some.getBodyEntries()).containsEntry(key, dataspace);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("allAuthenticationSettings")
    void allIncludeUserAgentOptionally(Properties properties) {
        val key = AuthenticationSettings.Keys.USER_AGENT;
        properties.remove(key);

        val none = AuthenticationStrategy.of(properties).buildAuthenticate();
        softly.assertThat(none.getHeaders()).containsEntry(key, AuthenticationSettings.Defaults.USER_AGENT);

        val userAgent = UUID.randomUUID().toString();
        properties.put(key, userAgent);

        val some = AuthenticationStrategy.of(properties).buildAuthenticate();
        softly.assertThat(some.getHeaders()).containsEntry(key, userAgent);
    }
}
