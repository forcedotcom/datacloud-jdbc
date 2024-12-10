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

import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.allPropertiesExcept;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPassword;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPrivateKey;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForRefreshToken;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.randomString;
import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class AuthenticationSettingsTest {
    private static AuthenticationSettings sut(Properties properties) throws SQLException {
        return AuthenticationSettings.of(properties);
    }

    @InjectSoftAssertions
    SoftAssertions softly;

    @SneakyThrows
    private static Stream<Arguments> constructors() {
        List<Properties> properties = Arrays.asList(null, new Properties());
        List<Function<Properties, AuthenticationSettings>> ctors = ImmutableList.of(
                rethrowFunction(AuthenticationSettings::of),
                rethrowFunction(PasswordAuthenticationSettings::new),
                rethrowFunction(PrivateKeyAuthenticationSettings::new),
                rethrowFunction(RefreshTokenAuthenticationSettings::new));

        return ctors.stream().flatMap(c -> properties.stream().map(p -> Arguments.of(p, c)));
    }

    @ParameterizedTest
    @MethodSource("constructors")
    void ofWithNullProperties(Properties p, Function<Properties, AuthenticationSettings> ctor) {
        if (p == null) {
            val expectedMessage = AuthenticationSettings.Messages.PROPERTIES_NULL;
            val expectedException = IllegalArgumentException.class;
            val e = assertThrows(expectedException, () -> ctor.apply(p));
            softly.assertThat((Throwable) e).hasMessage(expectedMessage).hasNoCause();
        } else {
            val expectedMessage = AuthenticationSettings.Messages.PROPERTIES_EMPTY;
            val expectedException = DataCloudJDBCException.class;
            val e = assertThrows(expectedException, () -> ctor.apply(p));
            softly.assertThat((Throwable) e)
                    .hasMessage(expectedMessage)
                    .hasCause(new IllegalArgumentException(expectedMessage));
        }
    }

    @Test
    void ofWithNoneOfTheRequiredProperties() {
        val p = allPropertiesExcept(
                AuthenticationSettings.Keys.PRIVATE_KEY,
                AuthenticationSettings.Keys.PASSWORD,
                AuthenticationSettings.Keys.REFRESH_TOKEN);
        val e = assertThrows(DataCloudJDBCException.class, () -> sut(p));
        assertThat((Throwable) e)
                .hasMessage(AuthenticationSettings.Messages.PROPERTIES_MISSING)
                .hasNoCause();
    }

    @Test
    @SneakyThrows
    void ofWithPassword() {
        val password = randomString();
        val userName = randomString();
        val p = propertiesForPassword(userName, password);

        val sut = sut(p);

        assertThat(sut).isInstanceOf(PasswordAuthenticationSettings.class);
        assertThat(((PasswordAuthenticationSettings) sut).getPassword()).isEqualTo(password);
    }

    @Test
    @SneakyThrows
    void ofWithPrivateKey() {
        val privateKey = randomString();
        val p = propertiesForPrivateKey(privateKey);

        val sut = sut(p);

        assertThat(sut).isInstanceOf(PrivateKeyAuthenticationSettings.class);
        assertThat(((PrivateKeyAuthenticationSettings) sut).getPrivateKey()).isEqualTo(privateKey);
    }

    @Test
    @SneakyThrows
    void ofWithRefreshToken() {
        val refreshToken = randomString();
        val p = propertiesForRefreshToken(refreshToken);

        val sut = sut(p);

        assertThat(sut).isInstanceOf(RefreshTokenAuthenticationSettings.class);
        assertThat((RefreshTokenAuthenticationSettings) sut)
                .satisfies(s -> assertThat(s.getRefreshToken()).isEqualTo(refreshToken));
    }

    @Test
    @SneakyThrows
    void getRelevantPropertiesFiltersUnexpectedProperties() {
        val p = allPropertiesExcept();
        p.setProperty("unexpected", randomString());

        val sut = sut(p);

        assertThat(sut.getRelevantProperties().containsKey("unexpected")).isFalse();
    }

    @Test
    @SneakyThrows
    void baseAuthenticationOptionalSettingsGettersReturnDefaultValues() {
        val p = allPropertiesExcept(
                AuthenticationSettings.Keys.USER_AGENT,
                AuthenticationSettings.Keys.DATASPACE,
                AuthenticationSettings.Keys.MAX_RETRIES);
        val sut = sut(p);

        assertThat(sut)
                .returns(
                        AuthenticationSettings.Defaults.USER_AGENT,
                        Assertions.from(AuthenticationSettings::getUserAgent))
                .returns(
                        AuthenticationSettings.Defaults.MAX_RETRIES,
                        Assertions.from(AuthenticationSettings::getMaxRetries))
                .returns(
                        AuthenticationSettings.Defaults.DATASPACE,
                        Assertions.from(AuthenticationSettings::getDataspace));
    }

    @Test
    @SneakyThrows
    void baseAuthenticationSettingsGettersReturnCorrectValues() {
        val loginUrl = randomString();
        val userName = randomString();
        val clientId = randomString();
        val clientSecret = randomString();
        val dataspace = randomString();
        val userAgent = randomString();
        val maxRetries = 123;

        val p = allPropertiesExcept();
        p.put(AuthenticationSettings.Keys.LOGIN_URL, loginUrl);
        p.put(AuthenticationSettings.Keys.USER_NAME, userName);
        p.put(AuthenticationSettings.Keys.CLIENT_ID, clientId);
        p.put(AuthenticationSettings.Keys.CLIENT_SECRET, clientSecret);
        p.put(AuthenticationSettings.Keys.DATASPACE, dataspace);
        p.put(AuthenticationSettings.Keys.USER_AGENT, userAgent);
        p.put(AuthenticationSettings.Keys.MAX_RETRIES, Integer.toString(maxRetries));

        val sut = sut(p);

        assertThat(sut)
                .returns(loginUrl, Assertions.from(AuthenticationSettings::getLoginUrl))
                .returns(clientId, Assertions.from(AuthenticationSettings::getClientId))
                .returns(clientSecret, Assertions.from(AuthenticationSettings::getClientSecret))
                .returns(userAgent, Assertions.from(AuthenticationSettings::getUserAgent))
                .returns(maxRetries, Assertions.from(AuthenticationSettings::getMaxRetries))
                .returns(dataspace, Assertions.from(AuthenticationSettings::getDataspace));
    }

    @Test
    @SneakyThrows
    void baseAuthenticationSettingsRequiredSettingsThrow() {
        AuthenticationSettings.Keys.REQUIRED_KEYS.forEach(k -> {
            val p = allPropertiesExcept(k);
            val e = assertThrows(DataCloudJDBCException.class, () -> sut(p));
            assertThat((Throwable) e)
                    .hasMessage(AuthenticationSettings.Messages.PROPERTIES_REQUIRED + k)
                    .hasCause(new IllegalArgumentException(AuthenticationSettings.Messages.PROPERTIES_REQUIRED + k));
        });
    }
}
