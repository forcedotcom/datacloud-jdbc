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

import static com.salesforce.datacloud.jdbc.util.Messages.ILLEGAL_CONNECTION_PROTOCOL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.ThrowingFunction;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
class DataCloudConnectionStringTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static ThrowingFunction<String, String, SQLException> wrap(
            ThrowingFunction<String, String, SQLException> func) {
        return func;
    }

    private static Stream<Arguments> urlTransforms() {
        return Stream.of(
                Arguments.argumentSet(
                        "getDatabaseUrl",
                        wrap(DataCloudConnectionString::getDatabaseUrl),
                        "jdbc:salesforce-datacloud://"),
                Arguments.argumentSet(
                        "getAuthenticationUrl", wrap(DataCloudConnectionString::getAuthenticationUrl), "https://"));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("urlTransforms")
    void acceptsWellFormedUrl(ThrowingFunction<String, String, SQLException> func, String prefix) {
        val url = "jdbc:salesforce-datacloud://login.salesforce.com";
        assertThat(DataCloudConnectionString.acceptsUrl(url)).isTrue();
        assertThat(func.apply(url)).isEqualTo(prefix + "login.salesforce.com");
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("urlTransforms")
    void getAuthenticationUrl_rejectsUrlThatContainsHttps() {
        val url = "jdbc:salesforce-datacloud:https://login.salesforce.com";
        assertThat(DataCloudConnectionString.acceptsUrl(url)).isFalse();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> DataCloudConnectionString.getAuthenticationUrl(url));
        assertThat(ex).hasMessage(ILLEGAL_CONNECTION_PROTOCOL);
    }

    @SneakyThrows
    @Test
    void getAuthenticationUrl_rejectsUrlThatDoesNotStartWithConnectionProtocol() {
        val url = "foo:https://login.salesforce.com";
        assertThat(DataCloudConnectionString.acceptsUrl(url)).isFalse();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> DataCloudConnectionString.getAuthenticationUrl(url));
        assertThat(ex).hasMessage(ILLEGAL_CONNECTION_PROTOCOL);
    }

    @SneakyThrows
    @Test
    void getAuthenticationUrl_rejectsUrlThatIsMalformed() {
        val url = "jdbc:salesforce-datacloud://log^in.sal^esf^orce.c^om";
        assertThat(DataCloudConnectionString.acceptsUrl(url)).isTrue();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> DataCloudConnectionString.getAuthenticationUrl(url));
        assertThat(ex).hasMessage(ILLEGAL_CONNECTION_PROTOCOL).hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @SneakyThrows
    @Test
    void getAuthenticationUrl_rejectsUrlThatIsNull() {
        assertThat(DataCloudConnectionString.acceptsUrl(null)).isFalse();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> DataCloudConnectionString.getAuthenticationUrl(null));
        assertThat(ex).hasMessage(ILLEGAL_CONNECTION_PROTOCOL);
    }

    @SneakyThrows
    @Test
    void givesPriorityToConnectionStringButPreservesExisting() {
        val key = UUID.randomUUID().toString();
        val existing = UUID.randomUUID().toString();
        val expected = UUID.randomUUID().toString();
        val another = UUID.randomUUID().toString();

        val properties = new Properties();
        properties.setProperty(key, existing);

        softly.assertThat(properties).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(key, existing));

        val str = String.format(
                "jdbc:salesforce-datacloud://login.salesforce.com;%s=%s;another=%s;;;;;", key, expected, another);
        val actual = DataCloudConnectionString.of(str);

        actual.withParameters(properties);

        softly.assertThat(properties)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(key, expected, "another", another));
    }

    @SneakyThrows
    @Test
    void removesConnectionStringParameters() {
        val user = UUID.randomUUID().toString();
        val password = UUID.randomUUID().toString();
        val dataspace = UUID.randomUUID().toString();
        val str = String.format(
                "jdbc:salesforce-datacloud://login.salesforce.com;user=%s;password=%s;dataspace=%s;;",
                user, password, dataspace);

        val properties = new Properties();
        val actual = DataCloudConnectionString.of(str);
        actual.withParameters(properties);

        softly.assertThat(actual.getDatabaseUrl()).isEqualTo("jdbc:salesforce-datacloud://login.salesforce.com");
        softly.assertThat(actual.getLoginUrl()).isEqualTo("https://login.salesforce.com");

        softly.assertThat(properties)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of("user", user, "password", password, "dataspace", dataspace));
    }

    @SneakyThrows
    @Test
    void preservesPort() {
        val actual = DataCloudConnectionString.of("jdbc:salesforce-datacloud://localhost:1234");
        softly.assertThat(actual.getDatabaseUrl()).isEqualTo("jdbc:salesforce-datacloud://localhost:1234");
        softly.assertThat(actual.getLoginUrl()).isEqualTo("https://localhost:1234");
    }
}
