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
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PropertiesExtensionsTest {
    private final String key = UUID.randomUUID().toString();

    @Test
    void optionalValidKeyAndValue() {
        val expected = UUID.randomUUID().toString();
        val p = new Properties();
        p.put(key, expected);

        val some = PropertiesExtensions.optional(p, key);
        assertThat(some).isPresent().contains(expected);
    }

    @Test
    void optionalNotPresentKey() {
        val none = PropertiesExtensions.optional(new Properties(), "key");
        assertThat(none).isNotPresent();
    }

    @Test
    void optionalNotPresentOnNullProperties() {
        assertThat(PropertiesExtensions.optional(null, "key")).isNotPresent();
    }

    @ParameterizedTest
    @ValueSource(strings = {"  ", "\t", "\n"})
    void optionalEmptyOnIllegalValue(String input) {
        val p = new Properties();
        p.put(key, input);

        val none = PropertiesExtensions.optional(p, UUID.randomUUID().toString());
        assertThat(none).isNotPresent();
    }

    @Test
    void requiredValidKeyAndValue() {
        val expected = UUID.randomUUID().toString();
        val p = new Properties();
        p.put(key, expected);

        val some = PropertiesExtensions.required(p, key);
        assertThat(some).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"  ", "\t", "\n"})
    void requiredThrowsOnBadValue(String input) {
        val p = new Properties();
        p.put(key, input);

        val e = assertThrows(IllegalArgumentException.class, () -> PropertiesExtensions.required(p, key));
        assertThat(e).hasMessage(PropertiesExtensions.Messages.REQUIRED_MISSING_PREFIX + key);
    }

    @Test
    void copy() {
        val included = ImmutableSet.of("a", "b", "c", "d", "e");
        val excluded = ImmutableSet.of("1", "2", "3", "4", "5");

        val p = new Properties();
        Stream.concat(included.stream(), excluded.stream()).forEach(k -> p.put(k, k.toUpperCase(Locale.ROOT)));

        val actual = PropertiesExtensions.copy(p, included);

        assertThat(actual)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("a", "A", "b", "B", "c", "C", "d", "D", "e", "E"));
    }

    @Test
    void toIntegerOrNull() {
        assertThat(PropertiesExtensions.toIntegerOrNull("123")).isEqualTo(123);
        assertThat(PropertiesExtensions.toIntegerOrNull("asdfasdf")).isNull();
    }

    @Test
    void getBooleanOrDefaultKeyExistsValidInvalidValues() {
        Properties properties = new Properties();
        properties.setProperty("myKeyTrue", "true");
        Boolean resultTrue = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyTrue", false);
        assertThat(resultTrue).isEqualTo(true);

        properties.setProperty("myKeyFalse", "false");
        Boolean resultFalse = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyFalse", true);
        assertThat(resultFalse).isEqualTo(false);

        properties.setProperty("myKeyEmpty", "");
        Boolean resultEmpty = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyEmpty", false);
        assertThat(resultEmpty).isEqualTo(false);
    }
}
