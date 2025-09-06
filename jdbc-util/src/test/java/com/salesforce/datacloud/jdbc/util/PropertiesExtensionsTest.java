/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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

    enum TestEnum {
        FIRST,
        SECOND,
        THIRD
    }

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
        val resultTrue = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyTrue", false);
        assertThat(resultTrue).isEqualTo(true);

        properties.setProperty("myKeyFalse", "false");
        val resultFalse = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyFalse", true);
        assertThat(resultFalse).isEqualTo(false);

        properties.setProperty("myKeyEmpty", "");
        val resultEmpty = PropertiesExtensions.getBooleanOrDefault(properties, "myKeyEmpty", false);
        assertThat(resultEmpty).isEqualTo(false);
    }

    @Test
    void getEnumOrDefaultReturnsEnumValueWhenValid() {
        Properties properties = new Properties();
        properties.setProperty("enumKey", "SECOND");

        val result = PropertiesExtensions.getEnumOrDefault(properties, "enumKey", TestEnum.FIRST);

        assertThat(result).isEqualTo(TestEnum.SECOND);
    }

    @Test
    void getEnumOrDefaultReturnsDefaultWhenInvalid() {
        Properties properties = new Properties();
        properties.setProperty("enumKey", "INVALID_VALUE");

        val result = PropertiesExtensions.getEnumOrDefault(properties, "enumKey", TestEnum.FIRST);

        assertThat(result).isEqualTo(TestEnum.FIRST);
    }

    @Test
    void getEnumOrDefaultReturnsDefaultWhenKeyMissing() {
        Properties properties = new Properties();

        val result = PropertiesExtensions.getEnumOrDefault(properties, "missingKey", TestEnum.THIRD);

        assertThat(result).isEqualTo(TestEnum.THIRD);
    }

    @Test
    void toListShouldSplitCommaSeparatedValues() {
        assertThat(PropertiesExtensions.toList("a,b,c")).containsExactly("a", "b", "c");
        assertThat(PropertiesExtensions.toList("a")).containsExactly("a");
        assertThat(PropertiesExtensions.toList("")).isEmpty();
    }

    @Test
    void toListShouldTrimWhitespace() {
        assertThat(PropertiesExtensions.toList(" a , b , c ")).containsExactly("a", "b", "c");
        assertThat(PropertiesExtensions.toList("  value  ")).containsExactly("value");
    }

    @Test
    void getListOrDefaultShouldReturnPropertyValueAsList() {
        Properties properties = new Properties();
        properties.setProperty("list", "a,b,c");

        assertThat(PropertiesExtensions.getListOrDefault(properties, "list")).containsExactly("a", "b", "c");
    }

    @Test
    void getListOrDefaultShouldReturnDefaultWhenKeyMissing() {
        Properties properties = new Properties();

        assertThat(PropertiesExtensions.getListOrDefault(properties, "missingKey", "default"))
                .containsExactly("default");
    }
}
