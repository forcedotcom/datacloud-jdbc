/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class StringCompatibilityTest {

    @ParameterizedTest
    @ValueSource(strings = {"test", " ", "  ", "\t", "hello world"})
    void isNotEmpty_shouldReturnTrue_forNonEmptyStrings(String input) {
        assertThat(StringCompatibility.isNotEmpty(input)).isTrue();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {""})
    void isNotEmpty_shouldReturnFalse_forNullOrEmptyStrings(String input) {
        assertThat(StringCompatibility.isNotEmpty(input)).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {""})
    void isNullOrEmpty_shouldReturnTrue_forNullOrEmptyStrings(String input) {
        assertThat(StringCompatibility.isNullOrEmpty(input)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "test", "  hello  "})
    void isNullOrEmpty_shouldReturnFalse_forNonEmptyStrings(String input) {
        assertThat(StringCompatibility.isNullOrEmpty(input)).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "   ", "\t", "\n"})
    void isNullOrBlank_shouldReturnTrue_forNullOrBlankStrings(String input) {
        assertThat(StringCompatibility.isNullOrBlank(input)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", " test ", "\thello", "a"})
    void isNullOrBlank_shouldReturnFalse_forNonBlankStrings(String input) {
        assertThat(StringCompatibility.isNullOrBlank(input)).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "   ", "\t", "\n"})
    void isBlank_shouldReturnTrue_forNullOrBlankStrings(String input) {
        assertThat(StringCompatibility.isBlank(input)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", " test ", "\thello", "a"})
    void isBlank_shouldReturnFalse_forNonBlankStrings(String input) {
        assertThat(StringCompatibility.isBlank(input)).isFalse();
    }
}
