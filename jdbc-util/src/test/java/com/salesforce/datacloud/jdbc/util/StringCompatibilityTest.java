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
