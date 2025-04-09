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

import static com.salesforce.datacloud.jdbc.util.Require.requireNotNullOrBlank;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import lombok.val;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class RequireTest {
    @ParameterizedTest(name = "#{index} - requireNotNullOrBlank throws on args='{0}'")
    @NullSource
    @ValueSource(strings = {"", " "})
    void requireThrowsOn(String value) {
        val exception = assertThrows(IllegalArgumentException.class, () -> requireNotNullOrBlank(value, "thing"));
        val expected = "Expected argument 'thing' to not be null or blank";
        assertThat(exception).hasMessage(expected);
    }
}
