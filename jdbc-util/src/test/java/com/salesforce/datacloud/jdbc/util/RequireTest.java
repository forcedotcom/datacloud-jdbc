/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
