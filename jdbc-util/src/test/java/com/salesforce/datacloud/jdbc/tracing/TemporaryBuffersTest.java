/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TemporaryBuffersTest {

    @Test
    void chars() {
        TemporaryBuffers.clearChars();
        char[] buffer10 = TemporaryBuffers.chars(10);
        assertThat(buffer10).hasSize(10);
        char[] buffer8 = TemporaryBuffers.chars(8);
        // Buffer was reused even though smaller.
        assertThat(buffer8).isSameAs(buffer10);
        char[] buffer20 = TemporaryBuffers.chars(20);
        assertThat(buffer20).hasSize(20);
    }
}
