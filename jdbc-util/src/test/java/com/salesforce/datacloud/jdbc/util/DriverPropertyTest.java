/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class DriverPropertyTest {
    @Test
    void propertiesHaveNameAndDescription() {
        assertThat(DriverProperty.CLIENT_ID.getName()).isEqualTo("clientId");
        assertThat(Arrays.stream(DriverProperty.values()))
                .allMatch(p -> !p.getName().isEmpty() && !p.getDescription().isEmpty());
    }
}
