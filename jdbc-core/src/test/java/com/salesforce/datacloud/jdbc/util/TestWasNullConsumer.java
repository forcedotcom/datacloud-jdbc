/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import lombok.Data;
import org.assertj.core.api.SoftAssertions;

@Data
public class TestWasNullConsumer implements QueryJDBCAccessorFactory.WasNullConsumer {
    private final SoftAssertions collector;

    private int wasNullSeen = 0;
    private int wasNotNullSeen = 0;

    @Override
    public void setWasNull(boolean wasNull) {
        if (wasNull) wasNullSeen++;
        else wasNotNullSeen++;
    }

    public TestWasNullConsumer hasNullSeen(int nullsSeen) {
        collector.assertThat(this.wasNullSeen).as("witnessed null count").isEqualTo(nullsSeen);
        return this;
    }

    public TestWasNullConsumer hasNotNullSeen(int notNullSeen) {
        collector.assertThat(this.wasNotNullSeen).as("witnessed not null count").isEqualTo(notNullSeen);
        return this;
    }

    public TestWasNullConsumer assertThat() {
        return this;
    }
}
