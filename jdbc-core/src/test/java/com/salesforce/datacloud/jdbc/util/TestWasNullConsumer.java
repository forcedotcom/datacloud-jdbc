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
