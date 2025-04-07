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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class ConnectionQuerySettingsTest {
    @Test
    @SneakyThrows
    public void testLegacyQuerySetting() {
        val settings = Maps.immutableEntry("serverSetting.date_style", "YMD");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SHOW date_style");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("ISO, YMD");
                },
                settings);
    }

    @Test
    @SneakyThrows
    public void testQuerySetting() {
        val settings = Maps.immutableEntry("querySetting.date_style", "YMD");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SHOW date_style");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("ISO, YMD");
                },
                settings);
    }
}
