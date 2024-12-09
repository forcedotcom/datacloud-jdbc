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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

public class ConnectionSettingsTest extends HyperTestBase {
    @Test
    @SneakyThrows
    public void testHyperRespectsConnectionSetting() {
        val settings = Maps.immutableEntry("serverSetting.date_style", "YMD");
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SELECT CURRENT_DATE");
                    result.next();

                    val expected = LocalDate.parse(result.getDate(1).toString(), formatter);
                    val actual = result.getDate(1);

                    assertThat(actual.toString()).isEqualTo(expected.toString());
                },
                settings);
    }
}
