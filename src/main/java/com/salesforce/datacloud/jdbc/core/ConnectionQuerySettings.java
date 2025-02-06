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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionQuerySettings {
    private static final String HYPER_SETTING = "querySetting.";
    private static final String HYPER_LEGACY_SETTING = "serverSetting.";
    private final Map<String, String> settings;

    public static ConnectionQuerySettings of(Properties properties) {
        Map<String, String> settings = new HashMap<>();
        for (val e : properties.entrySet()) {
            if (e.getKey().toString().startsWith(HYPER_SETTING)) {
                settings.put(
                        e.getKey().toString().substring(HYPER_SETTING.length()),
                        e.getValue().toString());
            } else if (e.getKey().toString().startsWith(HYPER_LEGACY_SETTING)) {
                log.warn("`serverSetting` connection properties are deprecated, use `querySetting` instead.");
                settings.put(
                        e.getKey().toString().substring(HYPER_LEGACY_SETTING.length()),
                        e.getValue().toString());
            }
        }
        return new ConnectionQuerySettings(settings);
    }

    public Map<String, String> getSettings() {
        return Collections.unmodifiableMap(settings);
    }
}
