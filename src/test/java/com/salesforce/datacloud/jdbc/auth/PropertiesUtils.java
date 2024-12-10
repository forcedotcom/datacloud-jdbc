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
package com.salesforce.datacloud.jdbc.auth;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.val;

public class PropertiesUtils {
    public static Properties allPropertiesExcept(Set<String> except) {
        val properties = new Properties();
        AuthenticationSettings.Keys.ALL.stream()
                .filter(k -> !except.contains(k))
                .forEach(k -> properties.setProperty(k, randomString()));
        return properties;
    }

    public static Properties allPropertiesExcept(String... excepts) {
        Set<String> except = excepts == null || excepts.length == 0
                ? ImmutableSet.of()
                : Arrays.stream(excepts).collect(Collectors.toSet());
        return allPropertiesExcept(except);
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static Properties propertiesForPrivateKey(String privateKey) {
        val properties =
                allPropertiesExcept(AuthenticationSettings.Keys.PASSWORD, AuthenticationSettings.Keys.REFRESH_TOKEN);
        properties.setProperty(AuthenticationSettings.Keys.PRIVATE_KEY, privateKey);
        properties.setProperty(AuthenticationSettings.Keys.LOGIN_URL, "login.test1.pc-rnd.salesforce.com");
        properties.setProperty(AuthenticationSettings.Keys.CLIENT_ID, "client_id");
        properties.setProperty(AuthenticationSettings.Keys.USER_NAME, "user_name");
        return properties;
    }

    public static Properties propertiesForPassword(String userName, String password) {
        val properties =
                allPropertiesExcept(AuthenticationSettings.Keys.PRIVATE_KEY, AuthenticationSettings.Keys.REFRESH_TOKEN);
        properties.setProperty(AuthenticationSettings.Keys.USER_NAME, userName);
        properties.setProperty(AuthenticationSettings.Keys.PASSWORD, password);
        properties.setProperty(AuthenticationSettings.Keys.LOGIN_URL, "login.test1.pc-rnd.salesforce.com");
        return properties;
    }

    public static Properties propertiesForRefreshToken(String refreshToken) {
        val properties =
                allPropertiesExcept(AuthenticationSettings.Keys.PASSWORD, AuthenticationSettings.Keys.PRIVATE_KEY);
        properties.setProperty(AuthenticationSettings.Keys.REFRESH_TOKEN, refreshToken);
        properties.setProperty(AuthenticationSettings.Keys.LOGIN_URL, "login.test1.pc-rnd.salesforce.com");
        return properties;
    }
}
