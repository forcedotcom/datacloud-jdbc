/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
