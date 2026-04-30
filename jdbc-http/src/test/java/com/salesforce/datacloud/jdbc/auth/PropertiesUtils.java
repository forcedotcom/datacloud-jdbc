/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import java.util.Properties;

public class PropertiesUtils {
    public static Properties propertiesForCdpToken(String cdpToken, String tenantUrl) {
        Properties properties = new Properties();
        properties.setProperty("cdpToken", cdpToken);
        properties.setProperty("tenantUrl", tenantUrl);
        return properties;
    }
}
