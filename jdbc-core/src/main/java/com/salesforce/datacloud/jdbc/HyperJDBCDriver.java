/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HyperJDBCDriver implements Driver {
    static {
        try {
            log.info(
                    "Registering Hyper JDBC driver. info={}, classLoader={}",
                    formatDriverInfo(),
                    HyperJDBCDriver.class.getClassLoader());
            DriverManager.registerDriver(new HyperJDBCDriver());
            log.info("Hyper JDBC driver registered");
        } catch (SQLException e) {
            log.error("Error occurred while registering Hyper JDBC driver. {}", e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public DataCloudConnection connect(@NonNull String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        return HyperDatasource.connectUsingProperties(url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        // We only check for the prefix. We don't check if, e.g., all parameters
        // are valid. The `connect` method will throw an exception if the
        // parameters are invalid. This is consistent with the behavior expected
        // by the JDBC specification.
        return url.startsWith("jdbc:salesforce-hyper:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DriverVersion.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return DriverVersion.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }
}
