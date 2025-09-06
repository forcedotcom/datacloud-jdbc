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

/**
 * Provides a JDBC driver for `jdbc:salesforce-datacloud:` URLs.
 *
 * This provides integration with JDBC's `DriverManager.getConnection()` interface.
 * In modern applications, you should prefer using the `DataCloudDatasource` class, as
 * it provides a type-safe way to create a DataCloudConnection.
 */
@Slf4j
public class DataCloudJDBCDriver implements Driver {
    static {
        try {
            log.info(
                    "Registering DataCloud JDBC driver. info={}, classLoader={}",
                    formatDriverInfo(),
                    DataCloudJDBCDriver.class.getClassLoader());
            DriverManager.registerDriver(new DataCloudJDBCDriver());
            log.info("DataCloud JDBC driver registered");
        } catch (SQLException e) {
            log.error("Error occurred while registering DataCloud JDBC driver. {}", e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public DataCloudConnection connect(@NonNull String url, Properties info) throws SQLException {
        log.info("connect url={}", url);

        if (!acceptsURL(url)) {
            return null;
        }

        return DataCloudDatasource.connectUsingProperties(url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return DataCloudDatasource.acceptsURL(url);
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
