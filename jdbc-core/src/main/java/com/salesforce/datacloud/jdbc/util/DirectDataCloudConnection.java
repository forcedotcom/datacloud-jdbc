/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DirectDataCloudConnection {
    public static final String DIRECT = "direct";

    private DirectDataCloudConnection() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static boolean isDirect(Properties properties) {
        return getBooleanOrDefault(properties, DIRECT, false);
    }

    public static DataCloudConnection of(String url, Properties properties) throws SQLException {
        final boolean direct = getBooleanOrDefault(properties, DIRECT, false);
        if (!direct) {
            throw new DataCloudJDBCException("Cannot establish direct connection without " + DIRECT + " enabled");
        }

        final DataCloudConnectionString connString = DataCloudConnectionString.of(url);
        final URI uri = URI.create(connString.getLoginUrl());

        log.info("Creating data cloud connection {}", uri);

        ManagedChannelBuilder<?> builder =
                ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext();

        return DataCloudConnection.of(builder, properties);
    }
}
