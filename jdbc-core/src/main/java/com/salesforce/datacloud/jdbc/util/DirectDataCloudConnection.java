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

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class DirectDataCloudConnection {
    public static final String DIRECT = "direct";

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
