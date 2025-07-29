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
package com.salesforce.datacloud.jdbc.examples;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class TimeZoneQuerySettingTest {
    @Test
    public void testTimezoneSetting() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("querySetting.time_zone", "Asia/Tokyo");

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        try (DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("show timezone;");
                while (rs.next()) {
                    log.info("Retrieved value:{}", rs.getString(1));
                }
            }
        }
    }
}
