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

import static org.mockito.Mockito.when;

import java.util.Properties;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.mockito.Mockito;

@UtilityClass
public class DataCloudConnectionMocker {
    public static DataCloudConnection mockedConnection(Properties properties, DataCloudJdbcManagedChannel channel) {
        val connection = Mockito.mock(DataCloudConnection.class);
        when(connection.getClientInfo()).thenReturn(properties);
        when(connection.getChannel()).thenReturn(channel);
        return connection;
    }
}
