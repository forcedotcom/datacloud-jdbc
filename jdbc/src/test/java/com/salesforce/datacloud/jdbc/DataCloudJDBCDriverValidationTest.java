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
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class DataCloudJDBCDriverValidationTest {

    private static final String VALID_URL = "jdbc:salesforce-datacloud://login.salesforce.com";

    @Test
    void connect_withUnknownProperty_raisesUserError() {
        Driver driver = new DataCloudJDBCDriver();
        Properties props = new Properties();
        props.setProperty("FOO", "BAR");

        assertThatExceptionOfType(DataCloudJDBCException.class)
                .isThrownBy(() -> driver.connect(VALID_URL, props))
                .withMessageContaining("Unknown JDBC properties");
    }

    @Test
    void connect_withUnsupportedUrl_returnsNull() throws SQLException {
        Driver driver = new DataCloudJDBCDriver();
        Properties props = new Properties();
        // Property validation should not run when URL is not accepted
        props.setProperty("FOO", "BAR");
        // Because acceptsURL will return false, connect must return null rather than throwing validation error
        driver.connect("jdbc:mysql://localhost:3306", props);
    }
}
