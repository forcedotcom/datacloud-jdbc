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
package com.salesforce.datacloud.jdbc.config;

import static com.salesforce.datacloud.jdbc.config.ResourceReader.readResourceAsProperties;
import static com.salesforce.datacloud.jdbc.config.ResourceReader.readResourceAsString;
import static com.salesforce.datacloud.jdbc.config.ResourceReader.readResourceAsStringList;
import static com.salesforce.datacloud.jdbc.config.ResourceReader.withResourceAsStream;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.io.IOException;
import java.util.UUID;
import lombok.val;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResourceReaderTest {
    private static final String expectedState = "58P01";
    private static final String validPath = "/simplelogger.properties";
    private static final String validProperty = "org.slf4j.simpleLogger.defaultLogLevel";

    @Test
    void withResourceAsStreamHandlesIOException() {
        val message = UUID.randomUUID().toString();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class,
                () -> withResourceAsStream(validPath, in -> {
                    throw new IOException(message);
                }));

        assertThat(ex)
                .hasMessage("Error while loading resource file. path=" + validPath)
                .hasRootCauseMessage(message);
        assertThat(ex.getSQLState()).isEqualTo(expectedState);
    }

    @Test
    void readResourceAsStringThrowsOnNotFound() {
        val badPath = "/" + UUID.randomUUID();
        val ex = Assertions.assertThrows(DataCloudJDBCException.class, () -> readResourceAsString(badPath));

        assertThat(ex).hasMessage("Resource file not found. path=" + badPath);
        assertThat(ex.getSQLState()).isEqualTo(expectedState);
    }

    @Test
    void readResourceAsPropertiesThrowsOnNotFound() {
        val badPath = "/" + UUID.randomUUID();
        val ex = Assertions.assertThrows(DataCloudJDBCException.class, () -> readResourceAsProperties(badPath));

        assertThat(ex).hasMessage("Resource file not found. path=" + badPath);
        assertThat(ex.getSQLState()).isEqualTo(expectedState);
    }

    @Test
    void readResourceAsStringHappyPath() {
        assertThat(readResourceAsString(validPath)).contains(validProperty);
    }

    @Test
    void readResourceAsPropertiesHappyPath() {
        assertThat(readResourceAsProperties(validPath).getProperty(validProperty))
                .isNotNull()
                .isNotBlank();
    }

    @Test
    void readResourceAsStringListHappyPath() {
        AssertionsForInterfaceTypes.assertThat(readResourceAsStringList(validPath))
                .hasSizeGreaterThanOrEqualTo(1);
    }
}
