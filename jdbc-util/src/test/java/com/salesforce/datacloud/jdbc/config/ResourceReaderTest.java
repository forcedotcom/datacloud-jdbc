/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
