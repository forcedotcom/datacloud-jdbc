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
package com.salesforce.datacloud.jdbc.core.partial;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Note that these tests do not use Statement::executeQuery which attempts to iterate immediately,
 * getQueryResult is not resilient to server timeout, only getQueryInfo.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
class DataCloudQueryPollingTest {
    Duration small = Duration.ofSeconds(5);

    @SneakyThrows
    @Test
    void throwsAboutNotEnoughRows_disallowLessThan() {
        try (val connection = getHyperQueryConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 109)");

            assertThat(connection
                            .waitForResultsProduced(statement.getQueryId(), small)
                            .allResultsProduced())
                    .isTrue();

            Assertions.assertThatThrownBy(() -> connection.waitForRowsAvailable(
                            statement.getQueryId(), 100, 10, Duration.ofSeconds(1), false))
                    .hasMessageContaining("Timed out waiting for enough items to be available")
                    .isInstanceOf(DataCloudJDBCException.class);
        }
    }

    @SneakyThrows
    @Test
    void throwsAboutNoNewRows_allowLessThan() {
        try (val connection = getHyperQueryConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 100)");

            assertThat(connection
                            .waitForResultsProduced(statement.getQueryId(), small)
                            .allResultsProduced())
                    .isTrue();

            Assertions.assertThatThrownBy(() -> connection.waitForRowsAvailable(
                            statement.getQueryId(), 100, 10, Duration.ofSeconds(1), true))
                    .hasMessageContaining("Timed out waiting for new items to be available")
                    .isInstanceOf(DataCloudJDBCException.class);
        }
    }

    @SneakyThrows
    @Test
    void userShouldWaitForQueryBeforeAccessingAsyncResultSet() {
        try (val connection = getHyperQueryConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {

            statement.executeAsyncQuery("SELECT pg_sleep(1000)");
            assertThatThrownBy(statement::getResultSet).hasMessageContaining("query results were not ready");
        }
    }
}
