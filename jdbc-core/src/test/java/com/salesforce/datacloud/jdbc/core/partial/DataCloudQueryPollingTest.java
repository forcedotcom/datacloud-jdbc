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

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Note that these tests do not use Statement::executeQuery which attempts to iterate immediately.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
class DataCloudQueryPollingTest {
    Duration small = Duration.ofSeconds(5);

    @SneakyThrows
    @Test
    void doesNotThrowWhenPredicateTimesOut() {
        try (val connection = getHyperQueryConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 109)");
            connection.waitFor(statement.getQueryId(), small, QueryStatus::allResultsProduced);

            val result = connection.waitFor(
                    statement.getQueryId(), Duration.ofMillis(100), QueryStatus.Predicates.rowsAvailable(100, 10));
            assertThat(result.getRowCount()).isEqualTo(109);
            assertThat(result.allResultsProduced()).isTrue();
        }
    }
}
