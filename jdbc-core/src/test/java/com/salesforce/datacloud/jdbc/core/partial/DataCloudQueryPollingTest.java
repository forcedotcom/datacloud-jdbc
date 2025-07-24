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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.time.Duration;

import io.grpc.StatusRuntimeException;
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
    void throwsWhenPredicateTimesOut() {
        try (val conn = getHyperQueryConnection();
             val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {

            stmt.executeAsyncQuery("select * from generate_series(1, 100) where pg_sleep(1);");

            assertThatThrownBy(() -> conn.waitFor(stmt.getQueryId(), Duration.ofMillis(250), QueryStatus::allResultsProduced))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .hasMessage("Failed to get query status response. queryId=%s", stmt.getQueryId())
                    .hasRootCauseInstanceOf(StatusRuntimeException.class)
                    .satisfies(e -> assertThat(e.getCause().getMessage()).startsWith("DEADLINE_EXCEEDED: CallOptions deadline exceeded after"));
        }
    }

    @SneakyThrows
    @Test
    void throwsWhenPredicateWillNeverSucceed() {
        try (val conn = getHyperQueryConnection();
             val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {

            stmt.execute("select * from generate_series(1, 100)");
            conn.waitFor(stmt.getQueryId(), small, QueryStatus::allResultsProduced);

            val result = conn.waitFor(stmt.getQueryId(), t -> t.getRowCount() >= 110);
            assertThat(result.getRowCount()).isEqualTo(100);
            assertThat(result.allResultsProduced()).isTrue();
        }
    }
}
