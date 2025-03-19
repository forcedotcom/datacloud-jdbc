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

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.grpcmock.GrpcMock.atLeast;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

/**
 * Note that these tests do not use Statement::executeQuery which attempts to iterate immediately,
 * getQueryResult is not resilient to server timeout, only getQueryInfo.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
class DataCloudQueryPollingTest extends HyperGrpcTestBase {
    Duration small = Duration.ofSeconds(5);

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(
            strings = {
                //                "SELECT PG_SLEEP(min(max(RANDOM(),5),5));",
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, 1024 * 1024 * 10) as s(a) order by a asc"
            })
    void getQueryInfoRetriesOnTimeout(String query) {
        val configWithSleep =
                HyperServerConfig.builder().grpcRequestTimeoutSeconds("2s").build();
        try (val connection = getInterceptedClientConnection(configWithSleep)) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute(query);
            val queryId = statement.getQueryId();

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            log.warn("waiting for results produced, queryId={}", queryId);

            val status = connection.waitForResultsProduced(queryId, Duration.ofSeconds(60));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), atLeast(2));

            assertThat(status.getQueryId()).isEqualTo(queryId);
        }
    }

    @SneakyThrows
    @Test
    void throwsAboutNotEnoughRows_disallowLessThan() {
        try (val connection = getInterceptedClientConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 109)");

            assertThat(connection
                            .waitForResultsProduced(statement.getQueryId(), small)
                            .allResultsProduced())
                    .isTrue();

            Assertions.assertThatThrownBy(() -> connection.waitForRowsAvailable(
                            statement.getQueryId(), 100, 10, Duration.ofSeconds(1), false))
                    .hasMessageContaining("Timed out waiting for enough rows to be available")
                    .isInstanceOf(DataCloudJDBCException.class);
        }
    }

    @SneakyThrows
    @Test
    void throwsAboutNoNewRows_allowLessThan() {
        try (val connection = getInterceptedClientConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 100)");

            assertThat(connection
                            .waitForResultsProduced(statement.getQueryId(), small)
                            .allResultsProduced())
                    .isTrue();

            Assertions.assertThatThrownBy(() -> connection.waitForRowsAvailable(
                            statement.getQueryId(), 100, 10, Duration.ofSeconds(1), true))
                    .hasMessageContaining("Timed out waiting for new rows to be available")
                    .isInstanceOf(DataCloudJDBCException.class);
        }
    }

    @SneakyThrows
    @Test
    void userShouldWaitForQueryBeforeAccessingResultSet() {
        try (val connection = getInterceptedClientConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {

            statement.execute("SELECT pg_sleep(1000)");
            assertThatThrownBy(statement::getResultSet).hasMessageContaining("query results were not ready");
        }
    }
}
