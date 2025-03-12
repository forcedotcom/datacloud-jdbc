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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.grpcmock.GrpcMock.atLeast;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

/**
 * Note that these tests do not use Statement::executeQuery which attempts to iterate immediately,
 * getQueryResult is not resilient to server timeout, only getQueryInfo.
 */
@Slf4j
class DataCloudQueryPollingTest extends HyperTestBase {
    private static final String large =
            "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, 1024 * 1024 * 10) as s(a) order by a asc";

    @SneakyThrows
    @Timeout(30)
    @ParameterizedTest
    @ValueSource(strings = {"SELECT pg_sleep(10);", large})
    void getQueryInfoRetriesOnTimeout(String query) {
        val configWithSleep =
                HyperServerConfig.builder().grpcRequestTimeoutSeconds("2s").build();
        try (val connection = getInterceptedClientConnection(configWithSleep)) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute(query);
            val queryId = statement.getQueryId();

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            log.warn("waiting for results produced, queryId={}", queryId);

            val status = connection.waitForResultsProduced(queryId, Duration.ofSeconds(20));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), atLeast(2));

            assertThat(status.getQueryId()).isEqualTo(queryId);
        }
    }

    @Test
    @SneakyThrows
    void getQueryInfoDoesNotRetryIfFailureToConnect() {
        try (val connection = getInterceptedClientConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.execute("select * from nonsense");

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            assertThatThrownBy(() -> connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30)));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(1));
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
