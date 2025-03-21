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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.grpcmock.GrpcMock.atLeast;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

@Slf4j
public class DataCloudQueryPollingMockTests extends HyperGrpcTestBase {
    @Test
    @SneakyThrows
    @Disabled("flakey test, disabled until HyperGrpcClientExecutor interface fix")
    void getQueryInfoDoesNotRetryIfFailureToConnect() {
        try (val connection = getInterceptedClientConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.execute("select * from nonsense");

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            assertThatThrownBy(() -> connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30)))
                    .isInstanceOf(DataCloudJDBCException.class);

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(1));
        }
    }

    @SneakyThrows
    @ParameterizedTest
    @Timeout(60)
    @ValueSource(
            strings = {
                    "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, 1024 * 1024 * 1024) as s(a) order by a asc;",
                    "SELECT PG_SLEEP(10);"
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

            try {
                connection.waitForResultsProduced(queryId, Duration.ofSeconds(30));
            } catch (Exception ex) {
                log.error("Caught exception when querying for status on a long running query with a short grpc timeout, \n" +
                        "hyper seems to cancel queries after some number of query-infos and the query is still running.\n" +
                        "This doesn't fail the test because we just want to know that we have successfully retried getQueryInfo", ex);
            }


            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), atLeast(2));

        }
    }
}
