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
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

import java.time.Duration;

import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

public class DataCloudQueryPollingMockTests extends HyperGrpcTestBase {
    @Test
    @SneakyThrows
    @Disabled
    void getQueryInfoDoesNotRetryIfFailureToConnect() {
        try (val connection = getInterceptedClientConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            statement.execute("select * from nonsense");

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            AssertionsForClassTypes.assertThatThrownBy(
                    () -> connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30)));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(1));
        }
    }
}
