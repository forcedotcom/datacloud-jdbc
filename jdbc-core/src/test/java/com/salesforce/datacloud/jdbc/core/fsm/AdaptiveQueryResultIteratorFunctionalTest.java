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
package com.salesforce.datacloud.jdbc.core.fsm;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.grpcmock.GrpcMock.*;
import static org.grpcmock.GrpcMock.atLeast;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.verifyThat;

import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

public class AdaptiveQueryResultIteratorFunctionalTest extends HyperGrpcTestBase {
    @SneakyThrows
    @Test
    @Disabled("Disabled until we can tune grpc-request-timeout and pg_sleep to reliably cause a query info CANCELLED")
    void getQueryInfoRetriesOnTimeout() {
        val size = 10000;
        val results = new ArrayList<Integer>();
        val configWithSleep =
                HyperServerConfig.builder().grpcRequestTimeoutSeconds("4s").build();

        try (val conn = getInterceptedClientConnection(configWithSleep)) {
            val stmt = conn.prepareStatement("SELECT g FROM generate_series(1,?) g WHERE pg_sleep(8)")
                    .unwrap(DataCloudPreparedStatement.class);
            stmt.setInt(1, size);
            val rs = stmt.executeQuery();

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            while (rs.next()) {
                results.add(rs.getInt(1));
            }

            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.rangeClosed(1, size).boxed().collect(Collectors.toList()));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), atLeast(2));
        }
    }
}
