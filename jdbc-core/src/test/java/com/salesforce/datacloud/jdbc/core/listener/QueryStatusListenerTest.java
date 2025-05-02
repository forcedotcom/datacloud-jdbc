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
package com.salesforce.datacloud.jdbc.core.listener;

import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import salesforce.cdp.hyperdb.v1.QueryParam;

class QueryStatusListenerTest extends HyperGrpcTestBase {
    private final String query = "select * from stuff";

    @SneakyThrows
    private QueryStatusListener sut(String query, QueryParam.TransferMode mode) {
        return sut(query, mode, Duration.ofMinutes(1));
    }

    @SneakyThrows
    private QueryStatusListener sut(String query, QueryParam.TransferMode mode, Duration timeout) {
        switch (mode) {
            case ASYNC:
                return AsyncQueryStatusListener.of(query, hyperGrpcClient, timeout);
            case ADAPTIVE:
                return AdaptiveQueryStatusListener.of(query, hyperGrpcClient, timeout);
            default:
                Assertions.fail("QueryStatusListener mode not supported. mode=" + mode.name());
                return null;
        }
    }

    private static Stream<QueryParam.TransferMode> supported() {
        return Stream.of(QueryParam.TransferMode.ASYNC, QueryParam.TransferMode.ADAPTIVE);
    }

    @ParameterizedTest
    @MethodSource("supported")
    void itKeepsTrackOfQueryAndQueryId(QueryParam.TransferMode mode) {
        val query = this.query + UUID.randomUUID();
        val queryId = UUID.randomUUID().toString();

        setupExecuteQuery(queryId, query, mode);
        val listener = sut(query, mode);

        QueryStatusListenerAssert.assertThat(listener).hasQueryId(queryId).hasQuery(query);
    }
}
