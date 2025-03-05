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
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

public class QueryTimeoutTests {
    @Test
    @SneakyThrows
    void getQueryInfoRetriesOnTimeout() {
        try (val server = HyperServerConfig.builder()
                        .grpcRequestTimeoutSeconds("5s")
                        .build()
                        .start();
                val connection = server.getConnection()) {

            connection.createStatement().executeQuery("SELECT pg_sleep(20);");
        }
    }

    @Test
    void getQueryInfoDoesNotRetryIfFailureToConnect() {}
}
