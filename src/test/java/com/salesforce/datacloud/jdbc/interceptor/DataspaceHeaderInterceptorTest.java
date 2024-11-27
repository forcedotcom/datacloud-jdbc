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
package com.salesforce.datacloud.jdbc.interceptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import java.util.Properties;
import java.util.UUID;
import lombok.val;
import org.junit.jupiter.api.Test;

class DataspaceHeaderInterceptorTest {
    @Test
    void ofReturnsNullWithNoDataspace() {
        assertThat(DataspaceHeaderInterceptor.of(new Properties())).isNull();
    }

    @Test
    void appliesDataspaceValueToMetadata() {
        val expected = UUID.randomUUID().toString();

        val metadata = new Metadata();
        sut(expected).mutate(metadata);

        assertThat(metadata.get(Metadata.Key.of("dataspace", ASCII_STRING_MARSHALLER)))
                .isEqualTo(expected);
    }

    private static DataspaceHeaderInterceptor sut(String dataspace) {
        val properties = new Properties();
        properties.put("dataspace", dataspace);

        return DataspaceHeaderInterceptor.of(properties);
    }
}
