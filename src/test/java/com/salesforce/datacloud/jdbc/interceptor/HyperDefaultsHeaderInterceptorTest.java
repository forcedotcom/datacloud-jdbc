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
import java.util.Objects;
import lombok.val;
import org.junit.jupiter.api.Test;

class HyperDefaultsHeaderInterceptorTest {

    private static final MaxMetadataSizeHeaderInterceptor sut = new MaxMetadataSizeHeaderInterceptor();

    @Test
    void setsMaxSize() {
        val key = Metadata.Key.of("grpc.max_metadata_size", ASCII_STRING_MARSHALLER);
        assertThat(Integer.parseInt(Objects.requireNonNull(actual().get(key)))).isEqualTo(1024 * 1024);
    }

    private static Metadata actual() {
        val metadata = new Metadata();
        sut.mutate(metadata);
        return metadata;
    }
}
