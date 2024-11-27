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
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PropertyBasedHeadersTests {
    public static Function<Properties, HeaderMutatingClientInterceptor> wrap(
            Function<Properties, HeaderMutatingClientInterceptor> function) {
        return function;
    }

    private static Stream<Arguments> optionalPropertyInterceptors() {
        return Stream.of(
                Arguments.of(
                        "external-client-context",
                        "x-hyperdb-external-client-context",
                        wrap(HyperExternalClientContextHeaderInterceptor::of)),
                Arguments.of("dataspace", "dataspace", wrap(DataspaceHeaderInterceptor::of)));
    }

    private static Stream<Arguments> defaultPropertyInterceptors() {
        return Stream.of(Arguments.of("workload", "x-hyperdb-workload", wrap(HyperWorkloadHeaderInterceptor::of)));
    }

    @ParameterizedTest
    @MethodSource("optionalPropertyInterceptors")
    void testOptionalPropertyInterceptorsNoValue(
            String unused, String header, Function<Properties, HeaderMutatingClientInterceptor> function) {
        assertThat(function.apply(new Properties())).isNull();
    }

    @ParameterizedTest
    @MethodSource("optionalPropertyInterceptors")
    void testOptionalPropertyInterceptorsWithValue(
            String key, String header, Function<Properties, HeaderMutatingClientInterceptor> function) {
        val expected = UUID.randomUUID().toString();
        val properties = new Properties();
        properties.setProperty(key, expected);

        val headers = new Metadata();
        function.apply(properties).mutate(headers);

        assertThat(headers.get(Metadata.Key.of(header, ASCII_STRING_MARSHALLER)))
                .isEqualTo(expected);
    }
}