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

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import io.grpc.Metadata;
import java.util.Properties;
import lombok.val;
import org.junit.Test;

public class PropertyBasedHeadersTests {
    @Test
    public void testEmptyPropertiesOnlyContainsWorkload() {
        val properties = new Properties();
        val metadata = DataCloudConnection.deriveMetadataFromProperties(properties);
        assertThat(metadata.keys()).containsExactly("x-hyperdb-workload");
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", ASCII_STRING_MARSHALLER)))
                .isEqualTo("jdbcv3");
    }

    @Test
    public void testPropertyForwarding() {
        val properties = new Properties();
        properties.setProperty("dataspace", "ds");
        properties.setProperty("workload", "wl");
        properties.setProperty("external-client-context", "ctx");

        val metadata = DataCloudConnection.deriveMetadataFromProperties(properties);
        assertThat(metadata.keys())
                .containsAll(ImmutableSet.of("x-hyperdb-workload", "dataspace", "x-hyperdb-external-client-context"));
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", ASCII_STRING_MARSHALLER)))
                .isEqualTo("wl");
        assertThat(metadata.get(Metadata.Key.of("dataspace", ASCII_STRING_MARSHALLER)))
                .isEqualTo("ds");
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-external-client-context", ASCII_STRING_MARSHALLER)))
                .isEqualTo("ctx");
    }
}
