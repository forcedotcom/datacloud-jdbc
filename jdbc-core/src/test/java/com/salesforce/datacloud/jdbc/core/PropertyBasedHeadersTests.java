/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import io.grpc.Metadata;
import java.sql.SQLException;
import java.util.Properties;
import lombok.val;
import org.junit.Test;

public class PropertyBasedHeadersTests {
    @Test
    public void testEmptyPropertiesOnlyContainsWorkload() throws SQLException {
        val properties = new Properties();
        val connectionProperties = ConnectionProperties.ofDestructive(properties);
        val metadata = DataCloudConnection.deriveHeadersFromProperties(connectionProperties);
        assertThat(metadata.keys()).containsExactly("x-hyperdb-workload");
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", ASCII_STRING_MARSHALLER)))
                .isEqualTo("jdbcv3");
    }

    @Test
    public void testPropertyForwarding() throws SQLException {
        val properties = new Properties();
        properties.setProperty("workload", "wl");
        properties.setProperty("externalClientContext", "ctx");
        val connectionProperties = ConnectionProperties.ofDestructive(properties);

        val metadata = DataCloudConnection.deriveHeadersFromProperties(connectionProperties);
        assertThat(metadata.keys())
                .containsAll(ImmutableSet.of("x-hyperdb-workload", "x-hyperdb-external-client-context"));
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", ASCII_STRING_MARSHALLER)))
                .isEqualTo("wl");
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-external-client-context", ASCII_STRING_MARSHALLER)))
                .isEqualTo("ctx");
    }
}
