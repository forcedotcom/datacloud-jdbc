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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

public class JDBCLimitsTest extends HyperTestBase {
    @Test
    @SneakyThrows
    public void testLargeQuery() {
        String query = "SELECT 'a', /*" + StringUtils.repeat('x', 62 * 1024 * 1024) + "*/ 'b'";
        // Verify that the full SQL string is submitted by checking that the value before and after the large
        // comment are returned
        assertWithStatement(statement -> {
            val result = statement.executeQuery(query);
            result.next();
            assertThat(result.getString(1)).isEqualTo("a");
            assertThat(result.getString(2)).isEqualTo("b");
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeQuery() {
        String query = "SELECT 'a', /*" + StringUtils.repeat('x', 65 * 1024 * 1024) + "*/ 'b'";
        assertWithStatement(statement -> {
            assertThatExceptionOfType(DataCloudJDBCException.class)
                    .isThrownBy(() -> {
                        statement.executeQuery(query);
                    })
                    // Also verify that we don't explode exception sizes by keeping the full query
                    .withMessageEndingWith("<truncated>")
                    .satisfies(t -> assertThat(t.getMessage()).hasSizeLessThan(16500));
        });
    }

    @Test
    @SneakyThrows
    public void testLargeRowResponse() {
        // 31 MB is the expected max row size configured in Hyper
        String value = StringUtils.repeat('x', 31 * 1024 * 1024);
        String query = "SELECT rpad('', 31*1024*1024, 'x')";
        // Verify that large responses are supported
        assertWithStatement(statement -> {
            val result = statement.executeQuery(query);
            result.next();
            assertThat(result.getString(1)).isEqualTo(value);
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeRowResponse() {
        // 31 MB is the expected max row size configured in Hyper, thus 33 MB should be too large
        String query = "SELECT rpad('', 33*1024*1024, 'x')";
        assertWithStatement(statement -> {
            assertThatExceptionOfType(DataCloudJDBCException.class)
                    .isThrownBy(() -> {
                        statement.executeQuery(query);
                    })
                    .withMessageContaining("tuple size limit exceeded");
        });
    }

    @Test
    @SneakyThrows
    public void testLargeParameterRoundtrip() {
        // 31 MB is the expected max row size configured in Hyper
        String value = StringUtils.repeat('x', 31 * 1024 * 1024);
        // Verify that large responses are supported
        assertWithConnection(connection -> {
            val stmt = connection.prepareStatement("SELECT ?");
            stmt.setString(1, value);
            val result = stmt.executeQuery();
            result.next();
            assertThat(result.getString(1)).isEqualTo(value);
        });
    }

    @Test
    @SneakyThrows
    public void testLargeParameter() {
        // We can send requests of up to 64MB so this parameter should still be accepted
        String value = StringUtils.repeat('x', 63 * 1024 * 1024);
        // Verify that large responses are supported
        assertWithConnection(connection -> {
            val stmt = connection.prepareStatement("SELECT length(?)");
            stmt.setString(1, value);
            val result = stmt.executeQuery();
            result.next();
            assertThat(result.getInt(1)).isEqualTo(value.length());
        });
    }

    @Test
    @SneakyThrows
    public void testTooLargeParameter() {
        // We can send requests of up to 64MB so this parameter should fail
        String value = StringUtils.repeat('x', 64 * 1024 * 1024);
        assertWithConnection(connection -> {
            assertThatExceptionOfType(DataCloudJDBCException.class).isThrownBy(() -> {
                val stmt = connection.prepareStatement("SELECT length(?)");
                stmt.setString(1, value);
                stmt.executeQuery();
            });
        });
    }

    @Test
    @SneakyThrows
    public void testLargeHeaders() {
        // We expect that under 1 MB total header size should be fine, we use workload as it'll get injected into the
        // header
        val settings = Maps.immutableEntry("workload", StringUtils.repeat('x', 1000 * 1024));
        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SELECT 'A'");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("A");
                },
                settings);
    }

    @Test
    @SneakyThrows
    public void testTooLargeHeaders() {
        // We expect that due to 1 MB total header size limit, setting such a large workload should fail
        val settings = Maps.immutableEntry("workload", StringUtils.repeat('x', 1024 * 1024));
        assertWithStatement(
                statement -> {
                    assertThatExceptionOfType(DataCloudJDBCException.class).isThrownBy(() -> {
                        statement.executeQuery("SELECT 'A'");
                    });
                },
                settings);
    }
}
