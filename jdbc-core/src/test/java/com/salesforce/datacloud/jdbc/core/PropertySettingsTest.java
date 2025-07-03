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

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

class PropertySettingsTest extends HyperGrpcTestBase {
    private static final String HYPER_SETTING = "querySetting.";

    @Test
    @SneakyThrows
    public void testQuerySetting() {
        val settings = Maps.immutableEntry("querySetting.date_style", "YMD");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SHOW date_style");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("ISO, YMD");
                },
                settings);
    }

    @Test
    void testGetSettingWithCorrectPrefix() throws DataCloudJDBCException {
        Map<String, String> expected = ImmutableMap.of("lc_time", "en_US");
        Properties properties = new Properties();
        properties.setProperty(HYPER_SETTING + "lc_time", "en_US");
        properties.setProperty("username", "alice");
        Settings settings = Settings.of(properties);
        assertThat(settings.getStatementSettings().getQuerySettings()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void testGetSettingReturnEmptyResultSet() throws DataCloudJDBCException {
        Map<String, String> expected = ImmutableMap.of();
        Properties properties = new Properties();
        properties.setProperty("c_time", "en_US");
        properties.setProperty("username", "alice");
        Settings settings = Settings.of(properties);
        assertThat(settings.getStatementSettings().getQuerySettings()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void testRoundtrip() throws DataCloudJDBCException {
        Properties properties = new Properties();
        // Cover string setting
        properties.setProperty("workload", "testWorkload");
        // Cover prefixed setting
        properties.setProperty("querySetting.A", "A");
        properties.setProperty("querySetting.B", "B");
        // Cover setting that internally is represented as non string (`Duration`)
        properties.setProperty("queryTimeout", "30");
        Settings settings = Settings.of(properties);
        Properties roundtripProperties = settings.toProperties();
        assertThat(roundtripProperties.entrySet()).containsExactlyInAnyOrderElementsOf(roundtripProperties.entrySet());
    }

    @Test
    void testGetSettingWithEmptyProperties() throws DataCloudJDBCException {
        Map<String, String> expected = ImmutableMap.of();
        Properties properties = new Properties();
        Settings settings = Settings.of(properties);
        assertThat(settings.getStatementSettings().getQuerySettings()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @SneakyThrows
    @Test
    void itSubmitsSettingsOnCall() {
        val key = UUID.randomUUID().toString();
        val setting = UUID.randomUUID().toString();
        val properties = new Properties();
        val actual = new AtomicReference<Map<String, String>>();
        properties.setProperty(HYPER_SETTING + key, setting);
        val builder = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext();

        val channel = DataCloudJdbcManagedChannel.of(builder);
        val stubProvider = new JdbcDriverStubProvider(channel, false);
        val connection = DataCloudConnection.of(stubProvider, properties);
        val client = HyperGrpcClientExecutor.of(
                connection.getStub(),
                connection.getSettings().getStatementSettings().getQuerySettings());

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(t -> {
                    actual.set(t.getSettingsMap());
                    return true;
                })
                .willReturn(ImmutableList.of(executeQueryResponse("", null, null))));

        client.executeQuery("").next();

        assertThat(actual.get()).containsOnly(Maps.immutableEntry(key, setting));

        stubProvider.close();
        channel.close();
    }
}
