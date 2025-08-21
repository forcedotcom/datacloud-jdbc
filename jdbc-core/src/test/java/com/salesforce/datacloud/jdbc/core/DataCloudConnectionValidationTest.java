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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

class DataCloudConnectionValidationTest {

    private static class FakeStubProvider implements HyperGrpcStubProvider {
        @Override
        public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
            throw new UnsupportedOperationException("not used in these tests");
        }

        @Override
        public void close() {}
    }

    @Test
    void of_withUnknownProperty_raisesUserError() {
        Properties props = new Properties();
        props.setProperty("FOO", "BAR");

        assertThatThrownBy(() -> DataCloudConnection.of(new FakeStubProvider(), props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Unknown JDBC properties");
    }

    @Test
    void of_withValidProperties_parsesQuerySettingsAndDefaults() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("querySetting.lc_time", "en_us");

        DataCloudConnection conn = DataCloudConnection.of(new FakeStubProvider(), props);
        // Access package-visible getter
        ConnectionProperties cp = conn.getConnectionProperties();
        assertThat(cp.getStatementProperties().getQuerySettings()).containsEntry("lc_time", "en_us");
        assertThat(cp.getWorkload()).isEqualTo("jdbcv3");
    }
}
