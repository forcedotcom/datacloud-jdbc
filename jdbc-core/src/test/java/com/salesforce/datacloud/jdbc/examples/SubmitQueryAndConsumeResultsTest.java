/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.GrpcChannelProperties;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This example uses a locally spawned Hyper instance to demonstrate best practices around connecting to Hyper.
 * This consciously only uses the JDBC API in the core and no helpers (outside of this class) to provide self-contained
 * examples.
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class SubmitQueryAndConsumeResultsTest {
    /**
     * This example shows how to create a Data Cloud Connection while still having full control over concerns like
     * authorization and tracing.
     */
    @Test
    public void testBareBonesExecuteQuery() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels that are set up in the way you like (mTLS / Plaintext / ...) and your
        // own interceptors as well as executors.
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperServerManager.get(ConfigFile.DEFAULT).getPort())
                .usePlaintext();
        // You can set settings for the stub provider
        val grpcChannelProperties =
                GrpcChannelProperties.builder().keepAliveEnabled(false).build();
        val stubProvider = JdbcDriverStubProvider.of(channelBuilder, grpcChannelProperties);
        // You can also set settings for the connection
        val connectionProperties =
                ConnectionProperties.builder().workload("test-workload").build();

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.of(stubProvider, connectionProperties, null)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,10) s");
                while (rs.next()) {
                    log.info("Retrieved value:{}", rs.getLong(1));
                }
            }
        }
    }
}
