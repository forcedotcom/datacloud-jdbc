/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This example uses a locally spawned Hyper instance to demonstrate best practices around connecting to Hyper.
 * This consciously only uses the JDBC API in the core and no helpers (outside of this class) to provide self-contained
 * examples.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
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
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,10) s");
                while (rs.next()) {
                    log.info("Retrieved value:{}", rs.getLong(1));
                }
            }
        }
    }
}
