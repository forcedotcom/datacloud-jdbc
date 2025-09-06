/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.grpcmock.GrpcMock.*;
import static org.grpcmock.GrpcMock.atLeast;
import static org.grpcmock.GrpcMock.calledMethod;
import static org.grpcmock.GrpcMock.verifyThat;

import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

public class AdaptiveQueryResultIteratorFunctionalTest extends HyperGrpcTestBase {
    @SneakyThrows
    @Test
    @Disabled("Disabled until we can tune grpc-request-timeout and pg_sleep to reliably cause a query info CANCELLED")
    void getQueryInfoRetriesOnTimeout() {
        val size = 10000;
        val results = new ArrayList<Integer>();
        val configWithSleep =
                HyperServerConfig.builder().grpcRequestTimeoutSeconds("4s").build();

        try (val conn = getInterceptedClientConnection(configWithSleep)) {
            val stmt = conn.prepareStatement("SELECT g FROM generate_series(1,?) g WHERE pg_sleep(8)")
                    .unwrap(DataCloudPreparedStatement.class);
            stmt.setInt(1, size);
            val rs = stmt.executeQuery();

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), times(0));

            while (rs.next()) {
                results.add(rs.getInt(1));
            }

            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.rangeClosed(1, size).boxed().collect(Collectors.toList()));

            verifyThat(calledMethod(HyperServiceGrpc.getGetQueryInfoMethod()), atLeast(2));
        }
    }
}
