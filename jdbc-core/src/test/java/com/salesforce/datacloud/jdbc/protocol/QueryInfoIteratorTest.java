/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import java.util.NoSuchElementException;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@ExtendWith(LocalHyperTestBase.class)
class QueryInfoIteratorTest {

    @Test
    @SneakyThrows
    void queryInfoIterator_shouldIterateOverQueryInfoMessages() {
        LocalHyperTestBase.assertWithStubProvider(provider -> {
            try (val connection = DataCloudConnection.of(provider, ConnectionProperties.defaultProperties(), "", null);
                    val stmt = connection.createStatement()) {
                // Submit a long-running query to get query id
                ((DataCloudStatement) stmt).executeAsyncQuery("SELECT pg_sleep(100000)");
                val queryId = ((DataCloudStatement) stmt).getQueryId();

                // Create query info iterator
                val stub = provider.getStub();
                val client = QueryAccessGrpcClient.of(queryId, stub);
                val iterator = QueryInfoIterator.of(client);

                // Verify we can get at least one query info
                assertThat(iterator.hasNext()).isTrue();
                val firstInfo = iterator.next();
                assertThat(firstInfo.getQueryStatus().getQueryId()).isEqualTo(queryId);
                // Status should be RUNNING_OR_UNSPECIFIED since query is still running
                assertThat(firstInfo.getQueryStatus().getCompletionStatus())
                        .isIn(QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);
            }
        });
    }

    @Test
    @SneakyThrows
    void queryInfoIterator_shouldIterateUntilQueryFinishes() {
        LocalHyperTestBase.assertWithStubProvider(provider -> {
            try (val connection = DataCloudConnection.of(provider, ConnectionProperties.defaultProperties(), "", null);
                    val stmt = connection.createStatement()) {
                // Submit a simple query that completes quickly
                val result = (DataCloudResultSet) stmt.executeQuery("SELECT 1");
                val queryId = result.getQueryId();

                // Create query info iterator
                val stub = provider.getStub();
                val client = QueryAccessGrpcClient.of(queryId, stub);
                val iterator = QueryInfoIterator.of(client);

                // Iterate through all query infos
                int count = 0;
                boolean sawFinished = false;
                while (iterator.hasNext()) {
                    val info = iterator.next();
                    count++;
                    if (info.hasQueryStatus()) {
                        val status = info.getQueryStatus().getCompletionStatus();
                        if (status == QueryStatus.CompletionStatus.FINISHED) {
                            sawFinished = true;
                        }
                    }
                }

                // Verify we got at least one info and saw the finished status
                assertThat(count).isGreaterThan(0);
                assertThat(sawFinished).isTrue();

                // Verify hasNext returns false after iteration
                assertThat(iterator.hasNext()).isFalse();
                // Verify calling next() after exhaustion throws NoSuchElementException
                assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
            }
        });
    }
}
