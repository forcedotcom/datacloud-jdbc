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
package com.salesforce.datacloud.jdbc.core.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.GrpcUtils;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.UUID;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@ExtendWith(InProcessGrpcMockExtension.class)
class AsyncQueryStatusPollerTest extends HyperGrpcTestBase {
    @Test
    void testPollTracksChunkCount() throws SQLException {
        val id = UUID.randomUUID().toString();
        val poller = new AsyncQueryStatusPoller(id, hyperGrpcClient);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.FINISHED, 3);

        assertThat(poller.pollChunkCount()).isEqualTo(3);
    }

    @Test
    void testPollChunkCountThrowsWhenNotReady() {
        val id = UUID.randomUUID().toString();
        val poller = new AsyncQueryStatusPoller(id, hyperGrpcClient);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);

        Assertions.assertThrows(DataCloudJDBCException.class, poller::pollChunkCount);
    }

    @Test
    void testPollIsReadyStopsFetchingAfterFinished() {
        val id = UUID.randomUUID().toString();
        val poller = new AsyncQueryStatusPoller(id, hyperGrpcClient);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.FINISHED);

        assertThat(poller.pollIsReady()).isTrue();
        assertThat(poller.pollIsReady()).isTrue();
        assertThat(poller.pollQueryStatus().getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.FINISHED);
        assertThat(poller.pollQueryStatus().getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.FINISHED);

        verifyGetQueryInfo(1);
    }

    @Test
    void testPollHappyPath() {
        val id = UUID.randomUUID().toString();
        val poller = new AsyncQueryStatusPoller(id, hyperGrpcClient);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);
        assertThat(poller.pollIsReady()).isFalse();
        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);
        assertThat(poller.pollQueryStatus().getCompletionStatus())
                .isEqualTo(QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RESULTS_PRODUCED);
        assertThat(poller.pollIsReady()).isTrue();
        assertThat(poller.pollQueryStatus().getCompletionStatus())
                .isEqualTo(QueryStatus.CompletionStatus.RESULTS_PRODUCED);

        setupGetQueryInfo(id, QueryStatus.CompletionStatus.FINISHED);
        assertThat(poller.pollIsReady()).isTrue();
        assertThat(poller.pollQueryStatus().getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.FINISHED);

        verifyGetQueryInfo(5);
    }

    @Test
    void throwsDataCloudJDBCExceptionOnPollFailure() {

        val fakeException = GrpcUtils.getFakeStatusRuntimeExceptionAsInvalidArgument();
        GrpcMock.stubFor(GrpcMock.unaryMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .willReturn(GrpcMock.exception(fakeException)));

        val poller = new AsyncQueryStatusPoller(UUID.randomUUID().toString(), hyperGrpcClient);

        val ex = assertThrows(DataCloudJDBCException.class, poller::pollQueryStatus);
        AssertionsForClassTypes.assertThat(ex)
                .hasMessageContaining("Table not found")
                .hasRootCauseInstanceOf(StatusRuntimeException.class);
    }
}
