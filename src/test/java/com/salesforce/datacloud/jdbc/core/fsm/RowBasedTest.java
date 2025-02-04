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
package com.salesforce.datacloud.jdbc.core.fsm;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RowBasedTest extends HyperTestBase {
    private String bigId;
    private String smallId;

    @BeforeAll
    void setupQueries() {
        this.bigId = getQueryId(1024 * 1024 * 10);
        this.smallId = getQueryId(32);
    }

    @Test
    void singleRpcReturnsIteratorButNotRowBasedFullRange() {
        val client = mock(HyperGrpcClientExecutor.class);
        val single = RowBased.of(client, "select 1", 0, 1, RowBased.Mode.SINGLE_RPC);

        assertThat(single).isInstanceOf(RowBasedSingleRpc.class).isNotInstanceOf(RowBasedFullRange.class);
    }

    @Test
    void fullRangeReturnsRowBasedFullRange() {
        val client = mock(HyperGrpcClientExecutor.class);
        val single = RowBased.of(client, "select 1", 0, 1, RowBased.Mode.FULL_RANGE);

        assertThat(single).isInstanceOf(RowBasedFullRange.class).isNotInstanceOf(RowBasedSingleRpc.class);
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(RowBased.Mode.class)
    void fetchWhereActualLessThanPageSize(RowBased.Mode mode) {
        val actual = sut(bigId, 10, 10, mode);
        val expected = assertThat(actual)
                .containsExactlyElementsOf(
                        () -> IntStream.range(10, 20).mapToObj(BigDecimal::new).iterator());
    }

    @ParameterizedTest
    @EnumSource(RowBased.Mode.class)
    void fetchWhereActualMoreThanPageSize(RowBased.Mode mode) {}

    @Test
    void fetchWherePageSizeLargerThanChunkLimit() {}

    private List<BigDecimal> sut(String queryId, long offset, long limit, RowBased.Mode mode) {
        val connection = getHyperQueryConnection();
        val resultSet = connection.getRowBasedResultSet(queryId, offset, limit, mode);
        return toList(resultSet);
    }

    @SneakyThrows
    private String getQueryId(int max) {
        val query = String.format(
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, %d) as s(a) order by a asc",
                max);

        try (val client = getHyperQueryConnection();
                val statement = client.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeAsyncQuery(query);
            while (!statement.isReady()) {
                Thread.sleep(250);
            }
            return statement.getStatus().getQueryId();
        }
    }

    private static List<BigDecimal> toList(DataCloudResultSet resultSet) {
        val iterator = new Iterator<BigDecimal>() {
            @SneakyThrows
            @Override
            public boolean hasNext() {
                return resultSet.next();
            }

            @SneakyThrows
            @Override
            public BigDecimal next() {
                return resultSet.getBigDecimal(1);
            }
        };

        return StreamUtilities.toStream(iterator).collect(Collectors.toList());
    }
}
