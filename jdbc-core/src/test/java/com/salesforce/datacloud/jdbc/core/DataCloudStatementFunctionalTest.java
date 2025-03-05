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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.sql.ResultSet;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

public class DataCloudStatementFunctionalTest extends HyperTestBase {
    @Test
    @SneakyThrows
    public void forwardAndReadOnly() {
        assertWithStatement(statement -> {
            val rs = statement.executeQuery("select 1");

            assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
            assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);

            assertThat(rs.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(rs.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        });
    }

    private static final String EXECUTED_MESSAGE = "a query was not executed before attempting to access results";

    @SneakyThrows
    @Test
    public void requiresExecutedResultSet() {
        assertWithStatement(statement -> assertThatThrownBy(statement::getResultSetType)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getResultSetConcurrency)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getFetchDirection)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));
    }
}
