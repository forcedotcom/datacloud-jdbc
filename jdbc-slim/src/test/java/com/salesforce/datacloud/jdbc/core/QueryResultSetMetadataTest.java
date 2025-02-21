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

import com.google.common.collect.ImmutableList;
import java.sql.ResultSetMetaData;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryResultSetMetadataTest {
    QueryDBMetadata queryDBMetadata = QueryDBMetadata.GET_COLUMNS;

    QueryResultSetMetadata queryResultSetMetadata;

    @BeforeEach
    public void init() {
        queryResultSetMetadata = new QueryResultSetMetadata(queryDBMetadata);
    }

    @Test
    public void testGetColumnCount() {
        assertThat(queryResultSetMetadata.getColumnCount()).isEqualTo(24);
    }

    @Test
    public void testIsAutoIncrement() {
        assertThat(queryResultSetMetadata.isAutoIncrement(1)).isFalse();
    }

    @Test
    public void testIsCaseSensitive() {
        assertThat(queryResultSetMetadata.isCaseSensitive(1)).isTrue();
    }

    @Test
    public void testIsSearchable() {
        assertThat(queryResultSetMetadata.isSearchable(1)).isFalse();
    }

    @Test
    public void testIsCurrency() {
        assertThat(queryResultSetMetadata.isCurrency(1)).isFalse();
    }

    @Test
    public void testIsNullable() {
        assertThat(queryResultSetMetadata.isNullable(1)).isEqualTo(1);
    }

    @Test
    public void testIsSigned() {
        assertThat(queryResultSetMetadata.isSigned(1)).isFalse();
    }

    @Test
    public void testGetColumnDisplaySize() {
        assertThat(queryResultSetMetadata.getColumnDisplaySize(1)).isEqualTo(9);
        assertThat(queryResultSetMetadata.getColumnDisplaySize(5)).isEqualTo(39);
    }

    @Test
    public void testGetColumnLabel() {
        for (int i = 1; i <= queryDBMetadata.getColumnNames().size(); i++) {
            assertThat(queryResultSetMetadata.getColumnLabel(i))
                    .isEqualTo(queryDBMetadata.getColumnNames().get(i - 1));
        }
    }

    @Test
    public void testGetColumnLabelWithNullColumnNameReturnsDefaultValue() {
        queryResultSetMetadata = new QueryResultSetMetadata(null, ImmutableList.of("Col1"), ImmutableList.of(1));
        assertThat(queryResultSetMetadata.getColumnLabel(1)).isEqualTo("C0");
    }

    @Test
    public void testGetColumnName() {
        for (int i = 1; i <= queryDBMetadata.getColumnNames().size(); i++) {
            assertThat(queryResultSetMetadata.getColumnName(i))
                    .isEqualTo(queryDBMetadata.getColumnNames().get(i - 1));
        }
    }

    @Test
    public void testGetSchemaName() {
        assertThat(queryResultSetMetadata.getSchemaName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetPrecision() {
        assertThat(queryResultSetMetadata.getPrecision(1)).isEqualTo(9);
        assertThat(queryResultSetMetadata.getPrecision(5)).isEqualTo(38);
    }

    @Test
    public void testGetScale() {
        assertThat(queryResultSetMetadata.getScale(1)).isEqualTo(0);
        assertThat(queryResultSetMetadata.getScale(5)).isEqualTo(18);
    }

    @Test
    public void testGetTableName() {
        assertThat(queryResultSetMetadata.getTableName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetCatalogName() {
        assertThat(queryResultSetMetadata.getCatalogName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void getColumnType() {
        for (int i = 1; i <= queryDBMetadata.getColumnTypeIds().size(); i++) {
            assertThat(queryResultSetMetadata.getColumnType(i))
                    .isEqualTo(queryDBMetadata.getColumnTypeIds().get(i - 1));
        }
    }

    @Test
    public void getColumnTypeName() {
        for (int i = 1; i <= queryDBMetadata.getColumnTypes().size(); i++) {
            assertThat(queryResultSetMetadata.getColumnTypeName(i))
                    .isEqualTo(queryDBMetadata.getColumnTypes().get(i - 1));
        }
    }

    @Test
    public void testIsReadOnly() {
        assertThat(queryResultSetMetadata.isReadOnly(1)).isTrue();
    }

    @Test
    public void isWritable() {
        assertThat(queryResultSetMetadata.isWritable(1)).isFalse();
    }

    @Test
    public void isDefinitelyWritable() {
        assertThat(queryResultSetMetadata.isDefinitelyWritable(1)).isFalse();
    }

    @Test
    public void getColumnClassName() {
        assertThat(queryResultSetMetadata.getColumnClassName(1)).isNull();
    }

    @Test
    public void unwrap() {
        assertThat(queryResultSetMetadata.unwrap(ResultSetMetaData.class)).isNull();
    }

    @Test
    public void isWrapperFor() {
        assertThat(queryResultSetMetadata.isWrapperFor(ResultSetMetaData.class)).isFalse();
    }
}
