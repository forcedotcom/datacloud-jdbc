/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.QueryDBMetadata;
import com.salesforce.datacloud.jdbc.core.SimpleMetadataResultSet;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleResultSetMetaDataTest {
    QueryDBMetadata queryDBMetadata = QueryDBMetadata.GET_COLUMNS;

    ResultSetMetaData simpleResultSetMetaData;

    @BeforeEach
    public void init() throws SQLException {
        SimpleMetadataResultSet simpleMetadataResultSet = SimpleMetadataResultSet.of(queryDBMetadata, null);
        simpleResultSetMetaData = simpleMetadataResultSet.getMetaData();
    }

    @Test
    public void testGetColumnCount() throws SQLException {
        assertThat(simpleResultSetMetaData.getColumnCount()).isEqualTo(24);
    }

    @Test
    public void testIsAutoIncrement() throws SQLException {
        assertThat(simpleResultSetMetaData.isAutoIncrement(1)).isFalse();
    }

    @Test
    public void testIsCaseSensitive() throws SQLException {
        assertThat(simpleResultSetMetaData.isCaseSensitive(1)).isTrue();
    }

    @Test
    public void testIsSearchable() throws SQLException {
        assertThat(simpleResultSetMetaData.isSearchable(1)).isTrue();
    }

    @Test
    public void testIsCurrency() throws SQLException {
        assertThat(simpleResultSetMetaData.isCurrency(1)).isFalse();
    }

    @Test
    public void testIsNullable() throws SQLException {
        assertThat(simpleResultSetMetaData.isNullable(1)).isEqualTo(1);
    }

    @Test
    public void testIsSigned() throws SQLException {
        assertThat(simpleResultSetMetaData.isSigned(1)).isFalse();
    }

    @Test
    public void testGetColumnDisplaySize() throws SQLException {
        assertThat(simpleResultSetMetaData.getColumnDisplaySize(1)).isEqualTo(9);
        assertThat(simpleResultSetMetaData.getColumnDisplaySize(5)).isEqualTo(11);
    }

    @Test
    public void testGetColumnLabel() throws SQLException {
        for (int i = 1; i <= queryDBMetadata.getColumnNames().size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnLabel(i))
                    .isEqualTo(queryDBMetadata.getColumnNames().get(i - 1));
        }
    }

    @Test
    public void testGetColumnLabelWithNullColumnNameReturnsDefaultValue() throws SQLException {
        ColumnMetadata columnMetadata = new ColumnMetadata(null, new ColumnType(JDBCType.VARCHAR), "TEXT");
        simpleResultSetMetaData = new SimpleResultSetMetaData(new ColumnMetadata[] {columnMetadata});
        assertThat(simpleResultSetMetaData.getColumnLabel(1)).isEqualTo("C0");
    }

    @Test
    public void testGetColumnName() throws SQLException {
        for (int i = 1; i <= queryDBMetadata.getColumnNames().size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnName(i))
                    .isEqualTo(queryDBMetadata.getColumnNames().get(i - 1));
        }
    }

    @Test
    public void testGetSchemaName() throws SQLException {
        assertThat(simpleResultSetMetaData.getSchemaName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetPrecision() throws SQLException {
        assertThat(simpleResultSetMetaData.getPrecision(1)).isEqualTo(9);
        assertThat(simpleResultSetMetaData.getPrecision(5)).isEqualTo(10);
    }

    @Test
    public void testGetScale() throws SQLException {
        assertThat(simpleResultSetMetaData.getScale(1)).isEqualTo(0);
        assertThat(simpleResultSetMetaData.getScale(5)).isEqualTo(0);
    }

    @Test
    public void testGetTableName() throws SQLException {
        assertThat(simpleResultSetMetaData.getTableName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetCatalogName() throws SQLException {
        assertThat(simpleResultSetMetaData.getCatalogName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void getColumnType() throws SQLException {
        for (int i = 1; i <= queryDBMetadata.getColumnTypeIds().size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnType(i))
                    .isEqualTo(queryDBMetadata.getColumnTypeIds().get(i - 1));
        }
    }

    @Test
    public void getColumnTypeName() throws SQLException {
        for (int i = 1; i <= queryDBMetadata.getColumnTypes().size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnTypeName(i))
                    .isEqualTo(queryDBMetadata.getColumnTypes().get(i - 1));
        }
    }

    @Test
    public void testIsReadOnly() throws SQLException {
        assertThat(simpleResultSetMetaData.isReadOnly(1)).isTrue();
    }

    @Test
    public void isWritable() throws SQLException {
        assertThat(simpleResultSetMetaData.isWritable(1)).isFalse();
    }

    @Test
    public void isDefinitelyWritable() throws SQLException {
        assertThat(simpleResultSetMetaData.isDefinitelyWritable(1)).isFalse();
    }

    @Test
    public void getColumnClassName() throws SQLException {
        assertThat(simpleResultSetMetaData.getColumnClassName(1)).isEqualTo("java.lang.String");
    }

    @Test
    public void getWarnings() throws SQLException {
        assertThat(simpleResultSetMetaData.getColumnClassName(1)).isEqualTo("java.lang.String");
    }

    @Test
    public void unwrap() throws SQLException {
        assertThat(simpleResultSetMetaData.unwrap(ResultSetMetaData.class)).isInstanceOf(SimpleResultSetMetaData.class);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        assertThat(simpleResultSetMetaData.isWrapperFor(ResultSetMetaData.class))
                .isTrue();
    }
}
