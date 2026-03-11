/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.MetadataSchemas;
import com.salesforce.datacloud.jdbc.core.SimpleMetadataResultSet;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleResultSetMetaDataTest {
    private static final List<ColumnMetadata> COLUMNS_SCHEMA = MetadataSchemas.COLUMNS;

    ResultSetMetaData simpleResultSetMetaData;

    @BeforeEach
    public void init() throws SQLException {
        SimpleMetadataResultSet simpleMetadataResultSet =
                SimpleMetadataResultSet.of(new SimpleResultSetMetaData(COLUMNS_SCHEMA), null);
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
        assertThat(simpleResultSetMetaData.getColumnDisplaySize(1)).isEqualTo(-1);
        assertThat(simpleResultSetMetaData.getColumnDisplaySize(5)).isEqualTo(11);
    }

    @Test
    public void testGetColumnLabel() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnLabel(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getName());
        }
    }

    @Test
    public void testGetColumnLabelWithNullColumnNameReturnsDefaultValue() throws SQLException {
        ColumnMetadata columnMetadata = new ColumnMetadata(null, new ColumnType(JDBCType.VARCHAR, true), "TEXT");
        simpleResultSetMetaData = new SimpleResultSetMetaData(new ColumnMetadata[] {columnMetadata});
        assertThat(simpleResultSetMetaData.getColumnLabel(1)).isEqualTo("C0");
    }

    @Test
    public void testGetColumnName() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnName(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getName());
        }
    }

    @Test
    public void testGetSchemaName() throws SQLException {
        assertThat(simpleResultSetMetaData.getSchemaName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetPrecision() throws SQLException {
        assertThat(simpleResultSetMetaData.getPrecision(1)).isEqualTo(-1);
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
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnType(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getType().getType().getVendorTypeNumber());
        }
    }

    @Test
    public void getColumnTypeName() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(simpleResultSetMetaData.getColumnTypeName(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getTypeName());
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
        // From GET_COLUMNS: column 1 is VARCHAR, column 17 is INTEGER
        assertThat(simpleResultSetMetaData.getColumnClassName(1)).isEqualTo("java.lang.String");
        assertThat(simpleResultSetMetaData.getColumnClassName(17)).isEqualTo("java.lang.Integer");

        // Cover remaining Java types from ColumnType.getJavaTypeName() switch
        assertThat(metaDataWithColumnType(JDBCType.BOOLEAN).getColumnClassName(1))
                .isEqualTo("java.lang.Boolean");
        assertThat(metaDataWithColumnType(JDBCType.SMALLINT).getColumnClassName(1))
                .isEqualTo("java.lang.Integer");
        assertThat(metaDataWithColumnType(JDBCType.BIGINT).getColumnClassName(1))
                .isEqualTo("java.lang.Long");
        assertThat(metaDataWithColumnType(JDBCType.NUMERIC).getColumnClassName(1))
                .isEqualTo("java.math.BigDecimal");
        assertThat(metaDataWithColumnType(JDBCType.FLOAT).getColumnClassName(1)).isEqualTo("java.lang.Float");
        assertThat(metaDataWithColumnType(JDBCType.DOUBLE).getColumnClassName(1))
                .isEqualTo("java.lang.Double");
        assertThat(metaDataWithColumnType(JDBCType.CHAR).getColumnClassName(1)).isEqualTo("java.lang.String");
        assertThat(metaDataWithColumnType(JDBCType.BINARY).getColumnClassName(1))
                .isEqualTo("[B");
        assertThat(metaDataWithColumnType(JDBCType.DATE).getColumnClassName(1)).isEqualTo("java.sql.Date");
        assertThat(metaDataWithColumnType(JDBCType.TIME).getColumnClassName(1)).isEqualTo("java.sql.Time");
        assertThat(metaDataWithColumnType(JDBCType.TIMESTAMP).getColumnClassName(1))
                .isEqualTo("java.sql.Timestamp");
        assertThat(metaDataWithColumnType(JDBCType.TIMESTAMP_WITH_TIMEZONE).getColumnClassName(1))
                .isEqualTo("java.sql.Timestamp");
        assertThat(metaDataWithColumnType(JDBCType.ARRAY, new ColumnType(JDBCType.VARCHAR, true))
                        .getColumnClassName(1))
                .isEqualTo("java.sql.Array");
    }

    @Test
    public void columnTypeWithNullableConstructor() throws SQLException {
        // Covers ColumnType(JDBCType type, boolean nullable)
        ColumnType nonNullable = new ColumnType(JDBCType.VARCHAR, false);
        ColumnType nullable = new ColumnType(JDBCType.INTEGER, true);
        SimpleResultSetMetaData metaNonNullable =
                new SimpleResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", nonNullable, "TEXT")});
        SimpleResultSetMetaData metaNullable =
                new SimpleResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", nullable, "INTEGER")});

        assertThat(metaNonNullable.isNullable(1)).isEqualTo(ResultSetMetaData.columnNoNulls);
        assertThat(metaNullable.isNullable(1)).isEqualTo(ResultSetMetaData.columnNullable);
    }

    private static SimpleResultSetMetaData metaDataWithColumnType(JDBCType jdbcType) {
        return metaDataWithColumnType(jdbcType, null);
    }

    private static SimpleResultSetMetaData metaDataWithColumnType(JDBCType jdbcType, ColumnType arrayElementType) {
        ColumnType columnType = arrayElementType != null
                ? new ColumnType(jdbcType, arrayElementType, true)
                : new ColumnType(jdbcType, true);
        return new SimpleResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", columnType, "TEXT")});
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
