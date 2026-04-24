/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.MetadataSchemas;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataCloudResultSetMetaDataTest {
    private static final List<ColumnMetadata> COLUMNS_SCHEMA = MetadataSchemas.COLUMNS;

    ResultSetMetaData resultSetMetaData;

    @BeforeEach
    public void init() throws SQLException {
        resultSetMetaData = new DataCloudResultSetMetaData(COLUMNS_SCHEMA);
    }

    @Test
    public void testGetColumnCount() throws SQLException {
        assertThat(resultSetMetaData.getColumnCount()).isEqualTo(24);
    }

    @Test
    public void testIsAutoIncrement() throws SQLException {
        assertThat(resultSetMetaData.isAutoIncrement(1)).isFalse();
    }

    @Test
    public void testIsCaseSensitive() throws SQLException {
        assertThat(resultSetMetaData.isCaseSensitive(1)).isTrue();
    }

    @Test
    public void testIsSearchable() throws SQLException {
        assertThat(resultSetMetaData.isSearchable(1)).isTrue();
    }

    @Test
    public void testIsCurrency() throws SQLException {
        assertThat(resultSetMetaData.isCurrency(1)).isFalse();
    }

    @Test
    public void testIsNullable() throws SQLException {
        assertThat(resultSetMetaData.isNullable(1)).isEqualTo(1);
    }

    @Test
    public void testIsSigned() throws SQLException {
        assertThat(resultSetMetaData.isSigned(1)).isFalse();
    }

    @Test
    public void testGetColumnDisplaySize() throws SQLException {
        // Column 1 (TABLE_CAT) is VARCHAR unbounded
        assertThat(resultSetMetaData.getColumnDisplaySize(1)).isEqualTo(HyperType.UNLIMITED_LENGTH);
        // Column 5 (DATA_TYPE) is INTEGER -> 10 digits + 1 sign = 11
        assertThat(resultSetMetaData.getColumnDisplaySize(5)).isEqualTo(11);
    }

    @Test
    public void testGetColumnLabel() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(resultSetMetaData.getColumnLabel(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getName());
        }
    }

    @Test
    public void testGetColumnLabelWithNullColumnNameReturnsDefaultValue() throws SQLException {
        ColumnMetadata columnMetadata = new ColumnMetadata(null, HyperType.varcharUnlimited(true), "TEXT");
        resultSetMetaData = new DataCloudResultSetMetaData(new ColumnMetadata[] {columnMetadata});
        assertThat(resultSetMetaData.getColumnLabel(1)).isEqualTo("C0");
    }

    @Test
    public void testGetColumnName() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(resultSetMetaData.getColumnName(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getName());
        }
    }

    @Test
    public void testGetSchemaName() throws SQLException {
        assertThat(resultSetMetaData.getSchemaName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetPrecision() throws SQLException {
        // Column 1 (TABLE_CAT) is VARCHAR unbounded
        assertThat(resultSetMetaData.getPrecision(1)).isEqualTo(HyperType.UNLIMITED_LENGTH);
        // Column 5 (DATA_TYPE) is INTEGER -> 10 digits
        assertThat(resultSetMetaData.getPrecision(5)).isEqualTo(10);
    }

    @Test
    public void testGetScale() throws SQLException {
        assertThat(resultSetMetaData.getScale(1)).isEqualTo(0);
        assertThat(resultSetMetaData.getScale(5)).isEqualTo(0);
    }

    @Test
    public void testGetTableName() throws SQLException {
        assertThat(resultSetMetaData.getTableName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void testGetCatalogName() throws SQLException {
        assertThat(resultSetMetaData.getCatalogName(1)).isEqualTo(StringUtils.EMPTY);
    }

    @Test
    public void getColumnTypeName() throws SQLException {
        for (int i = 1; i <= COLUMNS_SCHEMA.size(); i++) {
            assertThat(resultSetMetaData.getColumnTypeName(i))
                    .isEqualTo(COLUMNS_SCHEMA.get(i - 1).getTypeName());
        }
    }

    @Test
    public void testIsReadOnly() throws SQLException {
        assertThat(resultSetMetaData.isReadOnly(1)).isTrue();
    }

    @Test
    public void isWritable() throws SQLException {
        assertThat(resultSetMetaData.isWritable(1)).isFalse();
    }

    @Test
    public void isDefinitelyWritable() throws SQLException {
        assertThat(resultSetMetaData.isDefinitelyWritable(1)).isFalse();
    }

    @Test
    public void getColumnClassName() throws SQLException {
        // From GET_COLUMNS: column 1 is VARCHAR, column 17 is INTEGER
        assertThat(resultSetMetaData.getColumnClassName(1)).isEqualTo("java.lang.String");
        assertThat(resultSetMetaData.getColumnClassName(17)).isEqualTo("java.lang.Integer");

        // Cover remaining Java types
        assertThat(metaDataWithType(HyperType.bool(true)).getColumnClassName(1)).isEqualTo("java.lang.Boolean");
        assertThat(metaDataWithType(HyperType.int16(true)).getColumnClassName(1))
                .isEqualTo("java.lang.Integer");
        assertThat(metaDataWithType(HyperType.int64(true)).getColumnClassName(1))
                .isEqualTo("java.lang.Long");
        assertThat(metaDataWithType(HyperType.decimal(10, 2, true)).getColumnClassName(1))
                .isEqualTo("java.math.BigDecimal");
        assertThat(metaDataWithType(HyperType.float4(true)).getColumnClassName(1))
                .isEqualTo("java.lang.Float");
        assertThat(metaDataWithType(HyperType.float8(true)).getColumnClassName(1))
                .isEqualTo("java.lang.Double");
        assertThat(metaDataWithType(HyperType.fixedChar(1, true)).getColumnClassName(1))
                .isEqualTo("java.lang.String");
        assertThat(metaDataWithType(HyperType.binary(10, true)).getColumnClassName(1))
                .isEqualTo("[B");
        assertThat(metaDataWithType(HyperType.date(true)).getColumnClassName(1)).isEqualTo("java.sql.Date");
        assertThat(metaDataWithType(HyperType.time(true)).getColumnClassName(1)).isEqualTo("java.sql.Time");
        assertThat(metaDataWithType(HyperType.timestamp(true)).getColumnClassName(1))
                .isEqualTo("java.sql.Timestamp");
        assertThat(metaDataWithType(HyperType.timestampTz(true)).getColumnClassName(1))
                .isEqualTo("java.sql.Timestamp");
        assertThat(metaDataWithType(HyperType.array(HyperType.varcharUnlimited(true), true))
                        .getColumnClassName(1))
                .isEqualTo("java.sql.Array");
    }

    @Test
    public void nullableVsNonNullableColumn() throws SQLException {
        HyperType nonNullable = HyperType.varcharUnlimited(false);
        HyperType nullable = HyperType.int32(true);
        DataCloudResultSetMetaData metaNonNullable =
                new DataCloudResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", nonNullable, "TEXT")});
        DataCloudResultSetMetaData metaNullable =
                new DataCloudResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", nullable, "INTEGER")});

        assertThat(metaNonNullable.isNullable(1)).isEqualTo(ResultSetMetaData.columnNoNulls);
        assertThat(metaNullable.isNullable(1)).isEqualTo(ResultSetMetaData.columnNullable);
    }

    private static DataCloudResultSetMetaData metaDataWithType(HyperType type) {
        return new DataCloudResultSetMetaData(new ColumnMetadata[] {new ColumnMetadata("col", type, "TEXT")});
    }

    @Test
    public void unwrap() throws SQLException {
        assertThat(resultSetMetaData.unwrap(ResultSetMetaData.class)).isInstanceOf(DataCloudResultSetMetaData.class);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        assertThat(resultSetMetaData.isWrapperFor(ResultSetMetaData.class)).isTrue();
    }
}
