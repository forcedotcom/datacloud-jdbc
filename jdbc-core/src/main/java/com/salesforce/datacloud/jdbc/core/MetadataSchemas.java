/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.Constants.INTEGER;
import static com.salesforce.datacloud.jdbc.util.Constants.SHORT;
import static com.salesforce.datacloud.jdbc.util.Constants.TEXT;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnType;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.List;

/**
 * Static column metadata for standard JDBC DatabaseMetaData result set schemas (e.g. getTables,
 * getColumns). Each field is a list of {@link ColumnMetadata} describing the columns of that result
 * set.
 */
public final class MetadataSchemas {

    public static final List<ColumnMetadata> TABLE_TYPES = ImmutableList.of(column("TABLE_TYPE", TEXT, Types.VARCHAR));

    public static final List<ColumnMetadata> CATALOGS = ImmutableList.of(column("TABLE_CAT", TEXT, Types.VARCHAR));

    public static final List<ColumnMetadata> SCHEMAS =
            ImmutableList.of(column("TABLE_SCHEM", TEXT, Types.VARCHAR), column("TABLE_CATALOG", TEXT, Types.VARCHAR));

    public static final List<ColumnMetadata> TABLES = ImmutableList.of(
            column("TABLE_CAT", TEXT, Types.VARCHAR),
            column("TABLE_SCHEM", TEXT, Types.VARCHAR),
            column("TABLE_NAME", TEXT, Types.VARCHAR),
            column("TABLE_TYPE", TEXT, Types.VARCHAR),
            column("REMARKS", TEXT, Types.VARCHAR),
            column("TYPE_CAT", TEXT, Types.VARCHAR),
            column("TYPE_SCHEM", TEXT, Types.VARCHAR),
            column("TYPE_NAME", TEXT, Types.VARCHAR),
            column("SELF_REFERENCING_COL_NAME", TEXT, Types.VARCHAR),
            column("REF_GENERATION", TEXT, Types.VARCHAR));

    public static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
            column("TABLE_CAT", TEXT, Types.VARCHAR),
            column("TABLE_SCHEM", TEXT, Types.VARCHAR),
            column("TABLE_NAME", TEXT, Types.VARCHAR),
            column("COLUMN_NAME", TEXT, Types.VARCHAR),
            column("DATA_TYPE", INTEGER, Types.INTEGER),
            column("TYPE_NAME", TEXT, Types.VARCHAR),
            column("COLUMN_SIZE", INTEGER, Types.INTEGER),
            column("BUFFER_LENGTH", INTEGER, Types.INTEGER),
            column("DECIMAL_DIGITS", INTEGER, Types.INTEGER),
            column("NUM_PREC_RADIX", INTEGER, Types.INTEGER),
            column("NULLABLE", INTEGER, Types.INTEGER),
            column("REMARKS", TEXT, Types.VARCHAR),
            column("COLUMN_DEF", TEXT, Types.VARCHAR),
            column("SQL_DATA_TYPE", INTEGER, Types.INTEGER),
            column("SQL_DATETIME_SUB", INTEGER, Types.INTEGER),
            column("CHAR_OCTET_LENGTH", INTEGER, Types.INTEGER),
            column("ORDINAL_POSITION", INTEGER, Types.INTEGER),
            column("IS_NULLABLE", TEXT, Types.VARCHAR),
            column("SCOPE_CATALOG", TEXT, Types.VARCHAR),
            column("SCOPE_SCHEMA", TEXT, Types.VARCHAR),
            column("SCOPE_TABLE", TEXT, Types.VARCHAR),
            column("SOURCE_DATA_TYPE", SHORT, Types.SMALLINT),
            column("IS_AUTOINCREMENT", TEXT, Types.VARCHAR),
            column("IS_GENERATEDCOLUMN", TEXT, Types.VARCHAR));

    private static ColumnMetadata column(String name, String typeName, int typeId) {
        JDBCType jdbcType = JDBCType.valueOf(typeId);
        return new ColumnMetadata(name, new ColumnType(jdbcType, true), typeName);
    }

    private MetadataSchemas() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
