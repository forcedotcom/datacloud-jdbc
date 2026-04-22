/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.Constants.INTEGER;
import static com.salesforce.datacloud.jdbc.util.Constants.SHORT;
import static com.salesforce.datacloud.jdbc.util.Constants.TEXT;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.util.List;

/**
 * Static column metadata for standard JDBC DatabaseMetaData result set schemas (e.g. getTables,
 * getColumns). Each field is a list of {@link ColumnMetadata} describing the columns of that result
 * set.
 */
public final class MetadataSchemas {

    public static final List<ColumnMetadata> TABLE_TYPES = ImmutableList.of(text("TABLE_TYPE"));

    public static final List<ColumnMetadata> CATALOGS = ImmutableList.of(text("TABLE_CAT"));

    public static final List<ColumnMetadata> SCHEMAS = ImmutableList.of(text("TABLE_SCHEM"), text("TABLE_CATALOG"));

    public static final List<ColumnMetadata> TABLES = ImmutableList.of(
            text("TABLE_CAT"),
            text("TABLE_SCHEM"),
            text("TABLE_NAME"),
            text("TABLE_TYPE"),
            text("REMARKS"),
            text("TYPE_CAT"),
            text("TYPE_SCHEM"),
            text("TYPE_NAME"),
            text("SELF_REFERENCING_COL_NAME"),
            text("REF_GENERATION"));

    public static final List<ColumnMetadata> TYPE_INFO = ImmutableList.of(
            text("TYPE_NAME"),
            integer("DATA_TYPE"),
            integer("PRECISION"),
            text("LITERAL_PREFIX"),
            text("LITERAL_SUFFIX"),
            text("CREATE_PARAMS"),
            shortColumn("NULLABLE"),
            text("CASE_SENSITIVE"),
            shortColumn("SEARCHABLE"),
            text("UNSIGNED_ATTRIBUTE"),
            text("FIXED_PREC_SCALE"),
            text("AUTO_INCREMENT"),
            text("LOCAL_TYPE_NAME"),
            shortColumn("MINIMUM_SCALE"),
            shortColumn("MAXIMUM_SCALE"),
            integer("SQL_DATA_TYPE"),
            integer("SQL_DATETIME_SUB"),
            integer("NUM_PREC_RADIX"));

    public static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
            text("TABLE_CAT"),
            text("TABLE_SCHEM"),
            text("TABLE_NAME"),
            text("COLUMN_NAME"),
            integer("DATA_TYPE"),
            text("TYPE_NAME"),
            integer("COLUMN_SIZE"),
            integer("BUFFER_LENGTH"),
            integer("DECIMAL_DIGITS"),
            integer("NUM_PREC_RADIX"),
            integer("NULLABLE"),
            text("REMARKS"),
            text("COLUMN_DEF"),
            integer("SQL_DATA_TYPE"),
            integer("SQL_DATETIME_SUB"),
            integer("CHAR_OCTET_LENGTH"),
            integer("ORDINAL_POSITION"),
            text("IS_NULLABLE"),
            text("SCOPE_CATALOG"),
            text("SCOPE_SCHEMA"),
            text("SCOPE_TABLE"),
            shortColumn("SOURCE_DATA_TYPE"),
            text("IS_AUTOINCREMENT"),
            text("IS_GENERATEDCOLUMN"));

    private static ColumnMetadata text(String name) {
        return new ColumnMetadata(name, HyperType.varcharUnlimited(true), TEXT);
    }

    private static ColumnMetadata integer(String name) {
        return new ColumnMetadata(name, HyperType.int32(true), INTEGER);
    }

    private static ColumnMetadata shortColumn(String name) {
        return new ColumnMetadata(name, HyperType.int16(true), SHORT);
    }

    private MetadataSchemas() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
