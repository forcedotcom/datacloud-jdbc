/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

/**
 * Represents a field / column in a result set.
 *
 * <p>The {@link HyperType} carries the full internal type model. {@link #getTypeName()} is an
 * optional caller-provided override for {@link java.sql.ResultSetMetaData#getColumnTypeName(int)};
 * when {@code null}, the JDBC layer falls back to the default name derived from the
 * {@link HyperType}.
 */
@lombok.Value
public class ColumnMetadata {
    String name;
    HyperType type;
    /**
     * Optional override for {@code ResultSetMetaData.getColumnTypeName}; {@code null} means the
     * JDBC layer should use the default derived from {@link #type}. Used for metadata result sets
     * (e.g. {@code DatabaseMetaData.getTables}) where the JDBC spec pins a specific column-type
     * label that differs from the underlying Hyper type.
     */
    String typeName;

    /** Shorthand for a column with no caller-supplied type-name override. */
    public ColumnMetadata(String name, HyperType type) {
        this(name, type, null);
    }

    /** Full constructor (kept for callers that need to override the JDBC type-name label). */
    public ColumnMetadata(String name, HyperType type, String typeName) {
        this.name = name;
        this.type = type;
        this.typeName = typeName;
    }
}
