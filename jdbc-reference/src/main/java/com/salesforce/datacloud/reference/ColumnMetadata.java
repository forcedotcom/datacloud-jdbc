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
package com.salesforce.datacloud.reference;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Represents metadata for a single column in a `ResultSetMetaData`.
 * Provides JSON serialization/deserialization and comparison capabilities.
 */
@Data
@Builder
@Jacksonized
public class ColumnMetadata {

    private String columnName;
    private String columnLabel;
    private int columnType;
    private String columnTypeName;
    private int columnDisplaySize;
    private int precision;
    private int scale;
    private int isNullable;
    private boolean autoIncrement;
    private boolean caseSensitive;
    private boolean currency;
    private boolean definitelyWritable;
    private boolean readOnly;
    private boolean searchable;
    private boolean signed;
    private boolean writable;

    private String catalogName;
    private String schemaName;
    private String tableName;

    /**
     * Creates ColumnMetadata from ResultSetMetaData for a specific column.
     *
     * @param metaData the ResultSetMetaData
     * @param columnIndex the column index (1-based)
     * @return ColumnMetadata instance
     * @throws SQLException if metadata extraction fails
     */
    public static ColumnMetadata fromResultSetMetaData(ResultSetMetaData metaData, int columnIndex)
            throws SQLException {
        return new ColumnMetadata(
                metaData.getColumnName(columnIndex),
                metaData.getColumnLabel(columnIndex),
                metaData.getColumnType(columnIndex),
                metaData.getColumnTypeName(columnIndex),
                metaData.getColumnDisplaySize(columnIndex),
                metaData.getPrecision(columnIndex),
                metaData.getScale(columnIndex),
                metaData.isNullable(columnIndex),
                metaData.isAutoIncrement(columnIndex),
                metaData.isCaseSensitive(columnIndex),
                metaData.isCurrency(columnIndex),
                metaData.isDefinitelyWritable(columnIndex),
                metaData.isReadOnly(columnIndex),
                metaData.isSearchable(columnIndex),
                metaData.isSigned(columnIndex),
                metaData.isWritable(columnIndex),
                metaData.getCatalogName(columnIndex),
                metaData.getSchemaName(columnIndex),
                metaData.getTableName(columnIndex));
    }

    /**
     * Collects differences between this ColumnMetadata instance and another instance field by field.
     * Returns a list of strings describing the mismatches.
     *
     * @param other the ColumnMetadata instance to compare against
     * @return list of strings describing the mismatches
     */
    public List<String> collectDifferences(ColumnMetadata other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot validate against null ColumnMetadata");
        }

        ArrayList<String> differences = new ArrayList<>();

        if (this.columnType != other.columnType) {
            StringBuilder errors = new StringBuilder();
            errors.append("columnType mismatch: expected=")
                    .append(other.columnType)
                    .append(", actual=")
                    .append(this.columnType)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (!java.util.Objects.equals(this.columnTypeName, other.columnTypeName)) {
            StringBuilder errors = new StringBuilder();
            errors.append("columnTypeName mismatch: expected='")
                    .append(other.columnTypeName)
                    .append("', actual='")
                    .append(this.columnTypeName)
                    .append("'\n");
            differences.add(errors.toString());
        }

        if (this.columnDisplaySize != other.columnDisplaySize) {
            StringBuilder errors = new StringBuilder();
            errors.append("columnDisplaySize mismatch: expected=")
                    .append(other.columnDisplaySize)
                    .append(", actual=")
                    .append(this.columnDisplaySize)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.precision != other.precision) {
            StringBuilder errors = new StringBuilder();
            errors.append("precision mismatch: expected=")
                    .append(other.precision)
                    .append(", actual=")
                    .append(this.precision)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.scale != other.scale) {
            StringBuilder errors = new StringBuilder();
            errors.append("scale mismatch: expected=")
                    .append(other.scale)
                    .append(", actual=")
                    .append(this.scale)
                    .append("\n");
            differences.add(errors.toString());
        }

        /* PostgreSQL always returns `columnNullableUnknown` even if the query has a const null result. In Hyper we are more fine grained.
         * Thus we allow `unknown` to match both `columnNullable` and `columnNoNulls`. In the expectation checks we manually force the reference expectation
         * to `columnNullable` if the query has a const null result, as Hyper always returns a nullable type in that case.
         */
        if (this.isNullable != other.isNullable && (other.isNullable != ResultSetMetaData.columnNullableUnknown)) {
            StringBuilder errors = new StringBuilder();
            errors.append("isNullable mismatch: expected=")
                    .append(other.isNullable)
                    .append(", actual=")
                    .append(this.isNullable)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.autoIncrement != other.autoIncrement) {
            StringBuilder errors = new StringBuilder();
            errors.append("autoIncrement mismatch: expected=")
                    .append(other.autoIncrement)
                    .append(", actual=")
                    .append(this.autoIncrement)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.caseSensitive != other.caseSensitive) {
            StringBuilder errors = new StringBuilder();
            errors.append("caseSensitive mismatch: expected=")
                    .append(other.caseSensitive)
                    .append(", actual=")
                    .append(this.caseSensitive)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.currency != other.currency) {
            StringBuilder errors = new StringBuilder();
            errors.append("currency mismatch: expected=")
                    .append(other.currency)
                    .append(", actual=")
                    .append(this.currency)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.definitelyWritable != other.definitelyWritable) {
            StringBuilder errors = new StringBuilder();
            errors.append("definitelyWritable mismatch: expected=")
                    .append(other.definitelyWritable)
                    .append(", actual=")
                    .append(this.definitelyWritable)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.readOnly != other.readOnly) {
            StringBuilder errors = new StringBuilder();
            errors.append("readOnly mismatch: expected=")
                    .append(other.readOnly)
                    .append(", actual=")
                    .append(this.readOnly)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.searchable != other.searchable) {
            StringBuilder errors = new StringBuilder();
            errors.append("searchable mismatch: expected=")
                    .append(other.searchable)
                    .append(", actual=")
                    .append(this.searchable)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.signed != other.signed) {
            StringBuilder errors = new StringBuilder();
            errors.append("signed mismatch: expected=")
                    .append(other.signed)
                    .append(", actual=")
                    .append(this.signed)
                    .append("\n");
            differences.add(errors.toString());
        }

        if (this.writable != other.writable) {
            StringBuilder errors = new StringBuilder();
            errors.append("writable mismatch: expected=")
                    .append(other.writable)
                    .append(", actual=")
                    .append(this.writable)
                    .append("\n");
            differences.add(errors.toString());
        }

        // Consciously ignore labels and names as they can differ between databases
        // - catalogName
        // - schemaName
        // - tableName
        // - columnLabel
        // - columnName

        return differences;
    }
}
