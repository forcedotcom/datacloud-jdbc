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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    // We have to explicitly give names for boolean properties because Jackson auto name generation doesn't work for
    // them
    @JsonProperty("autoIncrement")
    private boolean isAutoIncrement;

    @JsonProperty("caseSensitive")
    private boolean isCaseSensitive;

    @JsonProperty("currency")
    private boolean isCurrency;

    @JsonProperty("definitelyWritable")
    private boolean isDefinitelyWritable;

    @JsonProperty("readOnly")
    private boolean isReadOnly;

    @JsonProperty("searchable")
    private boolean isSearchable;

    @JsonProperty("signed")
    private boolean isSigned;

    @JsonProperty("writable")
    private boolean isWritable;

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
     * Validates this ColumnMetadata instance against another instance field by field.
     * Throws an IllegalArgumentException if any fields don't match.
     *
     * @param other the ColumnMetadata instance to compare against
     * @throws IllegalArgumentException if any fields don't match
     */
    public void validateAgainst(ColumnMetadata other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot validate against null ColumnMetadata");
        }

        StringBuilder errors = new StringBuilder();
        if (this.columnType != other.columnType) {
            errors.append("columnType mismatch: expected=")
                    .append(other.columnType)
                    .append(", actual=")
                    .append(this.columnType)
                    .append("\n");
        }

        if (!java.util.Objects.equals(this.columnTypeName, other.columnTypeName)) {
            errors.append("columnTypeName mismatch: expected='")
                    .append(other.columnTypeName)
                    .append("', actual='")
                    .append(this.columnTypeName)
                    .append("'\n");
        }

        if (this.columnDisplaySize != other.columnDisplaySize) {
            errors.append("columnDisplaySize mismatch: expected=")
                    .append(other.columnDisplaySize)
                    .append(", actual=")
                    .append(this.columnDisplaySize)
                    .append("\n");
        }

        if (this.precision != other.precision) {
            errors.append("precision mismatch: expected=")
                    .append(other.precision)
                    .append(", actual=")
                    .append(this.precision)
                    .append("\n");
        }

        if (this.scale != other.scale) {
            errors.append("scale mismatch: expected=")
                    .append(other.scale)
                    .append(", actual=")
                    .append(this.scale)
                    .append("\n");
        }

        if (this.isNullable != other.isNullable && (other.isNullable != ResultSetMetaData.columnNullableUnknown)) {
            errors.append("isNullable mismatch: expected=")
                    .append(other.isNullable)
                    .append(", actual=")
                    .append(this.isNullable)
                    .append("\n");
        }

        if (this.isAutoIncrement != other.isAutoIncrement) {
            errors.append("isAutoIncrement mismatch: expected=")
                    .append(other.isAutoIncrement)
                    .append(", actual=")
                    .append(this.isAutoIncrement)
                    .append("\n");
        }

        if (this.isCaseSensitive != other.isCaseSensitive) {
            errors.append("isCaseSensitive mismatch: expected=")
                    .append(other.isCaseSensitive)
                    .append(", actual=")
                    .append(this.isCaseSensitive)
                    .append("\n");
        }

        if (this.isCurrency != other.isCurrency) {
            errors.append("isCurrency mismatch: expected=")
                    .append(other.isCurrency)
                    .append(", actual=")
                    .append(this.isCurrency)
                    .append("\n");
        }

        if (this.isDefinitelyWritable != other.isDefinitelyWritable) {
            errors.append("isDefinitelyWritable mismatch: expected=")
                    .append(other.isDefinitelyWritable)
                    .append(", actual=")
                    .append(this.isDefinitelyWritable)
                    .append("\n");
        }

        if (this.isReadOnly != other.isReadOnly) {
            errors.append("isReadOnly mismatch: expected=")
                    .append(other.isReadOnly)
                    .append(", actual=")
                    .append(this.isReadOnly)
                    .append("\n");
        }

        if (this.isSearchable != other.isSearchable) {
            errors.append("isSearchable mismatch: expected=")
                    .append(other.isSearchable)
                    .append(", actual=")
                    .append(this.isSearchable)
                    .append("\n");
        }

        if (this.isSigned != other.isSigned) {
            errors.append("isSigned mismatch: expected=")
                    .append(other.isSigned)
                    .append(", actual=")
                    .append(this.isSigned)
                    .append("\n");
        }

        if (this.isWritable != other.isWritable) {
            errors.append("isWritable mismatch: expected=")
                    .append(other.isWritable)
                    .append(", actual=")
                    .append(this.isWritable)
                    .append("\n");
        }

        // Consciously ignore labels and names as they can differ between databases
        // - catalogName
        // - schemaName
        // - tableName
        // - columnLabel
        // - columnName

        // If there are any errors, throw exception with all details
        if (errors.length() > 0) {
            throw new IllegalArgumentException("ColumnMetadata validation failed:\n" + errors.toString());
        }
    }
}
