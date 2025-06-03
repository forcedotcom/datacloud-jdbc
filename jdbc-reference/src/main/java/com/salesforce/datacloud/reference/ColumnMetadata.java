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
}
