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

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class QueryResultSetMetadata implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final List<Integer> columnTypeIds;

    public QueryResultSetMetadata(List<String> columnNames, List<String> columnTypes, List<Integer> columnTypeIds) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnTypeIds = columnTypeIds;
    }

    public QueryResultSetMetadata(QueryDBMetadata metadata) {
        this.columnNames = metadata.getColumnNames();
        this.columnTypes = metadata.getColumnTypes();
        this.columnTypeIds = metadata.getColumnTypeIds();
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        int columnType = getColumnType(column);
        switch (columnType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.BINARY:
                return getColumnName(column).length();
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.TINYINT:
                return getPrecision(column) + 1;
            case Types.DECIMAL:
                return getPrecision(column) + 1 + 1;
            case Types.DOUBLE:
                return 24;
            case Types.BOOLEAN:
                return 5;
            default:
                return 25;
        }
    }

    @Override
    public String getColumnLabel(int column) {
        if (columnNames == null || column > columnNames.size()) {
            return "C" + (column - 1);
        } else {
            return columnNames.get(column - 1);
        }
    }

    @Override
    public String getColumnName(int column) {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) {
        return StringUtils.EMPTY;
    }

    @Override
    public int getPrecision(int column) {
        int columnType = getColumnType(column);
        switch (columnType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.BIT:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return getColumnName(column).length();
            case Types.INTEGER:
            case Types.DECIMAL:
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.TINYINT:
                return 38;
            default:
                return 0;
        }
    }

    @Override
    public int getScale(int column) {
        int columnType = getColumnType(column);
        switch (columnType) {
            case Types.INTEGER:
            case Types.DECIMAL:
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.TINYINT:
                return 18;
            default:
                return 0;
        }
    }

    @Override
    public String getTableName(int column) {
        return StringUtils.EMPTY;
    }

    @Override
    public String getCatalogName(int column) {
        return StringUtils.EMPTY;
    }

    @Override
    public int getColumnType(int column) {
        return columnTypeIds.get(column - 1);
    }

    @Override
    public String getColumnTypeName(int column) {
        return columnTypes.get(column - 1);
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
