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
package com.salesforce.datacloud.jdbc.util;

import java.sql.JDBCType;
import lombok.Getter;

@Getter
public class ColumnType {

    private JDBCType type;
    private ColumnType arrayElementType;
    private int precision;
    private int scale;

    // Used for variable length types, use 1GB as reasonably large number for now
    public static final int MAX_VARLEN_PRECISION = Integer.MAX_VALUE;
    // For date/time types use this as placeholder for maximum display size
    private static final int MAX_DATETIME_DISPLAYSIZE = 128;
    // Used for types where we are not expected to return a precision
    private static final int UNKNOWN_PRECISION = 0;
    // Used for types where we are not expected to return a scale
    private static final int UNKNOWN_SCALE = 0;

    public ColumnType(JDBCType type) {
        this.type = type;
    }

    public ColumnType(JDBCType type, ColumnType arrayElementType) {
        this.type = type;
        this.arrayElementType = arrayElementType;
    }

    public ColumnType(JDBCType type, int precision, int scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Implements the semantics of java.sql.ResultSetMetaData.getPrecision().
     */
    public int getPrecision() {
        // TODO: Add proper handling for Array
        switch (type) {
            case BIT:
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case DOUBLE:
                // Depends on extra_float_digits
                return 17;
            case FLOAT:
            case REAL:
                // Depends on extra_float_digits
                return 8;
            case CHAR:
            case DECIMAL:
            case NUMERIC:
            case VARCHAR:
                return precision;
            case BINARY:
            case VARBINARY:
                return MAX_VARLEN_PRECISION;
            // For dates / times use numbers that are sufficiently large for display
            case DATE:
                return 13;
            case TIME:
                return 15;
            case TIMESTAMP:
                return 29;
            case TIME_WITH_TIMEZONE:
                return MAX_DATETIME_DISPLAYSIZE;
            case TIMESTAMP_WITH_TIMEZONE:
                return 35;
            case NULL:
            case JAVA_OBJECT:
            case BLOB:
            case CLOB:
                return UNKNOWN_PRECISION;
            case ARRAY:
                return arrayElementType.getPrecision();
            case LONGVARCHAR:
            case LONGVARBINARY:
            case OTHER:
            case DISTINCT:
            case STRUCT:
            case REF:
            case DATALINK:
            case ROWID:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case SQLXML:
            case REF_CURSOR:
                break;
        }
        throw new IllegalArgumentException("This type should never occur " + type);
    }

    /**
     * Implements the semantics of java.sql.ResultSetMetaData.getScale().
     */
    public int getScale() {
        // TODO: Add proper handling for Array
        switch (type) {
            case BIT:
            case BOOLEAN:
            case CHAR:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return UNKNOWN_SCALE;
            case DOUBLE:
            case FLOAT:
            case REAL:
                return getPrecision();
            case DECIMAL:
            case NUMERIC:
                return scale;
            // For dates / times use numbers that are sufficiently large for display
            case DATE:
                return UNKNOWN_SCALE;
            case TIME:
            case TIMESTAMP_WITH_TIMEZONE:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP:
                // Support microsecond fractions
                return 6;
            case NULL:
            case JAVA_OBJECT:
            case BLOB:
            case CLOB:
                return UNKNOWN_PRECISION;
            case ARRAY:
                return arrayElementType.getScale();
            case LONGVARCHAR:
            case LONGVARBINARY:
            case OTHER:
            case DISTINCT:
            case STRUCT:
            case REF:
            case DATALINK:
            case ROWID:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case SQLXML:
            case REF_CURSOR:
                break;
        }
        throw new IllegalArgumentException("This type should never occur " + type);
    }

    /**
     * Returns true if the column type is case sensitive.
     */
    public boolean isCaseSensitive() {
        switch (type) {
            case CHAR:
            case VARBINARY:
            case VARCHAR: return true;
            case ARRAY:
                return arrayElementType.isCaseSensitive();
            default: return false;
        }
    }

    /**
     * Returns true if the column type is signed.
     */
    public boolean isSigned() {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case NUMERIC:
                return true;
            case ARRAY:
                return arrayElementType.isSigned();
            default:
                return false;
        }
    }

    /**
     * Returns the display size of the column type.
     */
    public int getDisplaySize() {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getPrecision()+1;
            case DECIMAL:
            case NUMERIC:
                if (scale > 0) {
                    return getPrecision()+2;
                } else {
                    return getPrecision()+1;
                }
            case FLOAT:
            case REAL:
                return 15;
            case DOUBLE:
                return 25;
            case ARRAY:
                return arrayElementType.getDisplaySize();
            default:
                return getPrecision();
        }
    }
}
