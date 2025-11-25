/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import java.sql.JDBCType;
import lombok.Value;

/**
 * Represents the type of a SQL column.
 *
 * Provides accessors for the various JDBC properties of the types.
 */
@Value
public class ColumnType {

    /// The SQL type
    private JDBCType type;
    /// For Array: the element type
    /// Unused for other types
    private ColumnType arrayElementType;
    /// For Numerics: the NUMERIC(precision, scale)
    /// For Char / Varchar: the length, or 0 for unlimited length
    /// Unused for other types
    private int precisionOrStringLength;
    /// For Numerics: the NUMERIC(precision, scale)
    /// Unused for other types
    private int scale;

    /// Is this type nullable?
    private boolean nullable;

    // For date/time types use this as placeholder for maximum display size
    private static final int MAX_DATETIME_DISPLAYSIZE = 128;
    // Used for types where we are not expected to return a precision
    private static final int UNKNOWN_PRECISION = 0;
    // Used for types where we are not expected to return a scale
    private static final int UNKNOWN_SCALE = 0;

    public ColumnType(JDBCType type) {
        this.type = type;
        this.arrayElementType = null;
        this.precisionOrStringLength = -1;
        this.scale = 1;
        nullable = true;
    }

    public ColumnType(JDBCType type, ColumnType arrayElementType) {
        this.type = type;
        this.arrayElementType = arrayElementType;
        this.precisionOrStringLength = -1;
        this.scale = 1;
        nullable = true;
    }

    public ColumnType(JDBCType type, int precision, int scale) {
        this.type = type;
        this.arrayElementType = null;
        this.precisionOrStringLength = precision;
        this.scale = scale;
        nullable = true;
    }

    public ColumnType(JDBCType type, boolean nullable) {
        this.type = type;
        this.arrayElementType = null;
        this.precisionOrStringLength = -1;
        this.scale = 1;
        this.nullable = nullable;
    }

    /**
     * Implements the semantics of java.sql.ResultSetMetaData.getPrecision().
     */
    public int getPrecisionOrStringLength() {
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
            case FLOAT:
                // Depends on extra_float_digits
                return 17;
            case REAL:
                // Depends on extra_float_digits
                return 8;
            case CHAR:
            case DECIMAL:
            case NUMERIC:
            case VARCHAR:
                return precisionOrStringLength;
            case BINARY:
            case VARBINARY:
                return Integer.MAX_VALUE;
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
                return arrayElementType.getPrecisionOrStringLength();
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
                return getPrecisionOrStringLength();
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
            case VARCHAR:
                return true;
            case ARRAY:
                return arrayElementType.isCaseSensitive();
            default:
                return false;
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
            case REAL:
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
                // The number of digits of precision + 1 for the sign
                return getPrecisionOrStringLength() + 1;
            case DECIMAL:
            case NUMERIC:
                if (scale > 0) {
                    // The number of digits of precision + 1 for the decimal point and +1 for thesign
                    return getPrecisionOrStringLength() + 2;
                } else {
                    // The number of digits of precision + 1 for the sign
                    return getPrecisionOrStringLength() + 1;
                }
            case REAL:
                return 15;
            case FLOAT:
            case DOUBLE:
                return 25;
            case ARRAY:
                return arrayElementType.getDisplaySize();
            default:
                return getPrecisionOrStringLength();
        }
    }

    // See table B.3 from https://download.oracle.com/otn-pub/jcp/jdbc-4_3-mrel3-eval-spec/jdbc4.3-fr-spec.pdf
    public String getJavaTypeName() {
        switch (type) {
            case BOOLEAN:
                return "java.lang.Boolean";
            case SMALLINT:
            case INTEGER:
                return "java.lang.Integer";
            case BIGINT:
                return "java.lang.Long";
            case NUMERIC:
                return "java.math.BigDecimal";
            case FLOAT:
                return "java.lang.Float";
            case DOUBLE:
                return "java.lang.Double";
            case CHAR:
            case VARCHAR:
                return "java.lang.String";
            case BINARY:
                return "[B";
            case DATE:
                return "java.sql.Date";
            case TIME:
                return "java.sql.Time";
            case TIMESTAMP:
                return "java.sql.Timestamp";
            case TIMESTAMP_WITH_TIMEZONE:
                return "java.sql.Timestamp";
            case ARRAY:
                return "java.sql.Array";
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
