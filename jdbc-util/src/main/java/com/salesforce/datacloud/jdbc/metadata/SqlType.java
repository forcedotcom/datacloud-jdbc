package com.salesforce.datacloud.jdbc.metadata;

import java.sql.Types;

import lombok.Getter;

/**
 * Represents the type of a SQL column.
 * 
 * Instances of this class are immutable and created using static `create*` factory methods.
 *
 * Provides accessors for the various JDBC properties of the types.
 * Inspired by Postgres's JDBC driver.
 * See https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L90
 */
public class SqlType {
    public enum Type {
        Bool,
        SmallInt,
        Integer,
        BigInt,
        Numeric,
        Float,
        Double,
        Char,
        Varchar,
        Bytea,
        Date,
        Time,
        Timestamp,
        TimestampTZ,
        Interval,
        Array
    };

    /// The SQL type
    @Getter
    private final Type type;
    /// For Numerics: the NUMERIC(precision, scale)
    /// For Char / Varchar: the length, or 0 for unlimited length
    /// Unused for other types
    private final int precisionOrStringLength;
    /// For Numerics: the NUMERIC(precision, scale)
    /// Unused for other types
    private final int scale;
    /// For Array: the element type
    /// Unused for other types
    @Getter
    private final SqlType elementType;
    /// Is this type nullable?
    @Getter
    private final boolean nullable;

    /// Returns the SQL type name, as represented in SQL
    public String toString() {
        switch (type) {
            case Bool:
                return "BOOLEAN";
            case SmallInt:
                return "SMALLINT";
            case Integer:
                return "INTEGER";
            case BigInt:
                return "BIGINT";
            case Numeric:
                return "NUMERIC(" + precisionOrStringLength + ", " + scale + ")";
            case Float:
                return "REAL";
            case Double:
                return "DOUBLE PRECISION";
            case Char:
                return "CHAR(" + precisionOrStringLength + ")";
            case Varchar:
                if (precisionOrStringLength == 0) {
                    return "VARCHAR";
                } else {
                    return "VARCHAR(" + precisionOrStringLength + ")";
                }
            case Bytea:
                return "BYTEA";
            case Date:
                return "DATE";
            case Time:
                return "TIME";
            case Timestamp:
                return "TIMESTAMP";
            case TimestampTZ:
                return "TIMESTAMPTZ";
            case Interval:
                return "INTERVAL";
            case Array:
                return "ARRAY(" + elementType.toString() + ")";
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    ///////////////////////
    // JDBC type mapping //
    ///////////////////////

    public int getJdbcType() {
        switch (type) {
            case Bool:
                return Types.BOOLEAN;
            case SmallInt:
                return Types.SMALLINT;
            case Integer:
                return Types.INTEGER;
            case BigInt:
                return Types.BIGINT;
            case Numeric:
                return Types.NUMERIC;
            case Float:
                return Types.FLOAT;
            case Double:
                return Types.DOUBLE;
            case Char:
                return Types.CHAR;
            case Varchar:
                return Types.VARCHAR;
            case Bytea:
                return Types.BINARY;
            case Date:
                return Types.DATE;
            case Time:
                return Types.TIME;
            case Timestamp:
                return Types.TIMESTAMP;
            case TimestampTZ:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case Interval:
                return Types.JAVA_OBJECT;
            case Array:
                return Types.ARRAY;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    // See table B.3 from https://download.oracle.com/otn-pub/jcp/jdbc-4_3-mrel3-eval-spec/jdbc4.3-fr-spec.pdf
    public String getJavaTypeName() {
        switch (type) {
            case Bool:
                return "java.lang.Boolean";
            case SmallInt:
            case Integer:
                return "java.lang.Integer";
            case BigInt:
                return "java.lang.Long";
            case Numeric:
                return "java.math.BigDecimal";
            case Float:
                return "java.lang.Float";
            case Double:
                return "java.lang.Double";
            case Char:
            case Varchar:
                return "java.lang.String";
            case Bytea:
                return "[B";
            case Date:
                return "java.sql.Date";
            case Time:
                return "java.sql.Time";
            case Timestamp:
                return "java.sql.Timestamp";
            case TimestampTZ:
                return "java.sql.Timestamp";
            case Interval:
                return "java.time.Duration";
            case Array:
                return "java.sql.Array";
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public boolean isSigned() {
        switch (type) {
            case SmallInt:
            case Integer:
            case BigInt:
            case Float:
            case Double:
            case Numeric:
            case Interval:
                return true;
            case Bool:
            case Char:
            case Varchar:
            case Bytea:
            case Date:
            case Time:
            case Timestamp:
            case TimestampTZ:
            case Array:
                return false;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public int getPrecision() {
        switch (type) {
            case Bool:
                return 1;
            // "For numeric data, this is the maximum precision."
            case SmallInt:
                return 5;
            case Integer:
                return 10;
            case BigInt:
                return 19;
            case Float:
                // For float4 and float8, we can normally only get 6 and 15
                // significant digits out, but extra_float_digits may raise
                // that number by up to two digits.
                return 8;
            case Double:
                return 17;
            case Numeric:
                return precisionOrStringLength;
            case Char:
            case Varchar:
                // "For character data, this is the maximum length of the character string."
                return precisionOrStringLength == 0 ? Integer.MAX_VALUE : precisionOrStringLength;
            case Bytea:
                return Integer.MAX_VALUE;
            case Date:
            case Time:
            case Timestamp:
            case TimestampTZ:
            case Interval:
                // "For datetime datatypes, this is the length in characters of the String representation
                // (assuming the maximum allowed precision of the fractional seconds component)."
                return getDisplaySize();
            case Array:
                return Integer.MAX_VALUE;
        }
        return Integer.MAX_VALUE;
    }

    public int getScale() {
        // Based on https://github.com/pgjdbc/pgjdbc/blob/d9e20874590f59543c39a99b824e09344f00a813/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L862
        switch (type) {
            case Float:
                return 8;
            case Double:
                return 17;
            case Numeric:
                return scale;
            case Time:
            case Timestamp:
            case TimestampTZ:
            case Interval:
                return 6;
            default:
                // "0 is returned for data types where the scale is not applicable."
                return 0;
        }
    }

    public int getDisplaySize() {
        // Based on https://github.com/pgjdbc/pgjdbc/blob/d9e20874590f59543c39a99b824e09344f00a813/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L935
        switch (type) {
            case Bool:
                return 1;
            case SmallInt:
                return 6; // -32768 to +32767
            case Integer:
                return 11; // -2147483648 to +2147483647
            case BigInt:
                return 20; // -9223372036854775808 to +9223372036854775807
            case Float:
                // Varies based upon the extra_float_digits GUC.
                // These values are for the longest possible length.
                return 15; // sign + 9 digits + decimal point + e + sign + 2 digits
            case Double:
                return 25; // sign + 18 digits + decimal point + e + sign + 3 digits
            case Numeric:
                // sign + digits + decimal point (only if we have nonzero scale)
                return 1 + precisionOrStringLength + (scale > 0 ? 1 : 0);
            case Char:
            case Varchar:
                return precisionOrStringLength > 0 ? precisionOrStringLength : Integer.MAX_VALUE;
            case Bytea:
                return Integer.MAX_VALUE;    
            case Time:
                return 15; // '00:00:00.123456'::time
            case Date:
                return 13; // '294276-11-20'"::date"
            case Timestamp:
                return 28; // '294276-11-20 12:34:56.123456'::timestamp
            case TimestampTZ:
                // zone = '+11:30' = 6;
                return 28 + 6;
            case Interval:
                return 49; // "-123456789 years 11 months 33 days 23 hours 10.123456 seconds"
            case Array:
                return Integer.MAX_VALUE;
        }
        return Integer.MAX_VALUE;
    }

    ///////////////////////
    // Factory methods   //
    ///////////////////////

    /**
     * Private constructor for use by factory methods.
     */
    private SqlType(Type type, int scale, int precisionOrStringLength, boolean nullable, SqlType elementType) {
        this.type = type;
        this.scale = scale;
        this.precisionOrStringLength = precisionOrStringLength;
        this.nullable = nullable;
        this.elementType = elementType;
    }

    public static SqlType createBool() {
        return new SqlType(Type.Bool, 0, 0, true, null);
    }

    public static SqlType createSmallInt() {
        return new SqlType(Type.SmallInt, 0, 0, true, null);
    }

    public static SqlType createInteger() {
        return new SqlType(Type.Integer, 0, 0, true, null);
    }

    public static SqlType createBigInt() {
        return new SqlType(Type.BigInt, 0, 0, true, null);
    }

    public static SqlType createNumeric(int precision, int scale) {
        return new SqlType(Type.Numeric, scale, precision, true, null);
    }

    public static SqlType createFloat() {
        return new SqlType(Type.Float, 0, 0, true, null);
    }

    public static SqlType createDouble() {
        return new SqlType(Type.Double, 0, 0, true, null);
    }

    public static SqlType createChar(int length) {
        assert(length > 0);
        return new SqlType(Type.Char, length, 0, true, null);
    }

    public static SqlType createVarchar(int length) {
        assert(length >= 0);
        return new SqlType(Type.Varchar, length, 0, true, null);
    }

    public static SqlType createBytea() {
        return new SqlType(Type.Bytea, 0, 0, true, null);
    }

    public static SqlType createDate() {
        return new SqlType(Type.Date, 0, 0, true, null);
    }

    public static SqlType createTime() {
        return new SqlType(Type.Time, 0, 0, true, null);
    }

    public static SqlType createTimestamp() {
        return new SqlType(Type.Timestamp, 0, 0, true, null);
    }

    public static SqlType createTimestampTZ() {
        return new SqlType(Type.TimestampTZ, 0, 0, true, null);
    }

    public static SqlType createInterval() {
        return new SqlType(Type.Interval, 0, 0, true, null);
    }

    public static SqlType createArray(SqlType elementType) {
        return new SqlType(Type.Array, 0, 0, true, elementType);
    }
}