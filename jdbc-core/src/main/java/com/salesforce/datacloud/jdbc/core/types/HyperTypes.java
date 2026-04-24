/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.types;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeKind;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bridge between {@link HyperType} and the public JDBC surface (precision, scale, display size,
 * Java class, JDBC type code and name, etc.).
 *
 * <p>All JDBC-spec behaviour for column metadata lives here, so callers like
 * {@code DataCloudResultSetMetaData}, {@code QueryMetadataUtil} and {@code DataCloudArray} do
 * not each re-implement their own type-classification logic.
 */
public final class HyperTypes {

    // Used for date / time types as a placeholder for maximum display size.
    private static final int MAX_DATETIME_DISPLAYSIZE = 128;
    private static final int UNKNOWN_PRECISION = 0;
    private static final int UNKNOWN_SCALE = 0;
    private static final int UNLIMITED_PRECISION = Integer.MAX_VALUE;

    private HyperTypes() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    // ----------------------------------------------------------------------
    // JDBC type code / name
    // ----------------------------------------------------------------------

    /** The {@link JDBCType} this Hyper type is presented as over the JDBC API. */
    public static JDBCType toJdbcType(HyperType t) {
        switch (t.getKind()) {
            case BOOL:
                return JDBCType.BOOLEAN;
            case INT8:
                return JDBCType.TINYINT;
            case INT16:
                return JDBCType.SMALLINT;
            case INT32:
                return JDBCType.INTEGER;
            case INT64:
                return JDBCType.BIGINT;
            case OID:
                // Hyper's OID is an unsigned 32-bit internal type. We surface it as BIGINT to
                // match the pgjdbc reference (see JDBCReferenceTest): pgjdbc reports oid with
                // Types.BIGINT, which keeps `getLong("oid_col")` working for integer-treating
                // callers. HyperType.oid stays a distinct kind so the pg_catalog path can keep
                // precision/display accurate (10 digits for the u32 range).
                return JDBCType.BIGINT;
            case FLOAT4:
                return JDBCType.REAL;
            case FLOAT8:
                return JDBCType.DOUBLE;
            case DECIMAL:
                return JDBCType.DECIMAL;
            case CHAR:
                return JDBCType.CHAR;
            case VARCHAR:
                return JDBCType.VARCHAR;
            case BINARY:
                return JDBCType.BINARY;
            case VARBINARY:
                return JDBCType.VARBINARY;
            case DATE:
                return JDBCType.DATE;
            case TIME:
                return JDBCType.TIME;
            case TIME_TZ:
                return JDBCType.TIME_WITH_TIMEZONE;
            case TIMESTAMP:
                return JDBCType.TIMESTAMP;
            case TIMESTAMP_TZ:
                return JDBCType.TIMESTAMP_WITH_TIMEZONE;
            case ARRAY:
                return JDBCType.ARRAY;
            case NULL:
                return JDBCType.NULL;
            case INTERVAL:
            case JSON:
            case UNKNOWN:
                return JDBCType.OTHER;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    /** The {@link java.sql.Types} integer code this Hyper type is presented as over the JDBC API. */
    public static int toJdbcTypeCode(HyperType t) {
        return toJdbcType(t).getVendorTypeNumber();
    }

    /**
     * Bridge from a {@link java.sql.Types} integer code to a {@link HyperType}.
     *
     * <p>Used by places that still take a raw JDBC type code on input (e.g.
     * {@link java.sql.PreparedStatement#setNull(int, int)}). For types that carry precision or
     * scale ({@code DECIMAL}, {@code CHAR}, {@code VARCHAR}, {@code BINARY}) the returned
     * {@link HyperType} uses placeholder values; callers that know the actual precision should
     * use the {@link HyperType} factory methods directly instead.
     */
    public static HyperType fromJdbcTypeCode(int jdbcTypeCode, boolean nullable) {
        switch (jdbcTypeCode) {
            case Types.BIT:
            case Types.BOOLEAN:
                return HyperType.bool(nullable);
            case Types.TINYINT:
                return HyperType.int8(nullable);
            case Types.SMALLINT:
                return HyperType.int16(nullable);
            case Types.INTEGER:
                return HyperType.int32(nullable);
            case Types.BIGINT:
                return HyperType.int64(nullable);
            case Types.REAL:
                return HyperType.float4(nullable);
            case Types.FLOAT:
            case Types.DOUBLE:
                return HyperType.float8(nullable);
            case Types.DECIMAL:
            case Types.NUMERIC:
                return HyperType.decimal(0, 0, nullable);
            case Types.CHAR:
                return HyperType.fixedChar(1, nullable);
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return HyperType.varcharUnlimited(nullable);
            case Types.BINARY:
                return HyperType.binary(0, nullable);
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return HyperType.varbinary(nullable);
            case Types.DATE:
                return HyperType.date(nullable);
            case Types.TIME:
                return HyperType.time(nullable);
            case Types.TIME_WITH_TIMEZONE:
                return HyperType.timeTz(nullable);
            case Types.TIMESTAMP:
                return HyperType.timestamp(nullable);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return HyperType.timestampTz(nullable);
            case Types.ARRAY:
                return HyperType.array(HyperType.varcharUnlimited(nullable), nullable);
            case Types.NULL:
                return HyperType.nullType();
        }
        throw new IllegalArgumentException("Unsupported JDBC type code: " + jdbcTypeCode);
    }

    /**
     * The JDBC-conventional uppercase type name (e.g. {@code "BIGINT"}, {@code "VARCHAR"}).
     *
     * <p>For Hyper-specific kinds without a JDBC-standard name
     * ({@link HyperTypeKind#INTERVAL}, {@link HyperTypeKind#JSON}) the uppercase Hyper name is
     * returned instead of the generic {@code "OTHER"}. For {@link HyperTypeKind#UNKNOWN} the raw
     * {@code pg_catalog.format_type()} string is preserved verbatim so callers can still see the
     * source-of-truth name of a Hyper type the driver does not model.
     */
    public static String toJdbcTypeName(HyperType t) {
        switch (t.getKind()) {
            case INTERVAL:
                return "INTERVAL";
            case JSON:
                return "JSON";
            case UNKNOWN:
                return t.getUnknownTypeName();
            default:
                return toJdbcType(t).getName();
        }
    }

    // ----------------------------------------------------------------------
    // JDBC semantics (ResultSetMetaData-style queries)
    // ----------------------------------------------------------------------

    /** Implements {@link java.sql.ResultSetMetaData#getColumnClassName(int)}. */
    public static Class<?> toJavaClass(HyperType t) {
        switch (t.getKind()) {
            case BOOL:
                return Boolean.class;
            case INT8:
                // Historically the driver surfaces TINYINT as java.lang.Short (instead of Integer
                // suggested by JDBC 4.3 Table B.3). Preserved for backward compatibility.
                return Short.class;
            case INT16:
            case INT32:
                return Integer.class;
            case INT64:
            case OID:
                return Long.class;
            case FLOAT4:
                return Float.class;
            case FLOAT8:
                return Double.class;
            case DECIMAL:
                return BigDecimal.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DATE:
                return Date.class;
            case TIME:
            case TIME_TZ:
                return Time.class;
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return Timestamp.class;
            case ARRAY:
                return Array.class;
            case NULL:
                return Object.class;
            case INTERVAL:
            case JSON:
                return String.class;
            case UNKNOWN:
                // The driver cannot accessor-convert UNKNOWN; callers only get the row byte-for-byte
                // via getObject(), so we advertise Object.
                return Object.class;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    /** Implements {@link java.sql.ResultSetMetaData#getPrecision(int)}. */
    public static int getPrecision(HyperType t) {
        switch (t.getKind()) {
            case BOOL:
                return 1;
            case INT8:
                return 3;
            case INT16:
                return 5;
            case INT32:
                return 10;
            case INT64:
                return 19;
            case OID:
                // Unsigned 32-bit: max 4294967295, 10 digits. Presented as BIGINT, but precision
                // reflects the actual value range.
                return 10;
            case FLOAT4:
                return 8;
            case FLOAT8:
                return 17;
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return t.getPrecision();
            case DATE:
                return 13;
            case TIME:
                return 15;
            case TIME_TZ:
                return MAX_DATETIME_DISPLAYSIZE;
            case TIMESTAMP:
                return 29;
            case TIMESTAMP_TZ:
                return 35;
            case ARRAY:
                return getPrecision(t.getElement());
            case NULL:
                return UNKNOWN_PRECISION;
            case INTERVAL:
                // Covers year-month and day-time intervals up to the largest Postgres-style width.
                return 49;
            case JSON:
                return UNLIMITED_PRECISION;
            case UNKNOWN:
                return UNKNOWN_PRECISION;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    /** Implements {@link java.sql.ResultSetMetaData#getScale(int)}. */
    public static int getScale(HyperType t) {
        switch (t.getKind()) {
            case BOOL:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case OID:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
                return UNKNOWN_SCALE;
            case FLOAT4:
            case FLOAT8:
                // Match Postgres JDBC: for binary floats it reports scale == precision (8 for
                // REAL, 17 for DOUBLE). Binary floats have no decimal scale, so this is a
                // display convention rather than a meaningful fractional-digit count.
                return getPrecision(t);
            case DECIMAL:
                return t.getScale();
            case TIME:
            case TIME_TZ:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                // Microsecond fractions.
                return 6;
            case ARRAY:
                return getScale(t.getElement());
            case NULL:
            case INTERVAL:
            case JSON:
            case UNKNOWN:
                return UNKNOWN_SCALE;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    /** Implements {@link java.sql.ResultSetMetaData#getColumnDisplaySize(int)}. */
    public static int getDisplaySize(HyperType t) {
        switch (t.getKind()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case OID:
                // Number of digits of precision + 1 for the sign.
                return getPrecision(t) + 1;
            case DECIMAL:
                if (t.getScale() > 0) {
                    // precision + 1 for decimal point + 1 for sign
                    return getPrecision(t) + 2;
                }
                return getPrecision(t) + 1;
            case FLOAT4:
                return 15;
            case FLOAT8:
                return 25;
            case ARRAY:
                return getDisplaySize(t.getElement());
            default:
                return getPrecision(t);
        }
    }

    /** Implements {@link java.sql.ResultSetMetaData#isSigned(int)}. */
    public static boolean isSigned(HyperType t) {
        switch (t.getKind()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case OID:
            case FLOAT4:
            case FLOAT8:
            case DECIMAL:
                return true;
            case ARRAY:
                return isSigned(t.getElement());
            default:
                return false;
        }
    }

    /** Implements {@link java.sql.ResultSetMetaData#isCaseSensitive(int)}. */
    public static boolean isCaseSensitive(HyperType t) {
        switch (t.getKind()) {
            case CHAR:
            case VARCHAR:
            case VARBINARY:
            case JSON:
                return true;
            case ARRAY:
                return isCaseSensitive(t.getElement());
            default:
                return false;
        }
    }

    /** Whether this type is nullable. */
    public static boolean isNullable(HyperType t) {
        return t.isNullable();
    }

    // ----------------------------------------------------------------------
    // getColumns helpers (see QueryMetadataUtil)
    // ----------------------------------------------------------------------

    /**
     * {@code true} when {@code DECIMAL_DIGITS} should be populated for this type.
     *
     * <p>Only fixed-scale decimals qualify. Binary floating-point types ({@code REAL}, {@code
     * DOUBLE}) have no meaningful decimal scale — their precision is an exponent-driven property,
     * not a digit count — so we leave {@code DECIMAL_DIGITS} at {@code 0}.
     */
    public static boolean needsDecimalDigits(HyperType t) {
        return t.getKind() == HyperTypeKind.DECIMAL;
    }

    /** {@code true} when {@code CHAR_OCTET_LENGTH} should be populated for this type. */
    public static boolean needsCharOctetLength(HyperType t) {
        switch (t.getKind()) {
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    /**
     * {@code true} when the value is representable as a Java {@code long} (any-signed integer
     * kind, including Hyper's {@code OID}). Used by {@code ResultSet.getLong / getInt /
     * getDouble / getBigDecimal} to decide whether the accessor's integer path applies.
     *
     * <p>The switch is exhaustive on purpose: adding a new {@link HyperTypeKind} should prompt
     * the author to decide whether it belongs in this family, rather than silently defaulting
     * to {@code false}.
     */
    public static boolean isIntegerLike(HyperType t) {
        switch (t.getKind()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case OID:
                return true;
            case BOOL:
            case FLOAT4:
            case FLOAT8:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME:
            case TIME_TZ:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case ARRAY:
            case NULL:
            case INTERVAL:
            case JSON:
            case UNKNOWN:
                return false;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    /**
     * {@code true} when the value is representable as a Java {@code String} through the JDBC
     * {@code getString} / {@code getObject} text paths.
     */
    public static boolean isStringLike(HyperType t) {
        switch (t.getKind()) {
            case CHAR:
            case VARCHAR:
                return true;
            case BOOL:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case OID:
            case FLOAT4:
            case FLOAT8:
            case DECIMAL:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME:
            case TIME_TZ:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case ARRAY:
            case NULL:
            case INTERVAL:
            case JSON:
            case UNKNOWN:
                return false;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + t.getKind());
    }

    // ----------------------------------------------------------------------
    // getTypeInfo rows
    // ----------------------------------------------------------------------

    /**
     * Build the standard {@link java.sql.DatabaseMetaData#getTypeInfo()} row for each
     * {@link HyperTypeKind} the driver supports.
     *
     * <p>Emits one row per kind, skipping only {@link HyperTypeKind#NULL} (which is not a
     * user-declarable type). {@link HyperTypeKind#ARRAY} is a parameterised meta-type — the
     * emitted row uses {@code 0}/{@code null} placeholders for element-dependent fields since
     * they only make sense once an element type is chosen. Column order matches
     * {@link com.salesforce.datacloud.jdbc.core.MetadataSchemas#TYPE_INFO}.
     */
    public static List<Object> typeInfoRows() {
        List<Object> rows = new ArrayList<>();
        for (HyperTypeKind kind : HyperTypeKind.values()) {
            // NULL is an internal sentinel; UNKNOWN is a runtime escape hatch for Hyper types the
            // driver does not model — neither is something a client can declare.
            if (kind == HyperTypeKind.NULL || kind == HyperTypeKind.UNKNOWN) {
                continue;
            }
            rows.add(buildTypeInfoRow(kind));
        }
        return rows;
    }

    private static List<Object> buildTypeInfoRow(HyperTypeKind kind) {
        if (kind == HyperTypeKind.ARRAY) {
            return buildArrayTypeInfoRow();
        }
        HyperType prototype = prototypeFor(kind);
        String literalPrefix = isQuoted(kind) ? "'" : null;
        String literalSuffix = isQuoted(kind) ? "'" : null;
        String createParams = createParamsFor(kind);
        short minScale = 0;
        short maxScale = 0;
        if (kind == HyperTypeKind.DECIMAL) {
            maxScale = 38;
        } else if (kind == HyperTypeKind.TIME
                || kind == HyperTypeKind.TIME_TZ
                || kind == HyperTypeKind.TIMESTAMP
                || kind == HyperTypeKind.TIMESTAMP_TZ) {
            maxScale = 6;
        }
        int numPrecRadix = isBinary(kind) ? 2 : 10;

        return Arrays.asList(
                toJdbcTypeName(prototype), // TYPE_NAME
                toJdbcTypeCode(prototype), // DATA_TYPE
                getPrecision(prototype), // PRECISION
                literalPrefix, // LITERAL_PREFIX
                literalSuffix, // LITERAL_SUFFIX
                createParams, // CREATE_PARAMS
                (short) DatabaseMetaData.typeNullable, // NULLABLE
                isCaseSensitive(prototype), // CASE_SENSITIVE
                (short) DatabaseMetaData.typeSearchable, // SEARCHABLE
                !isSigned(prototype), // UNSIGNED_ATTRIBUTE
                false, // FIXED_PREC_SCALE
                false, // AUTO_INCREMENT
                prototype.toHyperTypeName(), // LOCAL_TYPE_NAME
                minScale, // MINIMUM_SCALE
                maxScale, // MAXIMUM_SCALE
                0, // SQL_DATA_TYPE
                0, // SQL_DATETIME_SUB
                numPrecRadix); // NUM_PREC_RADIX
    }

    /**
     * Element-dependent fields ({@code PRECISION}, {@code CASE_SENSITIVE}, {@code
     * UNSIGNED_ATTRIBUTE}, {@code CREATE_PARAMS}) are left as generic defaults; callers that need
     * element-specific metadata should inspect the actual column type via
     * {@link java.sql.DatabaseMetaData#getColumns}.
     */
    private static List<Object> buildArrayTypeInfoRow() {
        return Arrays.asList(
                "ARRAY", // TYPE_NAME
                Types.ARRAY, // DATA_TYPE
                0, // PRECISION (element-dependent)
                null, // LITERAL_PREFIX
                null, // LITERAL_SUFFIX
                null, // CREATE_PARAMS (element-dependent)
                (short) DatabaseMetaData.typeNullable, // NULLABLE
                false, // CASE_SENSITIVE (element-dependent)
                (short) DatabaseMetaData.typeSearchable, // SEARCHABLE
                true, // UNSIGNED_ATTRIBUTE
                false, // FIXED_PREC_SCALE
                false, // AUTO_INCREMENT
                "array", // LOCAL_TYPE_NAME
                (short) 0, // MINIMUM_SCALE
                (short) 0, // MAXIMUM_SCALE
                0, // SQL_DATA_TYPE
                0, // SQL_DATETIME_SUB
                10); // NUM_PREC_RADIX
    }

    private static HyperType prototypeFor(HyperTypeKind kind) {
        switch (kind) {
            case BOOL:
                return HyperType.bool(true);
            case INT8:
                return HyperType.int8(true);
            case INT16:
                return HyperType.int16(true);
            case INT32:
                return HyperType.int32(true);
            case INT64:
                return HyperType.int64(true);
            case OID:
                return HyperType.oid(true);
            case FLOAT4:
                return HyperType.float4(true);
            case FLOAT8:
                return HyperType.float8(true);
            case DECIMAL:
                return HyperType.decimal(38, 0, true);
            case CHAR:
                return HyperType.fixedChar(Integer.MAX_VALUE, true);
            case VARCHAR:
                return HyperType.varcharUnlimited(true);
            case BINARY:
                return HyperType.binary(Integer.MAX_VALUE, true);
            case VARBINARY:
                return HyperType.varbinary(true);
            case DATE:
                return HyperType.date(true);
            case TIME:
                return HyperType.time(true);
            case TIME_TZ:
                return HyperType.timeTz(true);
            case TIMESTAMP:
                return HyperType.timestamp(true);
            case TIMESTAMP_TZ:
                return HyperType.timestampTz(true);
            case INTERVAL:
                return HyperType.interval(true);
            case JSON:
                return HyperType.json(true);
            case ARRAY:
            case NULL:
            case UNKNOWN:
                // ARRAY requires an element kind, NULL is an internal sentinel, and UNKNOWN is a
                // runtime escape hatch — none are first-class types the driver advertises via
                // DatabaseMetaData.getTypeInfo().
                break;
        }
        throw new IllegalArgumentException("No prototype available for HyperTypeKind: " + kind);
    }

    private static String createParamsFor(HyperTypeKind kind) {
        switch (kind) {
            case DECIMAL:
                return "precision,scale";
            case CHAR:
            case VARCHAR:
                return "length";
            default:
                return null;
        }
    }

    private static boolean isQuoted(HyperTypeKind kind) {
        switch (kind) {
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME:
            case TIME_TZ:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case INTERVAL:
            case JSON:
                return true;
            default:
                return false;
        }
    }

    private static boolean isBinary(HyperTypeKind kind) {
        return kind == HyperTypeKind.BINARY || kind == HyperTypeKind.VARBINARY;
    }
}
