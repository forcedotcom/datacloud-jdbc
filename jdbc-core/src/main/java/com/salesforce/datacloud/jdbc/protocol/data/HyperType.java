/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import lombok.Value;

/**
 * Canonical internal representation of a Hyper column type.
 *
 * <p>Instances are immutable value objects. All Arrow, pg_catalog, prepared-statement,
 * and Spark conversions in the driver route through {@code HyperType}; translation to
 * {@link java.sql.JDBCType} / {@link java.sql.Types} is done at the JDBC API boundary by
 * {@link HyperTypes}.
 *
 * <p>The class exposes a fixed set of factory methods; {@link #accept} implements the
 * visitor pattern over {@link HyperTypeKind} for exhaustive processing.
 */
@Value
public class HyperType {

    /** Sentinel for {@link HyperTypeKind#VARCHAR} / {@link HyperTypeKind#VARBINARY} without an upper length bound. */
    public static final int UNLIMITED_LENGTH = Integer.MAX_VALUE;

    /** Discriminator for this type. Never {@code null}. */
    HyperTypeKind kind;

    /**
     * Length for CHAR / VARCHAR / BINARY / VARBINARY, or precision for DECIMAL. {@code 0} for kinds where
     * the concept does not apply. For unbounded VARCHAR / VARBINARY, this is {@link #UNLIMITED_LENGTH}.
     */
    int precision;

    /** Scale for DECIMAL; {@code 0} for other kinds. */
    int scale;

    /** Element type for ARRAY; {@code null} otherwise. */
    HyperType element;

    /** Whether this column is nullable. */
    boolean nullable;

    /**
     * Raw Hyper type name for {@link HyperTypeKind#UNKNOWN}; {@code null} for every other kind.
     * Preserved from {@code pg_catalog.format_type()} output so callers can still see what the
     * server emitted even when the driver does not model the type.
     */
    String unknownTypeName;

    // ----------------------------------------------------------------------
    // Factory methods
    // ----------------------------------------------------------------------

    public static HyperType bool(boolean nullable) {
        return new HyperType(HyperTypeKind.BOOL, 0, 0, null, nullable, null);
    }

    public static HyperType int8(boolean nullable) {
        return new HyperType(HyperTypeKind.INT8, 0, 0, null, nullable, null);
    }

    public static HyperType int16(boolean nullable) {
        return new HyperType(HyperTypeKind.INT16, 0, 0, null, nullable, null);
    }

    public static HyperType int32(boolean nullable) {
        return new HyperType(HyperTypeKind.INT32, 0, 0, null, nullable, null);
    }

    public static HyperType int64(boolean nullable) {
        return new HyperType(HyperTypeKind.INT64, 0, 0, null, nullable, null);
    }

    public static HyperType oid(boolean nullable) {
        return new HyperType(HyperTypeKind.OID, 0, 0, null, nullable, null);
    }

    public static HyperType float4(boolean nullable) {
        return new HyperType(HyperTypeKind.FLOAT4, 0, 0, null, nullable, null);
    }

    public static HyperType float8(boolean nullable) {
        return new HyperType(HyperTypeKind.FLOAT8, 0, 0, null, nullable, null);
    }

    public static HyperType decimal(int precision, int scale, boolean nullable) {
        return new HyperType(HyperTypeKind.DECIMAL, precision, scale, null, nullable, null);
    }

    /** Fixed-length CHAR with a positive length. */
    public static HyperType fixedChar(int length, boolean nullable) {
        return new HyperType(HyperTypeKind.CHAR, length, 0, null, nullable, null);
    }

    /** Bounded {@code VARCHAR(length)}. */
    public static HyperType varchar(int length, boolean nullable) {
        return new HyperType(HyperTypeKind.VARCHAR, length, 0, null, nullable, null);
    }

    /** Unbounded {@code VARCHAR} / {@code TEXT}. */
    public static HyperType varcharUnlimited(boolean nullable) {
        return new HyperType(HyperTypeKind.VARCHAR, UNLIMITED_LENGTH, 0, null, nullable, null);
    }

    public static HyperType binary(int length, boolean nullable) {
        return new HyperType(HyperTypeKind.BINARY, length, 0, null, nullable, null);
    }

    public static HyperType varbinary(boolean nullable) {
        return new HyperType(HyperTypeKind.VARBINARY, UNLIMITED_LENGTH, 0, null, nullable, null);
    }

    public static HyperType date(boolean nullable) {
        return new HyperType(HyperTypeKind.DATE, 0, 0, null, nullable, null);
    }

    public static HyperType time(boolean nullable) {
        return new HyperType(HyperTypeKind.TIME, 0, 0, null, nullable, null);
    }

    public static HyperType timeTz(boolean nullable) {
        return new HyperType(HyperTypeKind.TIME_TZ, 0, 0, null, nullable, null);
    }

    public static HyperType timestamp(boolean nullable) {
        return new HyperType(HyperTypeKind.TIMESTAMP, 0, 0, null, nullable, null);
    }

    public static HyperType timestampTz(boolean nullable) {
        return new HyperType(HyperTypeKind.TIMESTAMP_TZ, 0, 0, null, nullable, null);
    }

    public static HyperType array(HyperType element, boolean nullable) {
        if (element == null) {
            throw new IllegalArgumentException("ARRAY element type cannot be null");
        }
        return new HyperType(HyperTypeKind.ARRAY, 0, 0, element, nullable, null);
    }

    public static HyperType nullType() {
        return new HyperType(HyperTypeKind.NULL, 0, 0, null, true, null);
    }

    public static HyperType interval(boolean nullable) {
        return new HyperType(HyperTypeKind.INTERVAL, 0, 0, null, nullable, null);
    }

    public static HyperType json(boolean nullable) {
        return new HyperType(HyperTypeKind.JSON, 0, 0, null, nullable, null);
    }

    /**
     * Escape hatch for a Hyper type the driver does not model. Preserves the raw name from
     * {@code pg_catalog.format_type()} for debugging. See {@link HyperTypeKind#UNKNOWN}.
     */
    public static HyperType unknown(String rawTypeName, boolean nullable) {
        if (rawTypeName == null || rawTypeName.isEmpty()) {
            throw new IllegalArgumentException("unknown type name cannot be null or empty");
        }
        return new HyperType(HyperTypeKind.UNKNOWN, 0, 0, null, nullable, rawTypeName);
    }

    // ----------------------------------------------------------------------
    // Visitor dispatch
    // ----------------------------------------------------------------------

    /** Dispatch on {@link #kind} to the matching {@link HyperTypeVisitor} method. */
    public <T> T accept(HyperTypeVisitor<T> visitor) {
        switch (kind) {
            case BOOL:
                return visitor.visitBool(this);
            case INT8:
                return visitor.visitInt8(this);
            case INT16:
                return visitor.visitInt16(this);
            case INT32:
                return visitor.visitInt32(this);
            case INT64:
                return visitor.visitInt64(this);
            case OID:
                return visitor.visitOid(this);
            case FLOAT4:
                return visitor.visitFloat4(this);
            case FLOAT8:
                return visitor.visitFloat8(this);
            case DECIMAL:
                return visitor.visitDecimal(this);
            case CHAR:
                return visitor.visitChar(this);
            case VARCHAR:
                return visitor.visitVarchar(this);
            case BINARY:
                return visitor.visitBinary(this);
            case VARBINARY:
                return visitor.visitVarbinary(this);
            case DATE:
                return visitor.visitDate(this);
            case TIME:
                return visitor.visitTime(this);
            case TIME_TZ:
                return visitor.visitTimeTz(this);
            case TIMESTAMP:
                return visitor.visitTimestamp(this);
            case TIMESTAMP_TZ:
                return visitor.visitTimestampTz(this);
            case ARRAY:
                return visitor.visitArray(this);
            case NULL:
                return visitor.visitNull(this);
            case INTERVAL:
                return visitor.visitInterval(this);
            case JSON:
                return visitor.visitJson(this);
            case UNKNOWN:
                return visitor.visitUnknown(this);
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + kind);
    }

    /**
     * The Hyper-native type name (e.g. {@code "bigint"}, {@code "character varying(255)"},
     * {@code "oid"}). This is the string Hyper's own {@code pg_catalog.format_type()} would emit,
     * not a JDBC-standard name.
     */
    public String toHyperTypeName() {
        switch (kind) {
            case BOOL:
                return "boolean";
            case INT8:
                return "tinyint";
            case INT16:
                return "smallint";
            case INT32:
                return "integer";
            case INT64:
                return "bigint";
            case OID:
                return "oid";
            case FLOAT4:
                return "real";
            case FLOAT8:
                return "double precision";
            case DECIMAL:
                if (precision > 0) {
                    return "numeric(" + precision + "," + scale + ")";
                }
                return "numeric";
            case CHAR:
                return "character(" + precision + ")";
            case VARCHAR:
                if (precision == UNLIMITED_LENGTH) {
                    return "character varying";
                }
                return "character varying(" + precision + ")";
            case BINARY:
                return "binary(" + precision + ")";
            case VARBINARY:
                return "bytea";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIME_TZ:
                return "time with time zone";
            case TIMESTAMP:
                return "timestamp";
            case TIMESTAMP_TZ:
                return "timestamp with time zone";
            case ARRAY:
                return "array(" + element.toHyperTypeName() + ")";
            case NULL:
                return "null";
            case INTERVAL:
                return "interval";
            case JSON:
                return "json";
            case UNKNOWN:
                return unknownTypeName;
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + kind);
    }
}
