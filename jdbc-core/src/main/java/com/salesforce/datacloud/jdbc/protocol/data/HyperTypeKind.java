/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

/**
 * Discriminator for {@link HyperType}, covering every column type the JDBC driver recognizes.
 *
 * <p>This is the internal type model used throughout the driver. It is translated to
 * {@link java.sql.JDBCType} / {@link java.sql.Types} only at the JDBC API boundary, to
 * {@link org.apache.arrow.vector.types.pojo.ArrowType} at the Arrow boundary, and to Spark types at
 * the Spark boundary. Hyper-specific concepts that have no direct JDBC counterpart (e.g. {@code
 * OID}) are first-class here and get translated into an approximation at the JDBC boundary.
 */
public enum HyperTypeKind {
    /** {@code BOOL}. */
    BOOL,
    /** Signed 8-bit integer ({@code TINYINT}). */
    INT8,
    /** Signed 16-bit integer ({@code SMALLINT}). */
    INT16,
    /** Signed 32-bit integer ({@code INTEGER}). */
    INT32,
    /** Signed 64-bit integer ({@code BIGINT}). */
    INT64,
    /** Hyper's unsigned 32-bit object identifier type. Surfaced as {@code BIGINT} over JDBC. */
    OID,
    /** IEEE-754 single-precision float ({@code REAL}). */
    FLOAT4,
    /** IEEE-754 double-precision float ({@code DOUBLE}). */
    FLOAT8,
    /** Fixed-precision decimal (both Hyper {@code NUMERIC} and {@code DECIMAL} collapse here). */
    DECIMAL,
    /** Fixed-length character string ({@code CHAR(n)}). */
    CHAR,
    /** Variable-length character string ({@code VARCHAR(n)} or unbounded). */
    VARCHAR,
    /** Fixed-length binary data. */
    BINARY,
    /** Variable-length binary data. */
    VARBINARY,
    /** Calendar date (no time-of-day component). */
    DATE,
    /** Time-of-day without timezone. */
    TIME,
    /** Time-of-day with timezone. */
    TIME_TZ,
    /** Timestamp without timezone. */
    TIMESTAMP,
    /** Timestamp with timezone. */
    TIMESTAMP_TZ,
    /** Array of another {@link HyperType}. */
    ARRAY,
    /** The untyped {@code NULL} column; mostly seen in {@code SELECT NULL}. */
    NULL,
    /** Hyper's {@code interval} type (period of time). */
    INTERVAL,
    /** Hyper's {@code json} type. */
    JSON,
    /**
     * Escape hatch for a Hyper type the driver does not model (e.g. system-catalog types like
     * {@code aclitem} or {@code array(aclitem)} that surface when callers scan
     * {@code getColumns(null, null, null, null)} across {@code pg_catalog}).
     *
     * <p>Columns of this kind are surfaced over JDBC as {@link java.sql.Types#OTHER} with the raw
     * Hyper type string preserved via {@link HyperType#getUnknownTypeName()} for debugging. They
     * cannot be read, written, or transcoded — any Arrow/Spark/prepared-statement path that tries
     * to operate on an {@code UNKNOWN} column will fail loudly.
     */
    UNKNOWN
}
