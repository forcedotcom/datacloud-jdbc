/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the output of Hyper's {@code pg_catalog.format_type()} function into a {@link HyperType}.
 *
 * <p>This is the canonical pg_catalog-inbound boundary for the driver: the
 * {@code DatabaseMetaData.getColumns()} implementation reads {@code format_type(atttypid,
 * atttypmod)} rows and hands them to {@link #parse(String, boolean)} here.
 *
 * <p>The parser handles both the Postgres display names (e.g. {@code "smallint"}, {@code "integer"},
 * {@code "character varying(255)"}) that Hyper emits and the Postgres internal aliases
 * ({@code int2}, {@code int4}, {@code int8}, {@code bool}, {@code float4}, {@code float8}) as
 * a safety net. Unrecognized names throw {@link IllegalArgumentException} — the driver prefers
 * failing loudly over silently surfacing a type it does not model.
 */
public final class PgCatalogTypeParser {

    private static final Pattern NUMERIC_PATTERN =
            Pattern.compile("numeric(?:\\((\\d+)(?:\\s*,\\s*(\\d+))?\\))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHAR_PATTERN =
            Pattern.compile("(?:character|char|bpchar)(?:\\((\\d+)\\))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern VARCHAR_PATTERN =
            Pattern.compile("(?:character varying|varchar)(?:\\((\\d+)\\))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern ARRAY_PATTERN =
            Pattern.compile("array\\((.+)\\)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private PgCatalogTypeParser() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Parse a {@code pg_catalog.format_type()} output string into a {@link HyperType}.
     *
     * @param formatType the raw string emitted by {@code pg_catalog.format_type()}, e.g.
     *     {@code "bigint"}, {@code "numeric(18,2)"}, {@code "character varying(255)"},
     *     {@code "array(integer)"}
     * @param nullable nullability of the column
     * @throws IllegalArgumentException if {@code formatType} is {@code null}, empty, or a type
     *     the driver does not model
     */
    public static HyperType parse(String formatType, boolean nullable) {
        if (formatType == null || formatType.isEmpty()) {
            throw new IllegalArgumentException("Hyper type name cannot be null or empty");
        }
        String raw = formatType.trim();
        String lower = raw.toLowerCase();

        // Arrays are recursive: array(inner).
        Matcher arrayMatcher = ARRAY_PATTERN.matcher(lower);
        if (arrayMatcher.matches()) {
            HyperType element = parse(arrayMatcher.group(1).trim(), true);
            return HyperType.array(element, nullable);
        }

        // Fast path: simple canonical names.
        switch (lower) {
            case "boolean":
            case "bool":
                return HyperType.bool(nullable);
            case "tinyint":
                return HyperType.int8(nullable);
            case "smallint":
            case "int2":
                return HyperType.int16(nullable);
            case "integer":
            case "int":
            case "int4":
                return HyperType.int32(nullable);
            case "bigint":
            case "int8":
                return HyperType.int64(nullable);
            case "oid":
                return HyperType.oid(nullable);
            case "real":
            case "float4":
                return HyperType.float4(nullable);
            case "double precision":
            case "float8":
            case "float":
                return HyperType.float8(nullable);
            case "date":
                return HyperType.date(nullable);
            case "time":
            case "time without time zone":
                return HyperType.time(nullable);
            case "time with time zone":
            case "timetz":
                return HyperType.timeTz(nullable);
            case "timestamp":
            case "timestamp without time zone":
                return HyperType.timestamp(nullable);
            case "timestamp with time zone":
            case "timestamptz":
                return HyperType.timestampTz(nullable);
            case "text":
                return HyperType.varcharUnlimited(nullable);
            case "bytea":
                return HyperType.varbinary(nullable);
            case "interval":
                return HyperType.interval(nullable);
            case "json":
                return HyperType.json(nullable);
            default:
                break;
        }

        Matcher numericMatcher = NUMERIC_PATTERN.matcher(lower);
        if (numericMatcher.matches()) {
            int precision = numericMatcher.group(1) != null ? Integer.parseInt(numericMatcher.group(1)) : 0;
            int scale = numericMatcher.group(2) != null ? Integer.parseInt(numericMatcher.group(2)) : 0;
            return HyperType.decimal(precision, scale, nullable);
        }

        // "character varying(n)" must be matched BEFORE "character(n)" because it shares the
        // "character" prefix.
        Matcher varcharMatcher = VARCHAR_PATTERN.matcher(lower);
        if (varcharMatcher.matches()) {
            return varcharMatcher.group(1) != null
                    ? HyperType.varchar(Integer.parseInt(varcharMatcher.group(1)), nullable)
                    : HyperType.varcharUnlimited(nullable);
        }

        Matcher charMatcher = CHAR_PATTERN.matcher(lower);
        if (charMatcher.matches()) {
            int length = charMatcher.group(1) != null ? Integer.parseInt(charMatcher.group(1)) : 1;
            return HyperType.fixedChar(length, nullable);
        }

        throw new IllegalArgumentException(
                "Unsupported Hyper type: '" + raw + "' — add explicit HyperTypeKind support for it");
    }
}
