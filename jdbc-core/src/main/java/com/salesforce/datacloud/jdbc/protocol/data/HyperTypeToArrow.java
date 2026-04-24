/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Translates the driver's internal {@link HyperType} into Apache Arrow {@link FieldType} /
 * {@link Field} objects.
 *
 * <p>This is the canonical Arrow-outbound boundary, used primarily when building the Arrow schema
 * for a prepared-statement parameter batch. Callers that need an Arrow representation of a driver
 * type should go through here rather than hand-rolling {@link ArrowType} construction.
 */
public final class HyperTypeToArrow {

    private HyperTypeToArrow() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /** Build an Arrow {@link Field} with the given name and the mapped {@link FieldType}. */
    public static Field toField(String name, HyperType type) {
        if (type.getKind() == HyperTypeKind.ARRAY) {
            Field childField = toField("$element", type.getElement());
            return new Field(name, toFieldType(type), Collections.singletonList(childField));
        }
        return new Field(name, toFieldType(type), null);
    }

    /**
     * Map a {@link HyperType} to an Arrow {@link FieldType}.
     *
     * <p>For string kinds Arrow only has the unparameterised {@code Utf8}, so we stamp the
     * Hyper-side length on the field as metadata using the same keys Hyper's own server emits
     * ({@code hyper:type} and {@code hyper:max_string_length}). {@link ArrowToHyperTypeMapper}
     * reads those keys back, letting CHAR(n) / CHAR(1) / VARCHAR(n) round-trip through Arrow
     * without loss.
     */
    public static FieldType toFieldType(HyperType type) {
        ArrowType arrowType = toArrowType(type);
        Map<String, String> metadata = metadataFor(type);
        if (type.isNullable()) {
            return new FieldType(true, arrowType, null, metadata);
        }
        return new FieldType(false, arrowType, null, metadata);
    }

    /** Hyper-compatible field metadata for types whose length is not carried in the ArrowType. */
    private static Map<String, String> metadataFor(HyperType type) {
        switch (type.getKind()) {
            case CHAR:
                if (type.getPrecision() == 1) {
                    return Collections.singletonMap("hyper:type", "Char1");
                }
                // A general CHAR(n). Stamp both the discriminator and the length.
                Map<String, String> charMeta = new HashMap<>();
                charMeta.put("hyper:type", "Char");
                charMeta.put("hyper:max_string_length", Integer.toString(type.getPrecision()));
                return charMeta;
            case VARCHAR:
                if (type.getPrecision() == HyperType.UNLIMITED_LENGTH) {
                    // No length to stamp — unbounded VARCHAR is indistinguishable from "text" in
                    // Arrow and that's fine; both decode to varcharUnlimited.
                    return null;
                }
                return Collections.singletonMap("hyper:max_string_length", Integer.toString(type.getPrecision()));
            default:
                return null;
        }
    }

    /** Map a {@link HyperType} to an Arrow {@link ArrowType} (no nullability). */
    public static ArrowType toArrowType(HyperType type) {
        switch (type.getKind()) {
            case BOOL:
                return new ArrowType.Bool();
            case INT8:
                return new ArrowType.Int(8, true);
            case INT16:
                return new ArrowType.Int(16, true);
            case INT32:
                return new ArrowType.Int(32, true);
            case INT64:
            case OID:
                return new ArrowType.Int(64, true);
            case FLOAT4:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case FLOAT8:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL:
                // Arrow's Decimal requires a positive precision. For under-specified DECIMAL
                // parameters (bound without precision/scale), callers should derive precision
                // and scale from the actual BigDecimal value before calling here.
                if (type.getPrecision() <= 0) {
                    throw new IllegalArgumentException(
                            "DECIMAL HyperType must carry precision before Arrow conversion");
                }
                return new ArrowType.Decimal(type.getPrecision(), type.getScale(), 128);
            case CHAR:
            case VARCHAR:
                return new ArrowType.Utf8();
            case BINARY:
                return new ArrowType.FixedSizeBinary(type.getPrecision());
            case VARBINARY:
                return new ArrowType.Binary();
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
            case TIME_TZ:
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case TIMESTAMP_TZ:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            case ARRAY:
                return new ArrowType.List();
            case NULL:
                return new ArrowType.Null();
            case INTERVAL:
            case JSON:
                throw new IllegalArgumentException("Binding " + type.getKind() + " parameters is not supported");
            case UNKNOWN:
                throw new IllegalArgumentException(
                        "Cannot bind UNKNOWN type '" + type.getUnknownTypeName() + "'; the driver does not model it");
        }
        throw new IllegalStateException("Unhandled HyperTypeKind: " + type.getKind());
    }
}
