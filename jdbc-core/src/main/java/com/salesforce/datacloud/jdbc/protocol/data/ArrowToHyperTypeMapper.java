/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.util.Objects;
import lombok.val;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Translates an Apache Arrow {@link Field} (with Hyper-specific metadata) into a {@link HyperType}.
 *
 * <p>This is the canonical Arrow-inbound boundary for the driver: every place that needs to know
 * the SQL type of an Arrow stream column should call {@link #toHyperType(Field)}. Hyper
 * distinguishes {@code CHAR(n)}, {@code CHAR(1)} and {@code VARCHAR(n)} via the
 * {@code hyper:type} / {@code hyper:max_string_length} Arrow field metadata keys; those are
 * interpreted here.
 */
public final class ArrowToHyperTypeMapper {

    private ArrowToHyperTypeMapper() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /** Translate an Arrow {@link Field} to the driver's {@link HyperType}. */
    public static HyperType toHyperType(Field field) {
        Objects.requireNonNull(field, "Field cannot be null");
        return field.getType().accept(new ArrowTypeVisitor(field));
    }

    /** Arrow visitor that produces a {@link HyperType} for each supported Arrow type. */
    private static class ArrowTypeVisitor implements ArrowType.ArrowTypeVisitor<HyperType> {
        private final Field field;
        private final boolean nullable;

        ArrowTypeVisitor(Field field) {
            this.field = field;
            this.nullable = field.isNullable();
        }

        private IllegalArgumentException unsupportedTypeException() {
            return new IllegalArgumentException(
                    "Unsupported Arrow type: " + field.getType() + " for field " + field.getName());
        }

        @Override
        public HyperType visit(ArrowType.Null aNull) {
            return HyperType.nullType();
        }

        @Override
        public HyperType visit(ArrowType.Struct struct) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.List list) {
            val elementField = field.getChildren().get(0);
            return HyperType.array(toHyperType(elementField), nullable);
        }

        @Override
        public HyperType visit(ArrowType.LargeList largeList) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.FixedSizeList fixedSizeList) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Union union) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Map map) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Int anInt) {
            switch (anInt.getBitWidth()) {
                case 8:
                    return HyperType.int8(nullable);
                case 16:
                    return HyperType.int16(nullable);
                case 32:
                    return HyperType.int32(nullable);
                case 64:
                    return HyperType.int64(nullable);
                default:
                    throw unsupportedTypeException();
            }
        }

        @Override
        public HyperType visit(ArrowType.FloatingPoint floatingPoint) {
            switch (floatingPoint.getPrecision()) {
                case SINGLE:
                    return HyperType.float4(nullable);
                case DOUBLE:
                    return HyperType.float8(nullable);
                case HALF:
                    break;
            }
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Utf8 utf8) {
            val metadata = field.getMetadata();
            if (metadata != null) {
                // Hyper emits distinct CHAR vs CHAR(1) vs VARCHAR forms via field metadata.
                if ("Char".equals(metadata.get("hyper:type"))) {
                    int precision = Integer.parseInt(metadata.get("hyper:max_string_length"));
                    return HyperType.fixedChar(precision, nullable);
                } else if ("Char1".equals(metadata.get("hyper:type"))) {
                    return HyperType.fixedChar(1, nullable);
                } else if (metadata.containsKey("hyper:max_string_length")) {
                    int precision = Integer.parseInt(metadata.get("hyper:max_string_length"));
                    return HyperType.varchar(precision, nullable);
                }
            }
            // Without bounds metadata, treat Utf8 as unbounded varchar.
            return HyperType.varcharUnlimited(nullable);
        }

        @Override
        public HyperType visit(ArrowType.Utf8View utf8View) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.LargeUtf8 largeUtf8) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Binary binary) {
            return HyperType.varbinary(nullable);
        }

        @Override
        public HyperType visit(ArrowType.BinaryView binaryView) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.LargeBinary largeBinary) {
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
            return HyperType.binary(fixedSizeBinary.getByteWidth(), nullable);
        }

        @Override
        public HyperType visit(ArrowType.Bool bool) {
            return HyperType.bool(nullable);
        }

        @Override
        public HyperType visit(ArrowType.Decimal decimal) {
            return HyperType.decimal(decimal.getPrecision(), decimal.getScale(), nullable);
        }

        @Override
        public HyperType visit(ArrowType.Date date) {
            return HyperType.date(nullable);
        }

        @Override
        public HyperType visit(ArrowType.Time time) {
            return HyperType.time(nullable);
        }

        @Override
        public HyperType visit(ArrowType.Timestamp timestamp) {
            if (timestamp.getTimezone() != null) {
                return HyperType.timestampTz(nullable);
            } else {
                return HyperType.timestamp(nullable);
            }
        }

        @Override
        public HyperType visit(ArrowType.Interval interval) {
            // TODO: Interval support will need to come in a separate PR
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.Duration duration) {
            // TODO: Duration support will need to come in a separate PR
            throw unsupportedTypeException();
        }

        @Override
        public HyperType visit(ArrowType.ListView listView) {
            throw unsupportedTypeException();
        }
    }
}
