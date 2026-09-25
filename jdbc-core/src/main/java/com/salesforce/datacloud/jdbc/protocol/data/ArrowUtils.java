/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

@Slf4j
public final class ArrowUtils {

    private ArrowUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static List<ColumnMetadata> toColumnMetaData(List<Field> fields) {
        return fields.stream()
                .map(field -> new ColumnMetadata(field.getName(), ArrowToHyperTypeMapper.toHyperType(field)))
                .collect(Collectors.toList());
    }

    /**
     * Creates a Schema from a list of ParameterBinding.
     *
     * @param parameterBindings a list of ParameterBinding objects
     * @return a Schema object corresponding to the provided parameters
     */
    public static Schema createSchemaFromParameters(List<ParameterBinding> parameterBindings) {
        if (parameterBindings == null) {
            throw new IllegalArgumentException("ParameterBindings list cannot be null");
        }
        List<Field> fields = IntStream.range(0, parameterBindings.size())
                .mapToObj(i -> createField(parameterBindings.get(i), i + 1))
                .collect(Collectors.toList());

        return new Schema(fields);
    }

    private static Field createField(ParameterBinding parameterBinding, int index) {
        String name = String.valueOf(index);
        if (parameterBinding == null) {
            // Default type for null values, using VARCHAR for simplicity.
            return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
        }
        HyperType type = materializeDecimal(parameterBinding);
        return HyperTypeToArrow.toField(name, type);
    }

    /**
     * If the parameter was bound with an under-specified DECIMAL type (e.g. via
     * {@link java.sql.PreparedStatement#setNull(int, int)} where we do not yet know precision
     * and scale), derive those from the actual {@link BigDecimal} value so Arrow can build a
     * valid {@link ArrowType.Decimal}.
     */
    private static HyperType materializeDecimal(ParameterBinding parameterBinding) {
        HyperType type = parameterBinding.getType();
        if (type.getKind() == HyperTypeKind.DECIMAL
                && type.getPrecision() <= 0
                && parameterBinding.getValue() instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) parameterBinding.getValue();
            return HyperType.decimal(bd.precision(), bd.scale(), type.isNullable());
        }
        return type;
    }

    public static byte[] toArrowByteArray(List<ParameterBinding> parameters, Calendar calendar) throws IOException {
        List<ParameterBinding> normalizedParameters = normalizeDecimalScales(parameters);
        Schema schema = ArrowUtils.createSchemaFromParameters(normalizedParameters);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            VectorPopulator.populateVectors(root, normalizedParameters, calendar);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            return outputStream.toByteArray();
        }
    }

    /**
     * Arrow/Hyper {@code DECIMAL} requires {@code 0 <= scale <= precision}, but {@link BigDecimal}
     * can violate that relationship in both directions:
     *
     * <ul>
     *   <li>negative scale, from a "round" value (e.g. {@code new BigDecimal("12345670").stripTrailingZeros()}
     *       yields {@code unscaledValue=1234567, scale=-1});
     *   <li>{@code scale > precision}, from a small-magnitude value with leading zeros after the
     *       decimal point (e.g. {@code new BigDecimal("0.001")} yields {@code precision=1, scale=3}).
     * </ul>
     *
     * Left alone, either carries straight into the Arrow {@code Decimal} field we advertise for the
     * parameter, which Hyper rejects at query time ("Invalid scale N, scale must be between 0 and the
     * precision").
     *
     * <p>Rescale/re-derive any such value to a form satisfying both invariants up front, so the type
     * we advertise for the parameter and the value we encode for it stay consistent.
     */
    private static List<ParameterBinding> normalizeDecimalScales(List<ParameterBinding> parameters) {
        return parameters.stream().map(ArrowUtils::normalizeDecimalScale).collect(Collectors.toList());
    }

    private static ParameterBinding normalizeDecimalScale(ParameterBinding binding) {
        // binding is null when a lower-indexed parameter hasn't been bound yet --
        // ParameterAccumulator.setParameter() pads the list with null placeholders for any
        // skipped positions (e.g. setBigDecimal(3, ...) before 1/2 are set). createField() already
        // handles this same null for the type side; mirror it here on the value side.
        if (binding == null || !(binding.getValue() instanceof BigDecimal)) {
            return binding;
        }
        BigDecimal value = (BigDecimal) binding.getValue();
        // Widening a negative scale to 0 only ever multiplies the unscaled value by a positive
        // power of ten, so this is always exact -- no RoundingMode is needed.
        BigDecimal normalized = value.scale() < 0 ? value.setScale(0) : value;
        int precision = Math.max(normalized.precision(), normalized.scale());
        if (normalized.equals(value) && precision == value.precision()) {
            return binding;
        }
        return new ParameterBinding(
                HyperType.decimal(
                        precision, normalized.scale(), binding.getType().isNullable()),
                normalized);
    }
}
