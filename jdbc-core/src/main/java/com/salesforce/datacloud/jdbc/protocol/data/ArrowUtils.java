/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
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
    private static final int UNSPECIFIED_DECIMAL_PRECISION = 38;

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
    public static Schema createSchemaFromParameters(@NonNull List<ParameterBinding> parameterBindings) {
        List<Field> fields = IntStream.range(0, parameterBindings.size())
                .mapToObj(i -> createField(String.valueOf(i + 1), parameterBindings.get(i)))
                .collect(Collectors.toList());

        return new Schema(fields);
    }

    private static Field createField(String name, ParameterBinding parameterBinding) {
        if (parameterBinding == null) {
            // Default type for null values, using VARCHAR for simplicity.
            return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
        }
        HyperType type = materializeParameterType(parameterBinding);
        return HyperTypeToArrow.toField(name, type);
    }

    /**
     * Materializes parameter types that Arrow or Hyper cannot accept as bound. Under-specified
     * decimals derive precision from their value or use Hyper's maximum precision for null. Arrow
     * null fields are unsupported by Hyper, so an untyped null is represented as nullable VARCHAR.
     */
    private static HyperType materializeParameterType(ParameterBinding parameterBinding) {
        HyperType type = parameterBinding.getType();
        if (type.getKind() == HyperTypeKind.NULL && parameterBinding.getValue() == null) {
            return HyperType.varcharUnlimited(true);
        }
        if (type.getKind() == HyperTypeKind.DECIMAL && type.getPrecision() <= 0) {
            if (parameterBinding.getValue() instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) parameterBinding.getValue();
                return HyperType.decimal(bd.precision(), bd.scale(), type.isNullable());
            }
            if (parameterBinding.getValue() == null) {
                return HyperType.decimal(UNSPECIFIED_DECIMAL_PRECISION, 0, type.isNullable());
            }
        }
        return type;
    }

    public static byte[] toArrowByteArray(
            @NonNull Iterable<? extends Map.Entry<String, ParameterBinding>> parameterEntries, Calendar calendar)
            throws IOException {
        List<Field> fields = new ArrayList<>();
        List<ParameterBinding> parameters = new ArrayList<>();
        for (Map.Entry<String, ParameterBinding> entry : parameterEntries) {
            fields.add(createField(entry.getKey(), entry.getValue()));
            parameters.add(entry.getValue());
        }

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator)) {
            root.allocateNew();
            VectorPopulator.populateVectors(root, parameters, calendar);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            return outputStream.toByteArray();
        }
    }
}
