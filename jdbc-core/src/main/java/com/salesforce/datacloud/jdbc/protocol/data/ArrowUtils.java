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
        Schema schema = ArrowUtils.createSchemaFromParameters(parameters);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
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
