/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeKind;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeToArrow;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.Value;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Materialises a list of row-oriented metadata values into a populated Arrow
 * {@link VectorSchemaRoot}.
 *
 * <p>Used by the JDBC metadata path ({@code DatabaseMetaData.getTables}, {@code getColumns},
 * {@code getTypeInfo}, ...) so that both streaming query results and materialised metadata
 * results run through the same Arrow-backed result set.
 *
 * <p>The builder owns a fresh {@link RootAllocator}; the returned {@link Result} transfers
 * ownership of the allocator and schema root to the caller, which must close both — closing in
 * the {@link Result#close()} order tears down the root before the allocator to satisfy Arrow's
 * accounting invariants.
 */
public final class MetadataArrowBuilder {

    private MetadataArrowBuilder() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /** Populated result. Owns the allocator and the vector schema root. */
    @Value
    public static class Result implements AutoCloseable {
        VectorSchemaRoot root;
        RootAllocator allocator;

        @Override
        public void close() {
            // Close the root first: it releases ArrowBuf accounting back to the allocator, so the
            // allocator's closing budget check passes. Reversing the order trips a leak detector.
            try {
                root.close();
            } finally {
                allocator.close();
            }
        }
    }

    /**
     * Build an Arrow {@link VectorSchemaRoot} whose schema matches {@code columns} and whose rows
     * come from {@code rows} (each inner list is a row in column order).
     *
     * <p>Values are coerced to the target vector type on a best-effort basis (e.g. a
     * {@link Boolean} in a VARCHAR column becomes {@code "true"}/{@code "false"}). This mirrors
     * the loose coercion the removed {@code SimpleResultSet.getString} used to do on row-based
     * metadata data.
     */
    public static Result build(List<ColumnMetadata> columns, List<List<Object>> rows) {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = null;
        try {
            Schema schema = buildSchema(columns);
            root = VectorSchemaRoot.create(schema, allocator);
            root.allocateNew();
            populate(root, columns, rows);
            root.setRowCount(rows == null ? 0 : rows.size());
            Result result = new Result(root, allocator);
            root = null; // ownership transferred
            return result;
        } finally {
            if (root != null) {
                try {
                    root.close();
                } finally {
                    allocator.close();
                }
            }
        }
    }

    private static Schema buildSchema(List<ColumnMetadata> columns) {
        java.util.List<Field> fields = new java.util.ArrayList<>(columns.size());
        for (ColumnMetadata column : columns) {
            fields.add(HyperTypeToArrow.toField(column.getName(), column.getType()));
        }
        return new Schema(fields);
    }

    private static void populate(VectorSchemaRoot root, List<ColumnMetadata> columns, List<List<Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        for (int col = 0; col < columns.size(); col++) {
            ValueVector vector = root.getVector(columns.get(col).getName());
            HyperType type = columns.get(col).getType();
            for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                List<Object> row = rows.get(rowIdx);
                Object value = row == null || col >= row.size() ? null : row.get(col);
                setValue(vector, rowIdx, type, value);
            }
            vector.setValueCount(rows.size());
        }
    }

    private static void setValue(ValueVector vector, int index, HyperType type, Object value) {
        if (value == null) {
            setNull(vector, index, type);
            return;
        }
        HyperTypeKind kind = type.getKind();
        switch (kind) {
            case VARCHAR:
            case CHAR:
                byte[] bytes = asString(value).getBytes(StandardCharsets.UTF_8);
                ((VarCharVector) vector).setSafe(index, bytes);
                return;
            case INT8:
                ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
                return;
            case INT16:
                ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
                return;
            case INT32:
                ((IntVector) vector).setSafe(index, ((Number) value).intValue());
                return;
            case INT64:
                ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
                return;
            case BOOL:
                ((BitVector) vector).setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
                return;
            default:
                throw new IllegalArgumentException("MetadataArrowBuilder does not support HyperTypeKind " + kind
                        + "; metadata schemas are expected to use VARCHAR/CHAR and fixed-width integers only");
        }
    }

    private static void setNull(ValueVector vector, int index, HyperType type) {
        switch (type.getKind()) {
            case VARCHAR:
            case CHAR:
                ((VarCharVector) vector).setNull(index);
                return;
            case INT8:
                ((TinyIntVector) vector).setNull(index);
                return;
            case INT16:
                ((SmallIntVector) vector).setNull(index);
                return;
            case INT32:
                ((IntVector) vector).setNull(index);
                return;
            case INT64:
                ((BigIntVector) vector).setNull(index);
                return;
            case BOOL:
                ((BitVector) vector).setNull(index);
                return;
            default:
                throw new IllegalArgumentException("Unsupported metadata type kind for null: " + type.getKind());
        }
    }

    private static String asString(Object value) {
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
