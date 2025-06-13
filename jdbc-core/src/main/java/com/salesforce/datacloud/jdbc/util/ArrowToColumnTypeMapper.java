/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.util;

import java.sql.JDBCType;
import java.util.Objects;
import lombok.val;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public final class ArrowToColumnTypeMapper {

    /** Infer the JDBC ColumnType from the Arrow result metadata. */
    public static ColumnType toColumnType(Field field) {
        Objects.requireNonNull(field, "Field cannot be null");
        return field.getType().accept(new ArrowTypeVisitor(field));
    }

    /**
     * Visitor for ArrowType to convert to ColumnType. The visitor provides direct access to the specifc ArrowType objects
     */
    private static class ArrowTypeVisitor implements ArrowType.ArrowTypeVisitor<ColumnType> {
        private final Field field;

        public ArrowTypeVisitor(Field field) {
            this.field = field;
        }

        private IllegalArgumentException unsupportedTypeException() {
            return new IllegalArgumentException(
                    "Unsupported Arrow type: " + field.getType() + " for field " + field.getName());
        }

        @Override
        public ColumnType visit(ArrowType.Null aNull) {
            return new ColumnType(JDBCType.NULL);
        }

        @Override
        public ColumnType visit(ArrowType.Struct struct) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.List list) {
            val elementField = field.getChildren().get(0);
            return new ColumnType(JDBCType.ARRAY, toColumnType(elementField));
        }

        @Override
        public ColumnType visit(ArrowType.LargeList largeList) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.FixedSizeList fixedSizeList) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Union union) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Map map) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Int anInt) {
            switch (anInt.getBitWidth()) {
                case 8:
                    return new ColumnType(JDBCType.TINYINT);
                case 16:
                    return new ColumnType(JDBCType.SMALLINT);
                case 32:
                    return new ColumnType(JDBCType.INTEGER);
                case 64:
                    return new ColumnType(JDBCType.BIGINT);
                default:
                    throw unsupportedTypeException();
            }
        }

        @Override
        public ColumnType visit(ArrowType.FloatingPoint floatingPoint) {
            switch (floatingPoint.getPrecision()) {
                case SINGLE:
                    return new ColumnType(JDBCType.REAL);
                case DOUBLE:
                    return new ColumnType(JDBCType.DOUBLE);
                case HALF:
                    break;
            }
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Utf8 utf8) {
            val metadata = field.getMetadata();
            if (metadata != null) {
                if ("Char".equals(metadata.get("hyper:type"))) {
                    int precision = Integer.parseInt(metadata.get("hyper:max_string_length"));
                    return new ColumnType(JDBCType.CHAR, precision, 0);
                } else if ("Char1".equals(metadata.get("hyper:type"))) {
                    return new ColumnType(JDBCType.CHAR, 1, 0);
                } else if (metadata.containsKey("hyper:max_string_length")) {
                    int precision = Integer.parseInt(metadata.get("hyper:max_string_length"));
                    return new ColumnType(JDBCType.VARCHAR, precision, 0);
                }
            }
            // When the varchar has no explicit bounds it is "unlimited", thus MAX_VLAUE
            return new ColumnType(JDBCType.VARCHAR, Integer.MAX_VALUE, 0);
        }

        @Override
        public ColumnType visit(ArrowType.Utf8View utf8View) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.LargeUtf8 largeUtf8) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Binary binary) {
            return new ColumnType(JDBCType.VARBINARY);
        }

        @Override
        public ColumnType visit(ArrowType.BinaryView binaryView) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.LargeBinary largeBinary) {
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
            return new ColumnType(JDBCType.BINARY);
        }

        @Override
        public ColumnType visit(ArrowType.Bool bool) {
            return new ColumnType(JDBCType.BOOLEAN);
        }

        @Override
        public ColumnType visit(ArrowType.Decimal decimal) {
            return new ColumnType(JDBCType.DECIMAL, decimal.getPrecision(), decimal.getScale());
        }

        @Override
        public ColumnType visit(ArrowType.Date date) {
            return new ColumnType(JDBCType.DATE);
        }

        @Override
        public ColumnType visit(ArrowType.Time time) {
            return new ColumnType(JDBCType.TIME);
        }

        @Override
        public ColumnType visit(ArrowType.Timestamp timestamp) {
            if (timestamp.getTimezone() != null) {
                return new ColumnType(JDBCType.TIMESTAMP_WITH_TIMEZONE);
            } else {
                return new ColumnType(JDBCType.TIMESTAMP);
            }
        }

        @Override
        public ColumnType visit(ArrowType.Interval interval) {
            // TODO: Interval support will need to come in a separate PR
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.Duration duration) {
            // TODO: Duration support will need to come in a separate PR
            throw unsupportedTypeException();
        }

        @Override
        public ColumnType visit(ArrowType.ListView listView) {
            throw unsupportedTypeException();
        }
    }
}
