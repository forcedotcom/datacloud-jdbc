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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@UtilityClass
public class RealisticArrowGenerator {
    private static final Random random = new Random(10);

    public static QueryResult data() {
        val student = new Student(random.nextInt(), "Somebody", random.nextDouble());
        return getMockedData(ImmutableList.of(student)).findFirst().orElse(null);
    }

    @Value
    @AllArgsConstructor
    public static class Student {
        int id;
        String name;
        double grade;
    }

    public static Stream<QueryResult> getMockedData(List<Student> students) {
        val qr = QueryResult.newBuilder()
                .setBinaryPart(QueryResultPartBinary.newBuilder()
                        .setData(convertStudentsToArrowBinary(students))
                        .build())
                .build();
        return Stream.of(qr);
    }

    private Schema getSchema() {
        val intField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        val stringField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        val doubleField = new Field(
                "grade", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

        val fields = ImmutableList.of(intField, stringField, doubleField);
        return new Schema(fields);
    }

    @SneakyThrows
    private ByteString convertStudentsToArrowBinary(List<Student> students) {
        val rowCount = students.size();

        try (val allocator = new RootAllocator()) {
            val schemaPerson = getSchema();
            try (val vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)) {
                val idVector = (IntVector) vectorSchemaRoot.getVector("id");
                val nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
                val scoreVector = (Float8Vector) vectorSchemaRoot.getVector("grade");

                idVector.allocateNew(rowCount);
                nameVector.allocateNew(rowCount);
                scoreVector.allocateNew(rowCount);

                IntStream.range(0, rowCount).forEach(i -> {
                    idVector.setSafe(i, students.get(i).id);
                    nameVector.setSafe(i, students.get(i).name.getBytes(StandardCharsets.UTF_8));
                    scoreVector.setSafe(i, students.get(i).grade);
                });

                vectorSchemaRoot.setRowCount(rowCount);

                val outputStream = new ByteArrayOutputStream();
                try (val writer = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }

                val s = outputStream.toByteArray();
                return ByteString.copyFrom(s);
            }
        }
    }
}
