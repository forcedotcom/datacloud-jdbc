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
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.util.Cursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QueryJDBCCursorTest {

    static QueryJDBCCursor cursor;
    BufferAllocator allocator;

    @AfterEach
    public void tearDown() {
        allocator.close();
        cursor.close();
    }

    @Test
    public void testVarCharVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("VarChar", new ArrowType.Utf8());
        ((VarCharVector) root.getVector("VarChar")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testDecimalVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("Decimal", new ArrowType.Decimal(38, 18, 128));
        ((DecimalVector) root.getVector("Decimal")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testBooleanVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("Boolean", new ArrowType.Bool());
        ((BitVector) root.getVector("Boolean")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testDateMilliVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("DateMilli", new ArrowType.Date(DateUnit.MILLISECOND));
        ((DateMilliVector) root.getVector("DateMilli")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testDateDayVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("DateDay", new ArrowType.Date(DateUnit.DAY));
        ((DateDayVector) root.getVector("DateDay")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testTimeNanoVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("TimeNano", new ArrowType.Time(TimeUnit.NANOSECOND, 64));
        ((TimeNanoVector) root.getVector("TimeNano")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testTimeMicroVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("TimeMicro", new ArrowType.Time(TimeUnit.MICROSECOND, 64));
        ((TimeMicroVector) root.getVector("TimeMicro")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testTimeMilliVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("TimeMilli", new ArrowType.Time(TimeUnit.MILLISECOND, 32));
        ((TimeMilliVector) root.getVector("TimeMilli")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testTimeSecVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root = getVectorSchemaRoot("TimeSec", new ArrowType.Time(TimeUnit.SECOND, 32));
        ((TimeSecVector) root.getVector("TimeSec")).setNull(0);
        testCursorWasNull(root);
    }

    @Test
    public void testTimeStampVectorNullTrue() throws SQLException {
        final VectorSchemaRoot root =
                getVectorSchemaRoot("TimeStamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
        ((TimeStampMilliVector) root.getVector("TimeStamp")).setNull(0);
        testCursorWasNull(root);
    }

    private VectorSchemaRoot getVectorSchemaRoot(String name, ArrowType arrowType) {
        final Schema schema = new Schema(
                ImmutableList.of(new Field(name, new FieldType(true, arrowType, null), Collections.emptyList())));
        allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        return root;
    }

    private void testCursorWasNull(VectorSchemaRoot root) throws SQLException {
        root.setRowCount(1);
        cursor = new QueryJDBCCursor(root);
        cursor.next();
        List<Cursor.Accessor> accessorList = cursor.createAccessors(null, null, null);
        accessorList.get(0).getObject();
        assertThat(cursor.wasNull()).as("cursor.wasNull()").isTrue();
        root.close();
    }
}
