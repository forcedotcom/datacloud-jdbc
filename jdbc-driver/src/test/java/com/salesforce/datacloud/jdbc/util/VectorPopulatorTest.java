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

// import static org.mockito.Mockito.mock;
// import static org.mockito.Mockito.spy;
// import static org.mockito.Mockito.times;
// import static org.mockito.Mockito.verify;
// import static org.mockito.Mockito.when;
// import static org.mockito.Mockito.withSettings;
//
// import com.google.common.collect.ImmutableList;
// import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
// import java.math.BigDecimal;
// import java.nio.charset.StandardCharsets;
// import java.sql.Date;
// import java.sql.Time;
// import java.sql.Timestamp;
// import java.sql.Types;
// import java.time.LocalDateTime;
// import java.util.Arrays;
// import java.util.Calendar;
// import java.util.List;
// import java.util.TimeZone;
//
// import lombok.val;
// import org.apache.arrow.memory.BufferAllocator;
// import org.apache.arrow.memory.RootAllocator;
// import org.apache.arrow.vector.BigIntVector;
// import org.apache.arrow.vector.BitVector;
// import org.apache.arrow.vector.DateDayVector;
// import org.apache.arrow.vector.DecimalVector;
// import org.apache.arrow.vector.Float4Vector;
// import org.apache.arrow.vector.Float8Vector;
// import org.apache.arrow.vector.IntVector;
// import org.apache.arrow.vector.SmallIntVector;
// import org.apache.arrow.vector.TimeMicroVector;
// import org.apache.arrow.vector.TimeStampMicroTZVector;
// import org.apache.arrow.vector.VarCharVector;
// import org.apache.arrow.vector.VectorSchemaRoot;
// import org.apache.arrow.vector.types.DateUnit;
// import org.apache.arrow.vector.types.FloatingPointPrecision;
// import org.apache.arrow.vector.types.TimeUnit;
// import org.apache.arrow.vector.types.pojo.ArrowType;
// import org.apache.arrow.vector.types.pojo.Field;
// import org.apache.arrow.vector.types.pojo.FieldType;
// import org.apache.arrow.vector.types.pojo.Schema;
// import org.junit.After;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.Mockito;
// import org.mockito.junit.jupiter.MockitoExtension;
//
// @ExtendWith(MockitoExtension.class)
// public class VectorPopulatorTest {
//    private BufferAllocator allocator;
//    private VectorSchemaRoot mockRoot;
//    private VarCharVector varcharVector;
//    private Float4Vector float4Vector;
//    private Float8Vector float8Vector;
//    private IntVector intVector;
//    private SmallIntVector smallIntVector;
//    private BigIntVector bigIntVector;
//    private BitVector bitVector;
//    private DecimalVector decimalVector;
//    private DateDayVector dateDayVector;
//    private TimeMicroVector timeMicroVector;
//    private TimeStampMicroTZVector timestampMicroTZVector;
//    private List<ParameterBinding> parameterBindings;
//    private Calendar calendar;
//
//    @AfterEach
//    public void afterEach() {
//        allocator.close();
//    }
//
//    @BeforeEach
//    public void beforeEach() {
//        allocator = new RootAllocator();
//        mockRoot = mock(VectorSchemaRoot.class);
//        varcharVector = mock(VarCharVector.class);
//        float4Vector = spy(new Float4Vector("Float4Vector", allocator));
//        float8Vector = spy(new Float8Vector("Float8Vector", allocator));
//        intVector = spy(new IntVector("IntVector", allocator));
//        smallIntVector = mock(SmallIntVector.class);
//        bigIntVector = mock(BigIntVector.class);
//        bitVector = mock(BitVector.class);
//        decimalVector = mock(DecimalVector.class);
//        dateDayVector = mock(DateDayVector.class);
//        timeMicroVector = mock(TimeMicroVector.class);
//        timestampMicroTZVector = mock(TimeStampMicroTZVector.class);
//
//        Schema schema = new Schema(ImmutableList.of(
//                new Field("1", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("2", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
//                new Field("3", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
//                new Field("4", FieldType.nullable(new ArrowType.Int(32, true)), null),
//                new Field("5", FieldType.nullable(new ArrowType.Int(16, true)), null),
//                new Field("6", FieldType.nullable(new ArrowType.Int(64, true)), null),
//                new Field("7", FieldType.nullable(new ArrowType.Bool()), null),
//                new Field("8", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
//                new Field("9", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
//                new Field("10", FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), null),
//                new Field("11", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null)));
//
//        when(mockRoot.getSchema()).thenReturn(schema);
//
//        when(mockRoot.getVector("1")).thenReturn(varcharVector);
//        when(mockRoot.getVector("2")).thenReturn(float4Vector);
//        when(mockRoot.getVector("3")).thenReturn(float8Vector);
//        when(mockRoot.getVector("4")).thenReturn(intVector);
//        when(mockRoot.getVector("5")).thenReturn(smallIntVector);
//        when(mockRoot.getVector("6")).thenReturn(bigIntVector);
//        when(mockRoot.getVector("7")).thenReturn(bitVector);
//        when(mockRoot.getVector("8")).thenReturn(decimalVector);
//        when(mockRoot.getVector("9")).thenReturn(dateDayVector);
//        when(mockRoot.getVector("10")).thenReturn(timeMicroVector);
//        when(mockRoot.getVector("11")).thenReturn(timestampMicroTZVector);
//
//        // Initialize parameter bindings
//        parameterBindings = Arrays.asList(
//                new ParameterBinding(Types.VARCHAR, "test"),
//                new ParameterBinding(Types.FLOAT, 1.23f),
//                new ParameterBinding(Types.DOUBLE, 4.56),
//                new ParameterBinding(Types.INTEGER, 123),
//                new ParameterBinding(Types.SMALLINT, (short) 12345),
//                new ParameterBinding(Types.BIGINT, 123456789L),
//                new ParameterBinding(Types.BOOLEAN, true),
//                new ParameterBinding(Types.DECIMAL, new BigDecimal("12345.67")),
//                new ParameterBinding(Types.DATE, Date.valueOf("2024-01-01")),
//                new ParameterBinding(Types.TIME, Time.valueOf("12:34:56")),
//                new ParameterBinding(Types.TIMESTAMP, Timestamp.valueOf("2024-01-01 12:34:56")));
//
//        calendar = Calendar.getInstance(); // Mock calendar as needed
//    }
//
//    @Test
//    public void testPopulateVectors() {
//        VectorPopulator.populateVectors(mockRoot, parameterBindings, null);
//
//        verify(mockRoot, times(1)).getVector("1");
//        verify(varcharVector, times(1)).setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
//        verify(mockRoot, times(1)).getVector("2");
//        verify(float4Vector, times(1)).setSafe(0, 1.23f);
//        verify(mockRoot, times(1)).getVector("3");
//        verify(float8Vector, times(1)).setSafe(0, 4.56);
//        verify(mockRoot, times(1)).getVector("4");
//        verify(intVector, times(1)).setSafe(0, 123);
//        verify(mockRoot, times(1)).getVector("5");
//        verify(smallIntVector, times(1)).setSafe(0, (short) 12345);
//        verify(mockRoot, times(1)).getVector("6");
//        verify(bigIntVector, times(1)).setSafe(0, 123456789L);
//        verify(mockRoot, times(1)).getVector("7");
//        verify(bitVector, times(1)).setSafe(0, 1);
//        verify(mockRoot, times(1)).getVector("8");
//        verify(decimalVector, times(1))
//                .setSafe(0, new BigDecimal("12345.67").unscaledValue().longValue());
//        verify(mockRoot, times(1)).getVector("9");
//        verify(dateDayVector, times(1))
//                .setSafe(0, (int) Date.valueOf("2024-01-01").toLocalDate().toEpochDay());
//        Time time = Time.valueOf("12:34:56");
//        LocalDateTime localDateTime = new java.sql.Timestamp(time.getTime()).toLocalDateTime();
//        localDateTime = DateTimeUtils.adjustForCalendar(localDateTime, calendar, TimeZone.getDefault());
//
//        long midnightMillis = localDateTime.toLocalTime().toNanoOfDay() / 1_000_000;
//        long expectedMicroseconds = DateTimeUtils.millisToMicrosecondsSinceMidnight(midnightMillis);
//
//        verify(mockRoot, times(1)).getVector("10");
//        verify(timeMicroVector, times(1)).setSafe(0, expectedMicroseconds);
//
//        Timestamp timestamp = Timestamp.valueOf("2024-01-01 12:34:56");
//        LocalDateTime localDateTime1 = timestamp.toLocalDateTime();
//        localDateTime1 = DateTimeUtils.adjustForCalendar(localDateTime1, calendar, TimeZone.getDefault());
//
//        long expectedTimestampInMicroseconds = DateTimeUtils.localDateTimeToMicrosecondsSinceEpoch(localDateTime1);
//
//        verify(mockRoot, times(1)).getVector("11");
//        verify(timestampMicroTZVector, times(1)).setSafe(0, expectedTimestampInMicroseconds);
//
//        verify(mockRoot, times(1)).setRowCount(1);
//    }
//
//    @Test
//    public void testPopulateVectorsWithNullParameterBindings() {
//        Schema schema = new Schema(ImmutableList.of(
//                new Field("1", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("2", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("3", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("4", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("5", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("6", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("7", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("8", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("9", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("10", FieldType.nullable(new ArrowType.Utf8()), null),
//                new Field("11", FieldType.nullable(new ArrowType.Utf8()), null)));
//
//        when(mockRoot.getSchema()).thenReturn(schema);
//
//        List<ParameterBinding> parameterBindings = ImmutableList.of(
//                new ParameterBinding(Types.VARCHAR, null),
//                new ParameterBinding(Types.FLOAT, null),
//                new ParameterBinding(Types.DOUBLE, null),
//                new ParameterBinding(Types.INTEGER, null),
//                new ParameterBinding(Types.SMALLINT, null),
//                new ParameterBinding(Types.BIGINT, null),
//                new ParameterBinding(Types.BOOLEAN, null),
//                new ParameterBinding(Types.DECIMAL, null),
//                new ParameterBinding(Types.DATE, null),
//                new ParameterBinding(Types.TIME, null),
//                new ParameterBinding(Types.TIMESTAMP, null));
//
//        VectorPopulator.populateVectors(mockRoot, parameterBindings, calendar);
//
//        verify(mockRoot, times(1)).getVector("1");
//        verify(varcharVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("2");
//        verify(float4Vector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("3");
//        verify(float8Vector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("4");
//        verify(intVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("5");
//        verify(smallIntVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("6");
//        verify(bigIntVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("7");
//        verify(bitVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("8");
//        verify(decimalVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("9");
//        verify(dateDayVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("10");
//        verify(timeMicroVector, times(1)).setNull(0);
//        verify(mockRoot, times(1)).getVector("11");
//        verify(timestampMicroTZVector, times(1)).setNull(0);
//
//        verify(mockRoot, times(1)).setRowCount(1);
//    }
// }
