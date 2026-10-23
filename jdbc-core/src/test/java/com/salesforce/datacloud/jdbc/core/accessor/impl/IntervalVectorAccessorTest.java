/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.protocol.data.ArrowToHyperTypeMapper;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class IntervalVectorAccessorTest {

    private static final long MICROS = 1_000L;
    private static final long SECOND = 1_000_000_000L;
    private static final long MINUTE = 60 * SECOND;
    private static final long HOUR = 60 * MINUTE;

    /** Expected strings are what Hyper prints for {@code interval::text}. */
    @ParameterizedTest(name = "{0}m {1}d {2}ns -> {3}")
    @CsvSource({
        "0, 1, 0, P1D",
        "14, 3, 14706789000000, P1Y2M3DT4H5M6.789000S",
        "0, -1, 7200000000000, P-1DT2H",
        "0, 0, 0, PT0S",
        "0, 0, 5000000000, PT5S",
        "0, 0, 1500000000, PT1.500000S",
        "0, 0, -1500000000, PT-1.500000S",
        "12, 0, 0, P1Y",
        "-14, 0, 0, P-1Y-2M",
        "0, 0, 1800000000000, PT30M",
        "0, 0, -19805000000000, PT-5H-30M-5S",
        "0, 0, 129600000000000, PT36H",
        "0, 0, 1000, PT0.000001S",
        "1200, 0, 0, P100Y",
    })
    void formatMatchesHyperTextOutput(int months, int days, long nanos, String expected) {
        assertThat(IntervalVectorAccessor.format(months, days, nanos)).isEqualTo(expected);
    }

    @Test
    void formatHandlesNanosecondPrecisionAndExtremes() {
        assertThat(IntervalVectorAccessor.format(0, 0, 1)).isEqualTo("PT0.000000001S");
        assertThat(IntervalVectorAccessor.format(Integer.MAX_VALUE, Integer.MAX_VALUE, 0))
                .isEqualTo("P178956970Y7M2147483647D");
        assertThat(IntervalVectorAccessor.format(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE))
                .startsWith("P-178956970Y-8M-2147483648DT-")
                .endsWith("S");
    }

    @Test
    void accessorReadsValuesAndNulls() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                IntervalMonthDayNanoVector vector = new IntervalMonthDayNanoVector("c", allocator)) {
            vector.allocateNew(3);
            vector.setSafe(0, 14, 3, 4 * HOUR + 5 * MINUTE + 6 * SECOND + 789 * 1_000 * MICROS);
            vector.setNull(1);
            vector.setSafe(2, 0, 0, 0);
            vector.setValueCount(3);

            AtomicInteger row = new AtomicInteger();
            QueryJDBCAccessor accessor = QueryJDBCAccessorFactory.createAccessor(vector, row::get);

            assertThat(accessor.getObjectClass()).isEqualTo(String.class);
            assertThat(accessor.getString()).isEqualTo("P1Y2M3DT4H5M6.789000S");
            assertThat(accessor.wasNull()).isFalse();

            row.set(1);
            assertThat(accessor.getObject()).isNull();
            assertThat(accessor.wasNull()).isTrue();

            row.set(2);
            assertThat(accessor.getObject()).isEqualTo("PT0S");
            assertThat(accessor.wasNull()).isFalse();
        }
    }

    @Test
    void arrowMonthDayNanoMapsToHyperInterval() {
        Field field =
                new Field("c", new FieldType(true, new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), null), null);
        assertThat(ArrowToHyperTypeMapper.toHyperType(field)).isEqualTo(HyperType.interval(true));
    }

    @Test
    void otherArrowIntervalUnitsAreStillUnsupported() {
        for (IntervalUnit unit : new IntervalUnit[] {IntervalUnit.YEAR_MONTH, IntervalUnit.DAY_TIME}) {
            Field field = new Field("c", new FieldType(true, new ArrowType.Interval(unit), null), null);
            assertThatThrownBy(() -> ArrowToHyperTypeMapper.toHyperType(field))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported Arrow type");
        }
    }
}
