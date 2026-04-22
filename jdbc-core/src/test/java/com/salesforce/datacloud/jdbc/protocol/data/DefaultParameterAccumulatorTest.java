/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultParameterAccumulatorTest {

    private DefaultParameterAccumulator accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new DefaultParameterAccumulator();
    }

    @Test
    void testSetParameterValidIndex() {
        accumulator.setParameter(1, HyperType.varcharUnlimited(true), "TEST");
        List<ParameterBinding> parameters = accumulator.getParameters();

        assertEquals(1, parameters.size());
        assertEquals("TEST", parameters.get(0).getValue());
        assertEquals(HyperType.varcharUnlimited(true), parameters.get(0).getType());
    }

    @Test
    void testSetParameterExpandingList() {
        accumulator.setParameter(3, HyperType.int32(false), 42);
        List<ParameterBinding> parameters = accumulator.getParameters();

        assertEquals(3, parameters.size());
        assertNull(parameters.get(0));
        assertNull(parameters.get(1));
        assertEquals(42, parameters.get(2).getValue());
        assertEquals(HyperType.int32(false), parameters.get(2).getType());
    }

    @Test
    void testSetParameterNegativeIndexThrows() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> accumulator.setParameter(0, HyperType.varcharUnlimited(true), "TEST"));
        assertEquals("Parameter index must be greater than 0", thrown.getMessage());

        thrown = assertThrows(
                IllegalArgumentException.class,
                () -> accumulator.setParameter(-1, HyperType.varcharUnlimited(true), "TEST"));
        assertEquals("Parameter index must be greater than 0", thrown.getMessage());
    }

    @Test
    void testClearParameters() {
        accumulator.setParameter(1, HyperType.varcharUnlimited(true), "TEST");
        accumulator.setParameter(2, HyperType.int32(false), 123);

        accumulator.clearParameters();
        List<ParameterBinding> parameters = accumulator.getParameters();

        assertTrue(parameters.isEmpty());
    }
}
