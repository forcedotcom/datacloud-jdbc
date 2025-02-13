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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultParameterManagerTest {

    private DefaultParameterManager parameterManager;

    @BeforeEach
    void setUp() {
        parameterManager = new DefaultParameterManager();
    }

    @Test
    void testSetParameterValidIndex() throws SQLException {
        parameterManager.setParameter(1, java.sql.Types.VARCHAR, "TEST");
        List<ParameterBinding> parameters = parameterManager.getParameters();

        assertEquals(1, parameters.size());
        assertEquals("TEST", parameters.get(0).getValue());
        assertEquals(java.sql.Types.VARCHAR, parameters.get(0).getSqlType());
    }

    @Test
    void testSetParameterExpandingList() throws SQLException {
        parameterManager.setParameter(3, java.sql.Types.INTEGER, 42);
        List<ParameterBinding> parameters = parameterManager.getParameters();

        assertEquals(3, parameters.size());
        assertNull(parameters.get(0));
        assertNull(parameters.get(1));
        assertEquals(42, parameters.get(2).getValue());
        assertEquals(java.sql.Types.INTEGER, parameters.get(2).getSqlType());
    }

    @Test
    void testSetParameterNegativeIndexThrowsSQLException() {
        SQLException thrown = assertThrows(
                SQLException.class, () -> parameterManager.setParameter(0, java.sql.Types.VARCHAR, "TEST"));
        assertEquals("Parameter index must be greater than 0", thrown.getMessage());

        thrown = assertThrows(
                SQLException.class, () -> parameterManager.setParameter(-1, java.sql.Types.VARCHAR, "TEST"));
        assertEquals("Parameter index must be greater than 0", thrown.getMessage());
    }

    @Test
    void testClearParameters() throws SQLException {
        parameterManager.setParameter(1, java.sql.Types.VARCHAR, "TEST");
        parameterManager.setParameter(2, java.sql.Types.INTEGER, 123);

        parameterManager.clearParameters();
        List<ParameterBinding> parameters = parameterManager.getParameters();

        assertTrue(parameters.isEmpty());
    }
}
