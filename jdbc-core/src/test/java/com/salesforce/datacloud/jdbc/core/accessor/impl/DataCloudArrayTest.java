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
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.sql.Types;
import java.util.HashMap;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.ThrowingConsumer;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({SoftAssertionsExtension.class})
public class DataCloudArrayTest {
    @InjectSoftAssertions
    private SoftAssertions collector;

    FieldVector dataVector;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @AfterEach
    public void tearDown() {
        this.dataVector.close();
    }

    @SneakyThrows
    @Test
    void testGetBaseTypeReturnsCorrectBaseType() {
        val values = ImmutableList.of(true, false);
        dataVector = extension.createBitVector(values);
        val array = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        collector.assertThat(array.getBaseType()).isEqualTo(Types.BOOLEAN);
    }

    @SneakyThrows
    @Test
    void testGetBaseTypeNameReturnsCorrectBaseTypeName() {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val array = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        collector.assertThat(array.getBaseTypeName()).isEqualTo("INTEGER");
    }

    @SneakyThrows
    @Test
    void testGetArrayReturnsCorrectArray() {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        val array = (Object[]) dataCloudArray.getArray();
        val expected = values.toArray();
        collector.assertThat(array).isEqualTo(expected);
        collector.assertThat(array.length).isEqualTo(expected.length);
    }

    @SneakyThrows
    @Test
    void testGetArrayWithCorrectOffsetReturnsCorrectArray() {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        val array = (Object[]) dataCloudArray.getArray(0, 2);
        val expected = ImmutableList.of(1, 2).toArray();
        collector.assertThat(array).isEqualTo(expected);
        collector.assertThat(array.length).isEqualTo(expected.length);
    }

    @SneakyThrows
    @Test
    void testShouldThrowIfGetArrayHasIncorrectOffset() {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        assertThrows(
                ArrayIndexOutOfBoundsException.class, () -> dataCloudArray.getArray(0, dataVector.getValueCount() + 1));
    }

    @SneakyThrows
    @Test
    void testShouldThrowIfGetArrayHasIncorrectIndex() {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        assertThrows(
                ArrayIndexOutOfBoundsException.class, () -> dataCloudArray.getArray(-1, dataVector.getValueCount()));
    }

    private static Arguments impl(String name, ThrowingConsumer<DataCloudArray> impl) {
        return arguments(named(name, impl));
    }

    private static Stream<Arguments> unsupported() {
        return Stream.of(
                impl("getArray with map", a -> a.getArray(new HashMap<>())),
                impl("getArray with map & index", a -> a.getArray(0, 1, new HashMap<>())),
                impl("getResultSet", DataCloudArray::getResultSet),
                impl("getResultSet with map", a -> a.getResultSet(new HashMap<>())),
                impl("getResultSet with map & index", a -> a.getResultSet(0, 1, new HashMap<>())),
                impl("getResultSet with count & index", a -> a.getResultSet(0, 1)));
    }

    @ParameterizedTest
    @MethodSource("unsupported")
    @SneakyThrows
    void testUnsupportedOperations(ThrowingConsumer<DataCloudArray> func) {
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());

        val e = Assertions.assertThrows(RuntimeException.class, () -> func.accept(dataCloudArray));
        AssertionsForClassTypes.assertThat(e).hasRootCauseInstanceOf(DataCloudJDBCException.class);
        AssertionsForClassTypes.assertThat(e).hasMessageContaining("Array method is not supported in Data Cloud query");
    }
}
