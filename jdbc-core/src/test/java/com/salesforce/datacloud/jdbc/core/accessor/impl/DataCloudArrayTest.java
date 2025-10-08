/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
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
        if (this.dataVector != null) {
            this.dataVector.close();
        }
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
        val array = (Object[]) dataCloudArray.getArray(1, 2);
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

    @SneakyThrows
    @Test
    void testGetArrayWithEmptyArray() {
        // Test empty array case
        val values = ImmutableList.<Integer>of();
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, 0);
        val array = (Object[]) dataCloudArray.getArray();
        collector.assertThat(array).isEmpty();
        collector.assertThat(array.length).isEqualTo(0);
    }

    @SneakyThrows
    @Test
    void testJdbcArrayBoundaryCompliance() {
        // Comprehensive test for JDBC Array boundary compliance
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, 0); // Empty array

        // VALID CASES (should work)
        // getArray(1, 0) on empty array - valid JDBC operation
        val result = (Object[]) dataCloudArray.getArray(1, 0);
        collector.assertThat(result).isEmpty();

        // INVALID CASES (should fail)
        // Invalid JDBC index (0-based)
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(0, 0),
                "Index 0 is invalid (JDBC arrays use 1-based indexing)");

        // Index out of bounds for empty array
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(2, 0),
                "Index 2 is out of bounds for array size 0");

        // Count exceeds bounds
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(1, 1),
                "Index 1 + count 1 exceeds array size 0");
    }

    @SneakyThrows
    @Test
    void testJdbcArrayBoundaryComplianceWithNonEmptyArray() {
        // Test JDBC boundary compliance with non-empty array
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());

        // VALID CASES
        // getArray(1, 0) - valid zero count
        val result1 = (Object[]) dataCloudArray.getArray(1, 0);
        collector.assertThat(result1).isEmpty();

        // getArray(1, 1) - valid single element
        val result2 = (Object[]) dataCloudArray.getArray(1, 1);
        collector.assertThat(result2).hasSize(1);
        collector.assertThat(result2[0]).isEqualTo(1);

        // getArray(2, 2) - valid range
        val result3 = (Object[]) dataCloudArray.getArray(2, 2);
        collector.assertThat(result3).hasSize(2);
        collector.assertThat(result3[0]).isEqualTo(2);
        collector.assertThat(result3[1]).isEqualTo(3);

        // INVALID CASES
        // Invalid JDBC index (0-based)
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(0, 1),
                "Index 0 is invalid (JDBC arrays use 1-based indexing)");

        // Index out of bounds
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(4, 1),
                "Index 4 is out of bounds for array size 3");

        // Count exceeds bounds
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(2, 3),
                "Index 2 + count 3 exceeds array size 3");
    }

    @SneakyThrows
    @Test
    void testGetArrayWithStringArray() {
        val values = ImmutableList.of("abc", "def", "ghi", "jkl", "mno");
        dataVector = extension.createVarCharVectorFrom(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        val array = (Object[]) dataCloudArray.getArray();

        collector.assertThat(array).hasSize(5);
        // VarCharVector.getObject() returns Text objects, not String objects
        collector.assertThat(array[0]).isEqualTo(new Text("abc"));
        collector.assertThat(array[1]).isEqualTo(new Text("def"));
        collector.assertThat(array[2]).isEqualTo(new Text("ghi"));
        collector.assertThat(array[3]).isEqualTo(new Text("jkl"));
        collector.assertThat(array[4]).isEqualTo(new Text("mno"));
    }

    @SneakyThrows
    @Test
    void testGetArrayWithStringArrayAndNulls() {
        VarCharVector varcharVector = new VarCharVector("test-varchar-vector", extension.getRootAllocator());
        varcharVector.allocateNew(6);

        // Set values with NULL handling
        varcharVector.setSafe(0, "abc".getBytes(StandardCharsets.UTF_8));
        varcharVector.setNull(1); // Set NULL at index 1
        varcharVector.setSafe(2, "def".getBytes(StandardCharsets.UTF_8));
        varcharVector.setSafe(3, "ghi".getBytes(StandardCharsets.UTF_8));
        varcharVector.setSafe(4, "jkl".getBytes(StandardCharsets.UTF_8));
        varcharVector.setSafe(5, "mno".getBytes(StandardCharsets.UTF_8));

        varcharVector.setValueCount(6);

        dataVector = varcharVector;

        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());
        val array = (Object[]) dataCloudArray.getArray();

        collector.assertThat(array).hasSize(6);

        // VarCharVector.getObject() returns Text objects, not String objects
        collector.assertThat(array[0]).isEqualTo(new Text("abc"));
        collector.assertThat(array[1]).isNull();
        collector.assertThat(array[2]).isEqualTo(new Text("def"));
        collector.assertThat(array[3]).isEqualTo(new Text("ghi"));
        collector.assertThat(array[4]).isEqualTo(new Text("jkl"));
        collector.assertThat(array[5]).isEqualTo(new Text("mno"));
    }

    @SneakyThrows
    @Test
    void testConstructorWithInvalidStartOffset() {
        // Test extractDataFromVector with invalid start offset (covers line 46-47)
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);

        // Test negative start offset
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> new DataCloudArray(dataVector, -1, 2));

        // Test start offset >= vector size
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> new DataCloudArray(dataVector, dataVector.getValueCount(), 1));
    }

    @SneakyThrows
    @Test
    void testConstructorWithInvalidEndOffset() {
        // Test extractDataFromVector with end offset exceeding vector size (covers line 52-53)
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);

        // Test end offset exceeding vector size
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> new DataCloudArray(dataVector, 1, 3)); // start=1, count=3, end=4 > size=3
    }

    @SneakyThrows
    @Test
    void testGetArrayWithIndexPlusCountExceedingArraySize() {
        // Test checkBoundaries with index + count exceeding array size (covers line 137-138)
        val values = ImmutableList.of(1, 2, 3);
        dataVector = extension.createIntVector(values);
        val dataCloudArray = new DataCloudArray(dataVector, 0, dataVector.getValueCount());

        // Test index + count exceeding array size (JDBC 1-based indexing)
        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> dataCloudArray.getArray(2, 3)); // index=2, count=3, end=4 > size=3
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
        AssertionsForClassTypes.assertThat(e).hasRootCauseInstanceOf(SQLException.class);
        AssertionsForClassTypes.assertThat(e).hasMessageContaining("Array method is not supported in Data Cloud query");
    }
}
