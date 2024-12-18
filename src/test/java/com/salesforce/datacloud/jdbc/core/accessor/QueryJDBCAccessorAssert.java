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
package com.salesforce.datacloud.jdbc.core.accessor;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Objects;

/** {@link QueryJDBCAccessor} specific assertions - Generated by CustomAssertionGenerator. */
@javax.annotation.Generated(value = "assertj-assertions-generator")
public class QueryJDBCAccessorAssert extends AbstractObjectAssert<QueryJDBCAccessorAssert, QueryJDBCAccessor> {

    /**
     * Creates a new <code>{@link QueryJDBCAccessorAssert}</code> to make assertions on actual QueryJDBCAccessor.
     *
     * @param actual the QueryJDBCAccessor we want to make assertions on.
     */
    public QueryJDBCAccessorAssert(QueryJDBCAccessor actual) {
        super(actual, QueryJDBCAccessorAssert.class);
    }

    /**
     * An entry point for QueryJDBCAccessorAssert to follow AssertJ standard <code>assertThat()</code> statements.<br>
     * With a static import, one can write directly: <code>assertThat(myQueryJDBCAccessor)</code> and get specific
     * assertion with code completion.
     *
     * @param actual the QueryJDBCAccessor we want to make assertions on.
     * @return a new <code>{@link QueryJDBCAccessorAssert}</code>
     */
    @org.assertj.core.util.CheckReturnValue
    public static QueryJDBCAccessorAssert assertThat(QueryJDBCAccessor actual) {
        return new QueryJDBCAccessorAssert(actual);
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's array is equal to the given one.
     *
     * @param array the given array to compare the actual QueryJDBCAccessor's array to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's array is not equal to the given one.
     * @throws java.sql.SQLException if actual.getArray() throws one.
     */
    public QueryJDBCAccessorAssert hasArray(java.sql.Array array) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting array of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.Array actualArray = actual.getArray();
        if (!Objects.areEqual(actualArray, array)) {
            failWithMessage(assertjErrorMessage, actual, array, actualArray);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's asciiStream is equal to the given one.
     *
     * @param asciiStream the given asciiStream to compare the actual QueryJDBCAccessor's asciiStream to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's asciiStream is not equal to the given one.
     * @throws java.sql.SQLException if actual.getAsciiStream() throws one.
     */
    public QueryJDBCAccessorAssert hasAsciiStream(java.io.InputStream asciiStream) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting asciiStream of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.io.InputStream actualAsciiStream = actual.getAsciiStream();
        if (!Objects.areEqual(actualAsciiStream, asciiStream)) {
            failWithMessage(assertjErrorMessage, actual, asciiStream, actualAsciiStream);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's bigDecimal is equal to the given one.
     *
     * @param bigDecimal the given bigDecimal to compare the actual QueryJDBCAccessor's bigDecimal to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's bigDecimal is not equal to the given one.
     * @throws java.sql.SQLException if actual.getBigDecimal() throws one.
     */
    public QueryJDBCAccessorAssert hasBigDecimal(java.math.BigDecimal bigDecimal) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting bigDecimal of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.math.BigDecimal actualBigDecimal = actual.getBigDecimal();
        if (!Objects.areEqual(actualBigDecimal, bigDecimal)) {
            failWithMessage(assertjErrorMessage, actual, bigDecimal, actualBigDecimal);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's binaryStream is equal to the given one.
     *
     * @param binaryStream the given binaryStream to compare the actual QueryJDBCAccessor's binaryStream to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's binaryStream is not equal to the given one.
     * @throws java.sql.SQLException if actual.getBinaryStream() throws one.
     */
    public QueryJDBCAccessorAssert hasBinaryStream(java.io.InputStream binaryStream) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting binaryStream of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.io.InputStream actualBinaryStream = actual.getBinaryStream();
        if (!Objects.areEqual(actualBinaryStream, binaryStream)) {
            failWithMessage(assertjErrorMessage, actual, binaryStream, actualBinaryStream);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's blob is equal to the given one.
     *
     * @param blob the given blob to compare the actual QueryJDBCAccessor's blob to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's blob is not equal to the given one.
     * @throws java.sql.SQLException if actual.getBlob() throws one.
     */
    public QueryJDBCAccessorAssert hasBlob(java.sql.Blob blob) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting blob of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.Blob actualBlob = actual.getBlob();
        if (!Objects.areEqual(actualBlob, blob)) {
            failWithMessage(assertjErrorMessage, actual, blob, actualBlob);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's boolean is equal to the given one.
     *
     * @param expectedBoolean the given boolean to compare the actual QueryJDBCAccessor's boolean to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's boolean is not equal to the given one.
     * @throws java.sql.SQLException if actual.getBoolean() throws one.
     */
    public QueryJDBCAccessorAssert hasBoolean(boolean expectedBoolean) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting boolean of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check
        boolean actualBoolean = actual.getBoolean();
        if (actualBoolean != expectedBoolean) {
            failWithMessage(assertjErrorMessage, actual, expectedBoolean, actualBoolean);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's byte is equal to the given one.
     *
     * @param expectedByte the given byte to compare the actual QueryJDBCAccessor's byte to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's byte is not equal to the given one.
     * @throws java.sql.SQLException if actual.getByte() throws one.
     */
    public QueryJDBCAccessorAssert hasByte(byte expectedByte) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting byte of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check
        byte actualByte = actual.getByte();
        if (actualByte != expectedByte) {
            failWithMessage(assertjErrorMessage, actual, expectedByte, actualByte);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's bytes contains the given byte elements.
     *
     * @param bytes the given elements that should be contained in actual QueryJDBCAccessor's bytes.
     * @return this assertion object.
     * @throws AssertionError if the actual QueryJDBCAccessor's bytes does not contain all given byte elements.
     * @throws java.sql.SQLException if actual.getBytes() throws one.
     */
    public QueryJDBCAccessorAssert hasBytes(byte... bytes) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // check that given byte varargs is not null.
        if (bytes == null) failWithMessage("Expecting bytes parameter not to be null.");

        // check with standard error message (use overridingErrorMessage before contains to set your own
        // message).
        Assertions.assertThat(actual.getBytes()).contains(bytes);

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's bytes contains <b>only</b> the given byte elements and nothing else
     * in whatever order.
     *
     * @param bytes the given elements that should be contained in actual QueryJDBCAccessor's bytes.
     * @return this assertion object.
     * @throws AssertionError if the actual QueryJDBCAccessor's bytes does not contain all given byte elements and
     *     nothing else.
     * @throws java.sql.SQLException if actual.getBytes() throws one.
     */
    public QueryJDBCAccessorAssert hasOnlyBytes(byte... bytes) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // check that given byte varargs is not null.
        if (bytes == null) failWithMessage("Expecting bytes parameter not to be null.");

        // check with standard error message (use overridingErrorMessage before contains to set your own
        // message).
        Assertions.assertThat(actual.getBytes()).containsOnly(bytes);

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's bytes does not contain the given byte elements.
     *
     * @param bytes the given elements that should not be in actual QueryJDBCAccessor's bytes.
     * @return this assertion object.
     * @throws AssertionError if the actual QueryJDBCAccessor's bytes contains any given byte elements.
     * @throws java.sql.SQLException if actual.getBytes() throws one.
     */
    public QueryJDBCAccessorAssert doesNotHaveBytes(byte... bytes) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // check that given byte varargs is not null.
        if (bytes == null) failWithMessage("Expecting bytes parameter not to be null.");

        // check with standard error message (use overridingErrorMessage before contains to set your own
        // message).
        Assertions.assertThat(actual.getBytes()).doesNotContain(bytes);

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor has no bytes.
     *
     * @return this assertion object.
     * @throws AssertionError if the actual QueryJDBCAccessor's bytes is not empty.
     * @throws java.sql.SQLException if actual.getBytes() throws one.
     */
    public QueryJDBCAccessorAssert hasNoBytes() throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // we override the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting :\n  <%s>\nnot to have bytes but had :\n  <%s>";

        // check that it is not empty
        if (actual.getBytes().length > 0) {
            failWithMessage(assertjErrorMessage, actual, java.util.Arrays.toString(actual.getBytes()));
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's characterStream is equal to the given one.
     *
     * @param characterStream the given characterStream to compare the actual QueryJDBCAccessor's characterStream to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's characterStream is not equal to the given one.
     * @throws java.sql.SQLException if actual.getCharacterStream() throws one.
     */
    public QueryJDBCAccessorAssert hasCharacterStream(java.io.Reader characterStream) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting characterStream of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.io.Reader actualCharacterStream = actual.getCharacterStream();
        if (!Objects.areEqual(actualCharacterStream, characterStream)) {
            failWithMessage(assertjErrorMessage, actual, characterStream, actualCharacterStream);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's clob is equal to the given one.
     *
     * @param clob the given clob to compare the actual QueryJDBCAccessor's clob to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's clob is not equal to the given one.
     * @throws java.sql.SQLException if actual.getClob() throws one.
     */
    public QueryJDBCAccessorAssert hasClob(java.sql.Clob clob) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting clob of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.Clob actualClob = actual.getClob();
        if (!Objects.areEqual(actualClob, clob)) {
            failWithMessage(assertjErrorMessage, actual, clob, actualClob);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's double is equal to the given one.
     *
     * @param expectedDouble the given double to compare the actual QueryJDBCAccessor's double to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's double is not equal to the given one.
     * @throws java.sql.SQLException if actual.getDouble() throws one.
     */
    public QueryJDBCAccessorAssert hasDouble(double expectedDouble) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting double of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check value for double
        double actualDouble = actual.getDouble();
        if (actualDouble != expectedDouble) {
            failWithMessage(assertjErrorMessage, actual, expectedDouble, actualDouble);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's double is close to the given value by less than the given offset.
     *
     * <p>If difference is equal to the offset value, assertion is considered successful.
     *
     * @param expectedDouble the value to compare the actual QueryJDBCAccessor's double to.
     * @param assertjOffset the given offset.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's double is not close enough to the given value.
     * @throws java.sql.SQLException if actual.getDouble() throws one.
     */
    public QueryJDBCAccessorAssert hasDoubleCloseTo(double expectedDouble, double assertjOffset)
            throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        double actualDouble = actual.getDouble();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = String.format(
                "\nExpecting double:\n  <%s>\nto be close to:\n  <%s>\nby less than <%s> but difference was <%s>",
                actualDouble, expectedDouble, assertjOffset, Math.abs(expectedDouble - actualDouble));

        // check
        Assertions.assertThat(actualDouble)
                .overridingErrorMessage(assertjErrorMessage)
                .isCloseTo(expectedDouble, Assertions.within(assertjOffset));

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's float is equal to the given one.
     *
     * @param expectedFloat the given float to compare the actual QueryJDBCAccessor's float to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's float is not equal to the given one.
     * @throws java.sql.SQLException if actual.getFloat() throws one.
     */
    public QueryJDBCAccessorAssert hasFloat(float expectedFloat) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting float of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check value for float
        float actualFloat = actual.getFloat();
        if (actualFloat != expectedFloat) {
            failWithMessage(assertjErrorMessage, actual, expectedFloat, actualFloat);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's float is close to the given value by less than the given offset.
     *
     * <p>If difference is equal to the offset value, assertion is considered successful.
     *
     * @param expectedFloat the value to compare the actual QueryJDBCAccessor's float to.
     * @param assertjOffset the given offset.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's float is not close enough to the given value.
     * @throws java.sql.SQLException if actual.getFloat() throws one.
     */
    public QueryJDBCAccessorAssert hasFloatCloseTo(float expectedFloat, float assertjOffset)
            throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        float actualFloat = actual.getFloat();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = String.format(
                "\nExpecting float:\n  <%s>\nto be close to:\n  <%s>\nby less than <%s> but difference was <%s>",
                actualFloat, expectedFloat, assertjOffset, Math.abs(expectedFloat - actualFloat));

        // check
        Assertions.assertThat(actualFloat)
                .overridingErrorMessage(assertjErrorMessage)
                .isCloseTo(expectedFloat, Assertions.within(assertjOffset));

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's int is equal to the given one.
     *
     * @param expectedInt the given int to compare the actual QueryJDBCAccessor's int to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's int is not equal to the given one.
     * @throws java.sql.SQLException if actual.getInt() throws one.
     */
    public QueryJDBCAccessorAssert hasInt(int expectedInt) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting int of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check
        int actualInt = actual.getInt();
        if (actualInt != expectedInt) {
            failWithMessage(assertjErrorMessage, actual, expectedInt, actualInt);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's long is equal to the given one.
     *
     * @param expectedLong the given long to compare the actual QueryJDBCAccessor's long to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's long is not equal to the given one.
     * @throws java.sql.SQLException if actual.getLong() throws one.
     */
    public QueryJDBCAccessorAssert hasLong(long expectedLong) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting long of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check
        long actualLong = actual.getLong();
        if (actualLong != expectedLong) {
            failWithMessage(assertjErrorMessage, actual, expectedLong, actualLong);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's nCharacterStream is equal to the given one.
     *
     * @param nCharacterStream the given nCharacterStream to compare the actual QueryJDBCAccessor's nCharacterStream to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's nCharacterStream is not equal to the given one.
     * @throws java.sql.SQLException if actual.getNCharacterStream() throws one.
     */
    public QueryJDBCAccessorAssert hasNCharacterStream(java.io.Reader nCharacterStream) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting nCharacterStream of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.io.Reader actualNCharacterStream = actual.getNCharacterStream();
        if (!Objects.areEqual(actualNCharacterStream, nCharacterStream)) {
            failWithMessage(assertjErrorMessage, actual, nCharacterStream, actualNCharacterStream);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's nClob is equal to the given one.
     *
     * @param nClob the given nClob to compare the actual QueryJDBCAccessor's nClob to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's nClob is not equal to the given one.
     * @throws java.sql.SQLException if actual.getNClob() throws one.
     */
    public QueryJDBCAccessorAssert hasNClob(java.sql.NClob nClob) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting nClob of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.NClob actualNClob = actual.getNClob();
        if (!Objects.areEqual(actualNClob, nClob)) {
            failWithMessage(assertjErrorMessage, actual, nClob, actualNClob);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's nString is equal to the given one.
     *
     * @param nString the given nString to compare the actual QueryJDBCAccessor's nString to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's nString is not equal to the given one.
     * @throws java.sql.SQLException if actual.getNString() throws one.
     */
    public QueryJDBCAccessorAssert hasNString(String nString) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting nString of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        String actualNString = actual.getNString();
        if (!Objects.areEqual(actualNString, nString)) {
            failWithMessage(assertjErrorMessage, actual, nString, actualNString);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor was null.
     *
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor was not null.
     */
    public QueryJDBCAccessorAssert wasNull() {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // check that property call/field access is true
        if (!actual.wasNull()) {
            failWithMessage("\nExpecting that actual QueryJDBCAccessor was null but was not.");
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor was not null.
     *
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor was null.
     */
    public QueryJDBCAccessorAssert wasNotNull() {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // check that property call/field access is false
        if (actual.wasNull()) {
            failWithMessage("\nExpecting that actual QueryJDBCAccessor was not null but was.");
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's object is equal to the given one.
     *
     * @param object the given object to compare the actual QueryJDBCAccessor's object to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's object is not equal to the given one.
     * @throws java.sql.SQLException if actual.getObject() throws one.
     */
    public QueryJDBCAccessorAssert hasObject(Object object) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting object of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        Object actualObject = actual.getObject();
        if (!Objects.areEqual(actualObject, object)) {
            failWithMessage(assertjErrorMessage, actual, object, actualObject);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's objectClass is equal to the given one.
     *
     * @param objectClass the given objectClass to compare the actual QueryJDBCAccessor's objectClass to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's objectClass is not equal to the given one.
     */
    public QueryJDBCAccessorAssert hasObjectClass(Class objectClass) {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting objectClass of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        Class actualObjectClass = actual.getObjectClass();
        if (!Objects.areEqual(actualObjectClass, objectClass)) {
            failWithMessage(assertjErrorMessage, actual, objectClass, actualObjectClass);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's ref is equal to the given one.
     *
     * @param ref the given ref to compare the actual QueryJDBCAccessor's ref to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's ref is not equal to the given one.
     * @throws java.sql.SQLException if actual.getRef() throws one.
     */
    public QueryJDBCAccessorAssert hasRef(java.sql.Ref ref) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting ref of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.Ref actualRef = actual.getRef();
        if (!Objects.areEqual(actualRef, ref)) {
            failWithMessage(assertjErrorMessage, actual, ref, actualRef);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's sQLXML is equal to the given one.
     *
     * @param sQLXML the given sQLXML to compare the actual QueryJDBCAccessor's sQLXML to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's sQLXML is not equal to the given one.
     * @throws java.sql.SQLException if actual.getSQLXML() throws one.
     */
    public QueryJDBCAccessorAssert hasSQLXML(java.sql.SQLXML sQLXML) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting sQLXML of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.SQLXML actualSQLXML = actual.getSQLXML();
        if (!Objects.areEqual(actualSQLXML, sQLXML)) {
            failWithMessage(assertjErrorMessage, actual, sQLXML, actualSQLXML);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's short is equal to the given one.
     *
     * @param expectedShort the given short to compare the actual QueryJDBCAccessor's short to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's short is not equal to the given one.
     * @throws java.sql.SQLException if actual.getShort() throws one.
     */
    public QueryJDBCAccessorAssert hasShort(short expectedShort) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting short of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // check
        short actualShort = actual.getShort();
        if (actualShort != expectedShort) {
            failWithMessage(assertjErrorMessage, actual, expectedShort, actualShort);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's string is equal to the given one.
     *
     * @param string the given string to compare the actual QueryJDBCAccessor's string to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's string is not equal to the given one.
     * @throws java.sql.SQLException if actual.getString() throws one.
     */
    public QueryJDBCAccessorAssert hasString(String string) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting string of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        String actualString = actual.getString();
        if (!Objects.areEqual(actualString, string)) {
            failWithMessage(assertjErrorMessage, actual, string, actualString);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's struct is equal to the given one.
     *
     * @param struct the given struct to compare the actual QueryJDBCAccessor's struct to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's struct is not equal to the given one.
     * @throws java.sql.SQLException if actual.getStruct() throws one.
     */
    public QueryJDBCAccessorAssert hasStruct(java.sql.Struct struct) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting struct of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.sql.Struct actualStruct = actual.getStruct();
        if (!Objects.areEqual(actualStruct, struct)) {
            failWithMessage(assertjErrorMessage, actual, struct, actualStruct);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's uRL is equal to the given one.
     *
     * @param uRL the given uRL to compare the actual QueryJDBCAccessor's uRL to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's uRL is not equal to the given one.
     * @throws java.sql.SQLException if actual.getURL() throws one.
     */
    public QueryJDBCAccessorAssert hasURL(java.net.URL uRL) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting uRL of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.net.URL actualURL = actual.getURL();
        if (!Objects.areEqual(actualURL, uRL)) {
            failWithMessage(assertjErrorMessage, actual, uRL, actualURL);
        }

        // return the current assertion for method chaining
        return this;
    }

    /**
     * Verifies that the actual QueryJDBCAccessor's unicodeStream is equal to the given one.
     *
     * @param unicodeStream the given unicodeStream to compare the actual QueryJDBCAccessor's unicodeStream to.
     * @return this assertion object.
     * @throws AssertionError - if the actual QueryJDBCAccessor's unicodeStream is not equal to the given one.
     * @throws java.sql.SQLException if actual.getUnicodeStream() throws one.
     */
    public QueryJDBCAccessorAssert hasUnicodeStream(java.io.InputStream unicodeStream) throws java.sql.SQLException {
        // check that actual QueryJDBCAccessor we want to make assertions on is not null.
        isNotNull();

        // overrides the default error message with a more explicit one
        String assertjErrorMessage = "\nExpecting unicodeStream of:\n  <%s>\nto be:\n  <%s>\nbut was:\n  <%s>";

        // null safe check
        java.io.InputStream actualUnicodeStream = actual.getUnicodeStream();
        if (!Objects.areEqual(actualUnicodeStream, unicodeStream)) {
            failWithMessage(assertjErrorMessage, actual, unicodeStream, actualUnicodeStream);
        }

        // return the current assertion for method chaining
        return this;
    }
}
