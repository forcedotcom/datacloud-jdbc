/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertyParsingUtilsTest {
    @Test
    void takeOptional_shouldReturnEmpty_whenPropertiesIsNull() {
        Optional<String> result = PropertyParsingUtils.takeOptional(null, "key");
        assertThat(result).isEmpty();
    }

    @Test
    void takeOptional_shouldReturnEmpty_whenKeyDoesNotExist() {
        Properties properties = new Properties();
        Optional<String> result = PropertyParsingUtils.takeOptional(properties, "nonexistent");
        assertThat(result).isEmpty();
        assertThat(properties).isEmpty();
    }

    @Test
    void takeOptional_shouldReturnValueAndRemoveFromProperties_whenKeyExists() {
        Properties properties = new Properties();
        properties.setProperty("key", "value");

        Optional<String> result = PropertyParsingUtils.takeOptional(properties, "key");

        assertThat(result).contains("value");
        assertThat(properties).isEmpty();
    }

    @Test
    void takeOptional_shouldThrowException_whenValueIsNotString() {
        Properties properties = new Properties();
        properties.put("key", 123);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> PropertyParsingUtils.takeOptional(properties, "key"));

        assertThat(exception).hasMessage("Property `key` is a non-string value");
    }

    @Test
    void takeRequired_shouldReturnValue_whenKeyExists() {
        Properties properties = new Properties();
        properties.setProperty("key", "value");

        String result = PropertyParsingUtils.takeRequired(properties, "key");

        assertThat(result).isEqualTo("value");
        assertThat(properties).isEmpty();
    }

    @Test
    void takeRequired_shouldThrowException_whenKeyDoesNotExist() {
        Properties properties = new Properties();
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> PropertyParsingUtils.takeRequired(properties, "nonexistent"));

        assertThat(exception).hasMessage("Property `nonexistent` is missing");
    }

    @Test
    void takeOptionalInteger() {
        Properties properties = new Properties();
        properties.setProperty("number", "42");
        properties.setProperty("not-a-number", "not-a-number");

        Optional<Integer> nonexistent = PropertyParsingUtils.takeOptionalInteger(properties, "nonexistent");
        assertThat(nonexistent).isEmpty();

        Optional<Integer> result = PropertyParsingUtils.takeOptionalInteger(properties, "number");
        assertThat(result).contains(42);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PropertyParsingUtils.takeOptionalInteger(properties, "not-a-number"));
        assertThat(exception)
                .hasMessage(
                        "Failed to parse `not-a-number` property as an integer: For input string: \"not-a-number\"");

        assertThat(properties).isEmpty();
    }

    @Test
    void takeOptionalBoolean() {
        Properties properties = new Properties();
        properties.setProperty("true", "true");
        properties.setProperty("false", "false");
        properties.setProperty("not-a-boolean", "not-a-boolean");

        Optional<Boolean> nonexistent = PropertyParsingUtils.takeOptionalBoolean(properties, "nonexistent");
        assertThat(nonexistent).isEmpty();

        Optional<Boolean> resultTrue = PropertyParsingUtils.takeOptionalBoolean(properties, "true");
        assertThat(resultTrue).contains(true);

        Optional<Boolean> resultFalse = PropertyParsingUtils.takeOptionalBoolean(properties, "false");
        assertThat(resultFalse).contains(false);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PropertyParsingUtils.takeOptionalBoolean(properties, "not-a-boolean"));
        assertThat(exception).hasMessage("Failed to parse `not-a-boolean` property as a boolean");

        assertThat(properties).isEmpty();
    }

    enum TestEnum {
        VALUE1,
        VALUE2,
        VALUE3
    }

    @Test
    void takeOptionalEnum_shouldReturnEnumValue_whenValidEnum() {
        Properties properties = new Properties();
        properties.setProperty("key", "VALUE2");
        properties.setProperty("invalid", "INVALID");

        Optional<TestEnum> result = PropertyParsingUtils.takeOptionalEnum(properties, "key", TestEnum.class);
        assertThat(result).contains(TestEnum.VALUE2);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PropertyParsingUtils.takeOptionalEnum(properties, "invalid", TestEnum.class));
        assertThat(exception)
                .hasMessage("Failed to parse `invalid` property. Valid values are: [VALUE1, VALUE2, VALUE3]");

        assertThat(properties).isEmpty();
    }

    @Test
    void takeOptionalList() {
        Properties properties = new Properties();
        properties.setProperty("key", "value1,value2,value3");

        Optional<List<String>> result = PropertyParsingUtils.takeOptionalList(properties, "key");

        assertThat(result).contains(Arrays.asList("value1", "value2", "value3"));
        assertThat(properties).isEmpty();
    }

    @Test
    void validateRemainingProperties_shouldNotThrow_whenPropertiesIsNull() throws DataCloudJDBCException {
        PropertyParsingUtils.validateRemainingProperties(null);
        // Should not throw
    }

    @Test
    void validateRemainingProperties_shouldNotThrow_whenPropertiesIsEmpty() throws DataCloudJDBCException {
        Properties properties = new Properties();
        PropertyParsingUtils.validateRemainingProperties(properties);
        // Should not throw
    }

    @Test
    void validateRemainingProperties_shouldThrowException_whenUnknownPropertiesExist() {
        Properties properties = new Properties();
        properties.setProperty("unknown1", "value1");
        properties.setProperty("unknown2", "value2");

        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> PropertyParsingUtils.validateRemainingProperties(properties));

        assertThat(exception.getMessage()).startsWith("Unknown JDBC properties: ");
        assertThat(exception.getMessage()).contains("unknown1");
        assertThat(exception.getMessage()).contains("unknown2");
    }

    @Test
    void validateRemainingProperties_shouldThrowExceptionWithHelpfulMessage_whenCommonHyperSettingsWithoutPrefix() {
        Properties properties = new Properties();
        properties.setProperty("time_zone", "UTC");

        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> PropertyParsingUtils.validateRemainingProperties(properties));

        assertThat(exception.getMessage())
                .isEqualTo("Invalid property 'time_zone'. Use 'querySetting.time_zone' to set the query setting.");
    }

    @Test
    void validateRemainingProperties_shouldThrowExceptionWithHelpfulMessage_whenCommonHyperSettingsWithDifferentCase() {
        Properties properties = new Properties();
        properties.setProperty("TIME-ZONE", "UTC");

        DataCloudJDBCException exception = assertThrows(
                DataCloudJDBCException.class, () -> PropertyParsingUtils.validateRemainingProperties(properties));

        assertThat(exception.getMessage())
                .isEqualTo("Invalid property 'TIME-ZONE'. Use 'querySetting.time_zone' to set the query setting.");
    }
}
