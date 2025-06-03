package com.salesforce.datacloud.reference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

import java.util.List;

/**
 * Base class for type information that can handle both simple types and array object types.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.DEDUCTION,
    defaultImpl = SimpleTypeInfo.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = SimpleTypeInfo.class),
    @JsonSubTypes.Type(value = ArrayTypeInfo.class)
})
public interface TypeInfo {
}

/**
 * Represents a simple type defined as an array (e.g., ["Geography", "nullable"] or ["Varchar", 10, "nullable"])
 */
@Data
class SimpleTypeInfo implements TypeInfo {
    // This will be deserialized as a List<Object> to handle mixed types (strings and integers)
    private List<Object> typeArray;

    // Default constructor for Jackson
    public SimpleTypeInfo() {}

    // Constructor that accepts the array directly
    @JsonCreator
    public SimpleTypeInfo(List<Object> typeArray) {
        this.typeArray = typeArray;
    }
}

/**
 * Represents an array type with nested structure (e.g., {"type": "Array", "nullable": true, "inner": [...]})
 */
@Data
class ArrayTypeInfo implements TypeInfo {

    @JsonProperty("type")
    private String type;

    @JsonProperty("nullable")
    private Boolean nullable;

    @JsonProperty("inner")
    private SimpleTypeInfo inner;
}