/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.reference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import lombok.Data;

/**
 * Base class for type information that can handle both simple types and array object types.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION, defaultImpl = SimpleTypeInfo.class)
@JsonSubTypes({@JsonSubTypes.Type(value = SimpleTypeInfo.class), @JsonSubTypes.Type(value = ArrayTypeInfo.class)})
public interface TypeInfo {}

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

    public String getSqlTypeName() {
        String typeName = (String) typeArray.get(0).toString().toLowerCase();
        if (typeArray.size() == 3) {
            return typeName + "(" + typeArray.get(1).toString() + ")";
        } else if (typeArray.size() == 4) {
            if ("bignumeric".equals(typeName)) {
                typeName = "numeric";
            }
            return typeName + "(" + typeArray.get(1).toString() + ", "
                    + typeArray.get(2).toString() + ")";
        } else if ("char1".equals(typeName)) {
            return "char(1)";
        } else if ("double".equals(typeName)) {
            return "double precision";
        } else {
            return typeName;
        }
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
