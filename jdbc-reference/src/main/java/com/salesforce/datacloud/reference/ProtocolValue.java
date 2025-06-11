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
package com.salesforce.datacloud.reference;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Represents a protocol value test case from the JSON array.
 * Each protocol value contains a type specification, SQL query, expected result, and interestingness level.
 */
@Data
@Builder
@Jacksonized
public class ProtocolValue {

    public static final String PROTOCOL_VALUES_JSON = "/protocolvalues.json";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Enum representing the interestingness level of a protocol value test case.
     */
    public enum Interestingness {
        Null,
        Default,
        ProtocolTestOnly,
        Low,
        High
    }

    private TypeInfo type;
    private String sql;
    private String expectation;
    private Interestingness interestingness;

    /**
     * Checks if the type field represents a simple array type (e.g., ["Geography", "nullable"])
     *
     * @return true if type is a List, false otherwise
     */
    public boolean isSimpleType() {
        return type instanceof SimpleTypeInfo;
    }

    /**
     * Checks if the type field represents an array object type (e.g., {"type": "Array", ...})
     *
     * @return true if type is a Map, false otherwise
     */
    public boolean isArrayType() {
        return type instanceof ArrayTypeInfo;
    }

    /**
     * Gets the type as a SimpleTypeInfo if it's a simple type.
     *
     * @return the type as SimpleTypeInfo, or null if not a simple type
     */
    public SimpleTypeInfo getSimpleType() {
        assert isSimpleType();
        return (SimpleTypeInfo) type;
    }

    /**
     * Gets the type as an ArrayTypeInfo if it's an array type.
     *
     * @return the type as ArrayTypeInfo, or null if not an array type
     */
    public ArrayTypeInfo getArrayType() {
        assert isArrayType();
        return (ArrayTypeInfo) type;
    }

    /**
     * Loads all protocol values from the JSON resource file.
     *
     * @return List of protocol values parsed from the JSON file
     * @throws IOException if there's an error reading or parsing the file
     */
    public static List<ProtocolValue> loadProtocolValues() throws IOException {
        try (InputStream inputStream = ProtocolValue.class.getResourceAsStream(PROTOCOL_VALUES_JSON)) {
            return objectMapper.readValue(inputStream, new TypeReference<List<ProtocolValue>>() {});
        }
    }
}
