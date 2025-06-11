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

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Represents a baseline entry containing a SQL query, its associated column metadata,
 * and the returned values from query execution.
 * Used for generating baseline expectation files.
 */
@Data
@Builder
@Jacksonized
public class ReferenceEntry {
    private final String query;
    private final List<ColumnMetadata> columnMetadata;

    /**
     * The returned values from the query execution. Each row is represented as a List of ValueWithClass objects,
     * where each ValueWithClass contains both the string representation and the Java class name.
     */
    private final List<List<ValueWithClass>> returnedValues;
}
