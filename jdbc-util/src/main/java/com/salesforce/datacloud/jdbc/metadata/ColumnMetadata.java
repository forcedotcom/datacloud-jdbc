package com.salesforce.datacloud.jdbc.metadata;

/**
 * Represents a field / column in a result set.
 */
@lombok.Value
public class ColumnMetadata {
    private final String name;
    private final SqlType type;
}