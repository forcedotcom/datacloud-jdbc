/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.metadata;

/**
 * Represents a field / column in a result set.
 */
@lombok.Value
public class ColumnMetadata {
    private final String name;
    private final ColumnType type;
    private final String typeName;
}
