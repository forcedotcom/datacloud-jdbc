/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

/**
 * Represents a field / column in a result set. The {@link HyperType} carries the full internal
 * type model; {@link java.sql.ResultSetMetaData#getColumnTypeName(int)} is derived from it.
 */
@lombok.Value
public class ColumnMetadata {
    String name;
    HyperType type;
}
