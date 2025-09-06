/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.config;

public final class QueryResources {
    public static String getColumnsQueryText() {
        return loadQuery("get_columns_query");
    }

    public static String getSchemasQueryText() {
        return loadQuery("get_schemas_query");
    }

    public static String getTablesQueryText() {
        return loadQuery("get_tables_query");
    }

    private QueryResources() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static String loadQuery(String name) {
        return ResourceReader.readResourceAsString("/sql/" + name + ".sql");
    }
}
