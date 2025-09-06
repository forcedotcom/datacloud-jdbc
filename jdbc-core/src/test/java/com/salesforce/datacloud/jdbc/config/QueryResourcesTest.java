/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.config;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.val;
import org.junit.jupiter.api.Test;

class QueryResourcesTest {

    @Test
    void getColumnsQuery() {
        val actual = QueryResources.getColumnsQueryText();
        assertThat(actual)
                .contains("SELECT n.nspname,")
                .contains("FROM pg_catalog.pg_namespace n")
                .contains("WHERE c.relkind in ('r', 'p', 'v', 'f', 'm')");
    }

    @Test
    void getSchemasQuery() {
        val actual = QueryResources.getSchemasQueryText();
        assertThat(actual)
                .contains("SELECT nspname")
                .contains("FROM pg_catalog.pg_namespace")
                .contains("WHERE nspname");
    }

    @Test
    void getTablesQuery() {
        val actual = QueryResources.getTablesQueryText();
        assertThat(actual)
                .contains("SELECT")
                .contains("FROM pg_catalog.pg_namespace")
                .contains("LEFT JOIN pg_catalog.pg_description d ON");
    }
}
