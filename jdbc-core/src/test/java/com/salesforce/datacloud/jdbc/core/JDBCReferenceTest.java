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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.reference.ColumnMetadata;
import com.salesforce.datacloud.reference.ReferenceEntry;
import com.salesforce.datacloud.reference.ValueWithClass;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(HyperTestBase.class)
/**
 * This test case compares our JDBC driver against the behavior of the
 * PostgreSQL JDBC driver. It loads the pre-materialized test expectations
 * from`reference.json` and makes sure our driver behaves the same.
 */
public class JDBCReferenceTest {

    /**
     * Loads baseline entries from the reference.json file.
     *
     * @return Stream of ReferenceEntry objects
     */
    @SneakyThrows
    public static Stream<ReferenceEntry> getBaselineEntries() {
        ObjectMapper objectMapper = new ObjectMapper();

        try (val inputStream = requireNonNull(
                ReferenceEntry.class.getResourceAsStream("/reference.json"),
                "Could not find /reference.json resource")) {
            val referenceEntries = objectMapper.readValue(inputStream, new TypeReference<List<ReferenceEntry>>() {});
            val testableEntries = new ArrayList<ReferenceEntry>();

            for (ReferenceEntry e : referenceEntries) {
                boolean isTestable = true;
                // Patch result metadata expectations
                for (ColumnMetadata c : e.getColumnMetadata()) {
                    // We consciously decided to mark fields as non writeable (like Snowflake) as it aligns more with
                    // the
                    // semantics of a result set.
                    c.setWritable(false);
                    // We consciously decided to mark fields as nullable to represent what the arrow metadata returns
                    if (e.getQuery().contains("NULL")) {
                        c.setIsNullable(ResultSetMetaData.columnNullable);
                    }
                    // These types don't work in V3
                    if (c.getColumnTypeName().endsWith("regconfig")
                            || c.getColumnTypeName().endsWith("regproc")
                            || c.getColumnTypeName().endsWith("regprocedure")
                            || c.getColumnTypeName().endsWith("regclass")
                            || c.getColumnTypeName().endsWith("regoper")
                            || c.getColumnTypeName().endsWith("regoperator")
                            || c.getColumnTypeName().endsWith("regtype")
                            || c.getColumnTypeName().endsWith("regdictionary")) {
                        isTestable = false;
                    }
                    // This type isn't supported by the driver yet
                    if (c.getColumnTypeName().endsWith("interval")) {
                        isTestable = false;
                    }
                    // Timestamps have a bug for timestamp values before the Gregorian epoch.
                    // It is currently unclear if the bug is in Hyper, the JDBC driver or the Java Arrow library
                    else if (c.getColumnTypeName().equals("_timestamptz")
                            || c.getColumnTypeName().equals("_timestamp")) {
                        isTestable = false;
                    }
                    // This type behaves differently between Hyper and Postgres and is not worth it to test
                    else if (c.getColumnTypeName().endsWith("oid")) {
                        isTestable = false;
                    }
                    // The JDBC default is to have uppercase type names
                    c.setColumnTypeName(c.getColumnTypeName().toUpperCase());
                    // Change from Postgres specific type names to JDBC type names
                    if ("INT8".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.BIGINT.getName());
                    } else if ("INT2".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.SMALLINT.getName());
                    } else if ("INT4".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.INTEGER.getName());
                    } else if ("FLOAT8".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.DOUBLE.getName());
                    } else if ("FLOAT4".equals(c.getColumnTypeName())) {
                        assert (c.getColumnType() == JDBCType.REAL.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.REAL.getName());
                    } else if ("TEXT".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.VARCHAR.getName());
                    } else if ("BPCHAR".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.CHAR.getName());
                    } else if ("TIMESTAMPTZ".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.TIMESTAMP_WITH_TIMEZONE.getName());
                    }

                    // Both `Numeric` and `Decimal` can be used, keep using `Decimal` to avoid soft breaking
                    // consumers of older versions
                    if (JDBCType.NUMERIC.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.DECIMAL.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.DECIMAL.getName());
                    }
                    // Same reason for BINARY vs VARBINARY
                    if (JDBCType.BINARY.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.VARBINARY.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.VARBINARY.getName());
                    }
                    // We use boolean instead of bit
                    if (JDBCType.BIT.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.BOOLEAN.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.BOOLEAN.getName());
                    }
                    // We don't have a custom representation for the SQL JSON type
                    if (JDBCType.OTHER.getVendorTypeNumber().equals(c.getColumnType())
                            && "JSON".equals(c.getColumnTypeName())) {
                        c.setColumnType(JDBCType.VARCHAR.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.VARCHAR.getName());
                    }
                    // We don't use custom names for array types
                    if (JDBCType.ARRAY.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnTypeName(JDBCType.ARRAY.getName());
                    }
                }

                // Patch result value expecation
                for (List<ValueWithClass> v : e.getReturnedValues()) {
                    assertEquals(1, v.size(), "The test driver only supports one result value per query");
                    ValueWithClass value = v.get(0);
                    if (e.getQuery().contains("smallint") && (value.getJavaClassName() != null)) {
                        // Avatica doesn't support returning short as Integer (which the standard requires as described
                        // in B-3)
                        value.setJavaClassName(Short.class.getName());
                    } else if ("org.postgresql.util.PGobject".equals(value.getJavaClassName())) {
                        // We return JSON as a String
                        value.setJavaClassName(String.class.getName());
                    } else if (java.sql.Timestamp.class.getName().equals(value.getJavaClassName())) {
                        // We still have several bugs in the driver regarding timestamps and thus we can't compare
                        // against reference values
                        isTestable = false;
                    }
                }
                if (isTestable) {
                    testableEntries.add(e);
                }
            }
            return testableEntries.stream();
        }
    }

    /**
     * Tests DataCloudResultSet metadata and returned values against PostgreSQL baseline expectations.
     * This validates that our JDBC driver produces the same metadata and values as PostgreSQL.
     */
    @ParameterizedTest
    @MethodSource("getBaselineEntries")
    @SneakyThrows
    public void testMetadataAgainstBaseline(ReferenceEntry referenceEntry) {
        Properties properties = new Properties();
        properties.setProperty("timezone", "America/Los_Angeles");
        try (DataCloudConnection conn = getHyperQueryConnection(properties)) {

            val stmt = conn.createStatement();

            try (ResultSet rs = stmt.executeQuery(referenceEntry.getQuery())) {
                // Validate metadata and returned values against the reference entry
                referenceEntry.validateAgainstResultSet(rs, referenceEntry.getQuery());
            } catch (Exception e) {
                System.out.println("Failed to execute query: " + referenceEntry.getQuery());
                System.out.println("Error: " + e.getMessage());
                throw (e);
            }
        }
    }
}
