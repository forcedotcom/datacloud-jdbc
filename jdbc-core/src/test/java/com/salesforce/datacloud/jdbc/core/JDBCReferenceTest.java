/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.reference.ColumnMetadata;
import com.salesforce.datacloud.reference.ReferenceEntry;
import com.salesforce.datacloud.reference.ValueWithClass;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This test case compares our JDBC driver against the behavior of the
 * PostgreSQL JDBC driver. It loads the pre-materialized test expectations
 * from reference.json and makes sure our driver behaves the same.
 *
 * The JVM timezone is pinned to America/Los_Angeles to match the timezone
 * used when generating reference.json via PostgresReferenceGenerator.
 * This ensures Timestamp.toString() produces consistent stringValue output
 * across all environments (local, CI, etc.).
 */
@ExtendWith(LocalHyperTestBase.class)
public class JDBCReferenceTest {

    private static TimeZone originalTimeZone;

    @BeforeAll
    static void setTimezone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @AfterAll
    static void restoreTimezone() {
        TimeZone.setDefault(originalTimeZone);
    }

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
                        // Postgres reports TIMESTAMPTZ as TIMESTAMP (93); our driver correctly
                        // reports TIMESTAMP_WITH_TIMEZONE (2014) per JDBC 4.2
                        c.setColumnType(JDBCType.TIMESTAMP_WITH_TIMEZONE.getVendorTypeNumber());
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

                // Patch result value expectation
                for (List<ValueWithClass> v : e.getReturnedValues()) {
                    assertEquals(1, v.size(), "The test driver only supports one result value per query");
                    ValueWithClass value = v.get(0);
                    if (e.getQuery().contains("smallint") && (value.getJavaClassName() != null)) {
                        // JDBC spec table B-3 requires SMALLINT to return Integer
                        value.setJavaClassName(Integer.class.getName());
                    } else if ("org.postgresql.util.PGobject".equals(value.getJavaClassName())) {
                        // We return JSON as a String
                        value.setJavaClassName(String.class.getName());
                    }
                }
                if (isTestable) {
                    testableEntries.add(e);
                }
            }
            return testableEntries.stream();
        }
    }

    @ParameterizedTest
    @MethodSource("getBaselineEntries")
    @SneakyThrows
    public void testMetadataAgainstBaseline(ReferenceEntry referenceEntry) {
        Properties properties = new Properties();
        properties.setProperty("querySetting.time_zone", "America/Los_Angeles");
        try (DataCloudConnection conn = getHyperQueryConnection(properties)) {

            val stmt = conn.createStatement();

            try (ResultSet rs = stmt.executeQuery(referenceEntry.getQuery())) {
                referenceEntry.validateAgainstResultSet(rs, referenceEntry.getQuery());
            } catch (Exception e) {
                System.out.println("Failed to execute query: " + referenceEntry.getQuery());
                System.out.println("Error: " + e.getMessage());
                throw (e);
            }
        }
    }
}
