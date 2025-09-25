/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Getter;
import salesforce.cdp.hyperdb.v1.AttachedDatabase;

/**
 * Connection properties that control the JDBC connection behavior.
 */
@Getter
@Builder
public class ConnectionProperties {
    /**
     * Regex pattern to match database path properties in the format "databases.N.path"
     */
    private static final Pattern DATABASE_PATH_PATTERN = Pattern.compile("^databases\\.(\\d+)\\.path$");
    /**
     * The dataspace to use for the connection
     */
    @Builder.Default
    private final String dataspace = "";

    /**
     * The workload to use for the connection (default: jdbcv3)
     */
    @Builder.Default
    private final String workload = "jdbcv3";

    /**
     * The external client context to use for the connection
     */
    @Builder.Default
    private final String externalClientContext = "";

    /**
     * The user name to use for the connection
     */
    @Builder.Default
    private final String userName = "";

    /**
     * Statement properties associated with this connection
     */
    @Builder.Default
    private final StatementProperties statementProperties =
            StatementProperties.builder().build();

    /**
     * The attached databases for this connection (optional)
     */
    @Builder.Default
    private final List<AttachedDatabase> attachedDatabases = new ArrayList<>();

    /**
     * Parses connection properties from a Properties object.
     *
     * @param props The properties to parse
     * @return A ConnectionProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static ConnectionProperties of(Properties props) throws DataCloudJDBCException {
        ConnectionPropertiesBuilder builder = ConnectionProperties.builder();

        String dataspaceValue = props.getProperty("dataspace");
        if (dataspaceValue != null) {
            builder.dataspace(dataspaceValue);
        }
        String workloadValue = props.getProperty("workload");
        if (workloadValue != null) {
            builder.workload(workloadValue);
        }
        String externalClientContextValue = props.getProperty("external-client-context");
        if (externalClientContextValue != null) {
            builder.externalClientContext(externalClientContextValue);
        }
        String userNameValue = props.getProperty("userName");
        if (userNameValue != null) {
            builder.userName(userNameValue);
        }

        // Parse attached databases by iterating over properties to find databases.N.path patterns
        // Use TreeMap to automatically sort by index
        Map<Integer, AttachedDatabase> databaseMap = new TreeMap<>();
        for (String propertyName : props.stringPropertyNames()) {
            Matcher matcher = DATABASE_PATH_PATTERN.matcher(propertyName);
            if (matcher.matches()) {
                int index = Integer.parseInt(matcher.group(1));
                String databasePath = props.getProperty(propertyName);

                if (databasePath != null && !databasePath.trim().isEmpty()) {
                    String databaseAlias = props.getProperty("databases." + index + ".alias");
                    AttachedDatabase.Builder dbBuilder =
                            AttachedDatabase.newBuilder().setPath(databasePath.trim());

                    // Alias is optional - if provided, use it; otherwise leave it empty
                    if (databaseAlias != null && !databaseAlias.trim().isEmpty()) {
                        dbBuilder.setAlias(databaseAlias.trim());
                    }
                    databaseMap.put(index, dbBuilder.build());
                }
            }
        }

        // Validate that indexes are consecutive starting from 0
        if (!databaseMap.isEmpty()) {
            int expectedIndex = 0;
            for (Integer index : databaseMap.keySet()) {
                if (index != expectedIndex) {
                    throw new DataCloudJDBCException(
                            "Database indexes must be consecutive starting from 0. Missing index: " + expectedIndex
                                    + ". Found indexes: " + databaseMap.keySet());
                }
                expectedIndex++;
            }
        }

        List<AttachedDatabase> attachedDatabases = new ArrayList<>(databaseMap.values());
        builder.attachedDatabases(attachedDatabases);

        builder.statementProperties(StatementProperties.of(props));

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the connection properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (!dataspace.isEmpty()) {
            props.setProperty("dataspace", dataspace);
        }
        if (!workload.isEmpty()) {
            props.setProperty("workload", workload);
        }
        if (!externalClientContext.isEmpty()) {
            props.setProperty("external-client-context", externalClientContext);
        }
        if (!userName.isEmpty()) {
            props.setProperty("userName", userName);
        }

        // Serialize attached databases properties - format: "databases.N.path" and "databases.N.alias"
        for (int i = 0; i < attachedDatabases.size(); i++) {
            AttachedDatabase database = attachedDatabases.get(i);
            props.setProperty("databases." + i + ".path", database.getPath());
            // Only set alias if it's provided (not empty)
            if (database.hasAlias() && !database.getAlias().isEmpty()) {
                props.setProperty("databases." + i + ".alias", database.getAlias());
            }
        }

        props.putAll(statementProperties.toProperties());

        return props;
    }
}
