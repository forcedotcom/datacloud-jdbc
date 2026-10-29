# Salesforce Data 360 JDBC Driver

[![codecov](https://codecov.io/github/forcedotcom/datacloud-jdbc/graph/badge.svg?token=FNEAWV3I42)](https://codecov.io/github/forcedotcom/datacloud-jdbc)

With the Salesforce Data 360 JDBC driver you can efficiently query millions of rows of data with low latency, and perform bulk data extractions.
This driver is read-only, forward-only, and requires Java 8 or greater. It uses the new [Data 360 Query API SQL syntax](https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/dc-sql-reference/data-cloud-sql-context.html).

Download the jar here: [![Maven Central Version](https://img.shields.io/maven-central/v/com.salesforce.datacloud/jdbc)](https://repo1.maven.org/maven2/com/salesforce/datacloud/jdbc/)

## Example usage

We have a suite of tests that demonstrate preferred usage patterns when using APIs that are outside of the JDBC specification.
Please check out the [examples here](jdbc-core/src/test/java/com/salesforce/datacloud/jdbc/examples).

## Getting started

Most applications should use the shaded variant to avoid dependency conflicts:

```xml
<dependency>
    <groupId>com.salesforce.datacloud</groupId>
    <artifactId>jdbc</artifactId>
    <version>${jdbc.version}</version>
    <classifier>shaded</classifier>
</dependency>
```

If you need to manage gRPC and protos dependencies directly, use the standard JAR:

```xml
<dependency>
    <groupId>com.salesforce.datacloud</groupId>
    <artifactId>jdbc</artifactId>
    <version>${jdbc.version}</version>
</dependency>
```

Note: The default JAR includes generated protos compiled against specific gRPC versions. Applications using different gRPC versions may experience compatibility issues. Please use `jdbc-core` and your own proto generation.

The class name for this driver is:

```
com.salesforce.datacloud.jdbc.DataCloudJDBCDriver
```

## Usage

> [!INFO]
> Our API is versioned based on semantic versioning rules around our supported API.
> This supported API includes:
> 1. Any construct available through the JDBC specification we have implemented
> 2. The DataCloudQueryStatus class
> 3. The public methods in DataCloudConnection, DataCloudStatement, DataCloudResultSet, DataCloudPreparedStatement, and DataCloudNamedParameterStatement -- note that these will be refactored to be interfaces that will make the API more obvious in the near future
>
> Usage of any other public classes or methods not listed above should be considered relatively unsafe, though we will strive to not make changes and will use semantic versioning from 1.0.0 and on.

### Connection string

Use `jdbc:salesforce-datacloud://login.salesforce.com`

### JDBC Driver class

Use `com.salesforce.datacloud.jdbc.DataCloudJDBCDriver` as the driver class name for the JDBC application.

### Authentication

We support four of the [OAuth authorization flows][oauth authorization flows] provided by Salesforce.
All of these flows require a connected app be configured for the driver to authenticate as, see the documentation here: [connected app overview][connected app overview].
Set the following properties appropriately to establish a connection with your chosen OAuth authorization flow:

| Parameter    | Description                                                                                                          |
|--------------|----------------------------------------------------------------------------------------------------------------------|
| user         | The login name of the user.                                                                                          |
| password     | The password of the user.                                                                                            |
| clientId     | The consumer key of the connected app.                                                                               |
| clientSecret | The consumer secret of the connected app.                                                                            |
| privateKey   | The private key of the connected app.                                                                                |
| refreshToken | Token obtained from the web server, user-agent, or hybrid app token flow.                                            |


#### username and password authentication:

The documentation for username and password authentication can be found [here][username flow].

To configure username and password, set properties like so:

```java
Properties properties = new Properties();
properties.put("user", "${userName}");
properties.put("password", "${password}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

#### jwt authentication:

The documentation for jwt authentication can be found [here][jwt flow].

Instructions to generate a private key can be found [here](#generating-a-private-key-for-jwt-authentication)

```java
Properties properties = new Properties();
properties.put("user", "${userName}");
properties.put("privateKey", "${privateKey}");
properties.put("clientId", "${clientId}");
```

#### refresh token authentication:

The documentation for refresh token authentication can be found [here][refresh token flow].

```java
Properties properties = new Properties();
properties.put("refreshToken", "${refreshToken}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

#### client credentials authentication:

The documentation for client credentials authentication can be found [here][client credentials flow].

This flow authenticates as the connected app's run-as user with no interactive user context, so set only `clientId` and `clientSecret`:

```java
Properties properties = new Properties();
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

### Connection settings

See this page on available [connection settings][connection settings].
These settings can be configured in properties by using the prefix `querySetting.`

For example, to control locale set the following property:

```java
properties.put("querySetting.lc_time", "en_US");
```

---

### Generating a private key for jwt authentication

To authenticate using key-pair authentication you'll need to generate a certificate and register it with your connected app.

```shell
# create a key pair:
openssl genrsa -out keypair.key 2048
# create a digital certificate, follow the prompts:
openssl req -new -x509 -nodes -sha256 -days 365 -key keypair.key -out certificate.crt
# create a private key from the key pair:
openssl pkcs8 -topk8 -nocrypt -in keypair.key -out private.key
```

### JDBC Details
This section describes details around potential pitfalls / ambiguities related to JDBC
- The standard offers two types for fixed point decimals (`NUMERIC` and `DECIMAL`), this driver uses `DECIMAL` to represent such values
- The JDBC standard describes that `getObject` for a `SHORT` should return an `Integer`. Due to a current limitation we for now return a `Short` object. This will likely be fixed in a future version of the JDBC driver.
- The query timeout enforcement is done on the server side for both normal as well as async execution. To provide a safety net with regards to network problems the driver locally also does a delayed enforcement for normal query executions. The default delay is `5` seconds - which typically shouldn't be relevant as in normal circumstances the server will enforce the timeout. The local enforcement delay can be configured through the `queryTimeoutLocalEnforcementDelay` property

### Named parameters

`DataCloudConnection` supports Hyper's native `:name` parameters through `prepareNamedParameterStatement`:

```java
try (DataCloudNamedParameterStatement statement =
        connection.prepareNamedParameterStatement("SELECT :minimum + :increment")) {
    statement.setInt("minimum", 40);
    statement.setInt("increment", 2);

    try (ResultSet result = statement.executeQuery()) {
        result.next();
        int value = result.getInt(1); // 42
    }
}
```

Use `Connection.prepareStatement` for standard JDBC positional `?` parameters.

### Data type mapping

How [Data 360 SQL data types](https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/dc-sql-reference/datatypes.html) are presented over JDBC (`ResultSetMetaData.getColumnType` / `getColumnClassName`, `DatabaseMetaData.getColumns`).

| Data 360 SQL type (aliases)                         | `java.sql.Types`            | `getObject` Java class | Notes |
|-----------------------------------------------------|-----------------------------|------------------------|-------|
| `BOOLEAN` (`BOOL`)                                  | `BOOLEAN`                   | `Boolean`              | |
| `SMALLINT` (`INT2`)                                 | `SMALLINT`                  | `Integer`              | |
| `INTEGER` (`INT`, `INT4`)                           | `INTEGER`                   | `Integer`              | |
| `BIGINT` (`INT8`)                                   | `BIGINT`                    | `Long`                 | |
| `REAL` (`FLOAT4`)                                   | `REAL`                      | `Float`                | |
| `DOUBLE PRECISION` (`FLOAT`, `FLOAT8`)              | `DOUBLE`                    | `Double`               | |
| `NUMERIC(p, s)` (`DECIMAL(p, s)`)                   | `DECIMAL`                   | `BigDecimal`           | `NUMERIC` is reported as `DECIMAL` |
| `CHAR(n)` (`CHARACTER(n)`)                          | `CHAR`                      | `String`               | |
| `VARCHAR(n)` (`CHARACTER VARYING(n)`)               | `VARCHAR`                   | `String`               | |
| `TEXT`                                              | `VARCHAR`                   | `String`               | Unbounded `VARCHAR` |
| `BYTES` (`bytea`)                                   | `VARBINARY`                 | `byte[]`               | |
| `DATE`                                              | `DATE`                      | `java.sql.Date`        | |
| `TIME [WITHOUT TIME ZONE]`                          | `TIME`                      | `java.sql.Time`        | Microsecond precision |
| `TIMESTAMP [WITHOUT TIME ZONE]`                     | `TIMESTAMP`                 | `java.sql.Timestamp`   | See [Timestamp handling](#timestamp-handling) |
| `TIMESTAMP WITH TIME ZONE` (`TIMESTAMPTZ`)          | `TIMESTAMP_WITH_TIMEZONE`   | `java.sql.Timestamp`   | See [Timestamp handling](#timestamp-handling) |
| `ARRAY` of any of the above                         | `ARRAY`                     | `java.sql.Array`       | |
| `INTERVAL`                                          | n/a                         | n/a                    | Not supported in result sets: reading an `INTERVAL` column fails with `Unsupported Arrow type: Interval(MONTH_DAY_NANO)` |
| `JSON`                                              | `VARCHAR`                   | `String`               | Data 360 returns JSON values as text |

`JSON` has no dedicated JDBC representation: Data 360 returns it as text, so it is reported as `VARCHAR`.

### Timestamp handling

The driver follows the JDBC 4.2 specification for timestamp types. Use the `java.time` API for unambiguous behavior:

> **Rule of thumb:** if the value has a timezone, use `OffsetDateTime`; if it doesn't, use `LocalDateTime`.

#### Storing a wall-clock time (`TIMESTAMP` — no timezone)

Use this when you want to store a literal date and time with no timezone context (e.g. "meeting at 2pm").

```java
// Write — digits stored as-is, no timezone shift applied
pstmt.setObject(1, LocalDateTime.of(2024, 6, 15, 14, 30, 0));

// Read
LocalDateTime ldt = rs.getObject("col", LocalDateTime.class);
```

#### Storing an exact moment in time (`TIMESTAMPTZ` — with timezone)

Use this when you want to preserve a specific instant in UTC (e.g. "event occurred at 21:30:45 UTC").

```java
// Write — UTC epoch preserved exactly
pstmt.setObject(1, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));

// Read
OffsetDateTime odt = rs.getObject("col", OffsetDateTime.class);
```

#### Legacy `java.sql.Timestamp`

`setTimestamp(ts)` and `setObject(ts)` store the wall-clock value in the JVM default timezone as a naive `TIMESTAMP`. This is correct for wall-clock use cases but should not be used for `TIMESTAMPTZ` columns where preserving the UTC instant matters.

| Use case | Recommended write | Recommended read |
|----------|------------------|-----------------|
| Wall-clock (`TIMESTAMP`) | `setObject(LocalDateTime)` | `getObject(LocalDateTime.class)` |
| UTC instant (`TIMESTAMPTZ`) | `setObject(OffsetDateTime)` | `getObject(OffsetDateTime.class)` |
| Legacy (`TIMESTAMP`) | `setTimestamp(ts)` | `getTimestamp()` |

### Optional configuration

- `dataspace`: The data space to query, defaults to "default"

### Using the driver in DBeaver

DBeaver ships this driver as **Salesforce Data 360** (called **Salesforce Data Cloud** in older versions) and downloads it from Maven Central for you.
Follow the [Salesforce guide for DBeaver][dbeaver guide], with these notes:

- The driver does not advertise its properties, so the **Driver properties** tab starts empty and has no `clientId` or `clientSecret` fields.
  Right-click under **User Properties**, choose **Add new property**, then click the **Value** cell and enter the value. A property without a value is not sent, and the connection fails with `Property clientId is missing`.
- Client credentials authentication works in DBeaver too: add only `clientId` and `clientSecret`, and set the host to your My Domain host (for example `mydomain.my.salesforce.com`, without `https://`), because Salesforce does not support this flow on `login.salesforce.com`.
- `information_schema` is not available. The database navigator lists tables through `pg_catalog`, and you can query it directly:

  ```sql
  SELECT n.nspname AS table_schema, c.relname AS table_name
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'v') AND n.nspname !~ '^pg_'
  ORDER BY 1, 2
  ```

- DBeaver keeps user property values, including `clientSecret` and `privateKey`, in plain text in its `data-sources.json`. Do not commit or share that file.

### Usage sample code

```java
public static void executeQuery() throws ClassNotFoundException, SQLException {
    Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");

    Properties properties = new Properties();
    properties.put("user", "${userName}");
    properties.put("password", "${password}");
    properties.put("clientId", "${clientId}");
    properties.put("clientSecret", "${clientSecret}");

    try (var connection = DriverManager.getConnection("jdbc:salesforce-datacloud://login.salesforce.com", properties);
         var statement = connection.createStatement()) {
        var resultSet = statement.executeQuery("${query}");

        while (resultSet.next()) {
            // Iterate over the result set
        }
    }
}
```

[oauth authorization flows]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5
[username flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5
[jwt flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
[refresh token flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm&type=5
[client credentials flow]: https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_client_credentials_flow.htm&type=5
[connection settings]: https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
[connected app overview]: https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5
[dbeaver guide]: https://developer.salesforce.com/docs/data/data-cloud-query-guide/guide/jdbc-dbeaver.html
