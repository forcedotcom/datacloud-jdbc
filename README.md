# Salesforce DataCloud JDBC Driver

[![codecov](https://codecov.io/github/forcedotcom/datacloud-jdbc/graph/badge.svg?token=FNEAWV3I42)](https://codecov.io/github/forcedotcom/datacloud-jdbc)

With the Salesforce Data Cloud JDBC driver you can efficiently query millions of rows of data with low latency, and perform bulk data extractions.
This driver is read-only, forward-only, and requires Java 8 or greater. It uses the new [Data Cloud Query API SQL syntax](https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/dc-sql-reference/data-cloud-sql-context.html).

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
> 3. The public methods in DataCloudConnection, DataCloudStatement, DataCloudResultSet, and DataCloudPreparedStatement -- note that these will be refactored to be interfaces that will make the API more obvious in the near future
>
> Usage of any other public classes or methods not listed above should be considered relatively unsafe, though we will strive to not make changes and will use semantic versioning from 1.0.0 and on.

### Connection string

Use `jdbc:salesforce-datacloud://login.salesforce.com`

### JDBC Driver class

Use `com.salesforce.datacloud.jdbc.DataCloudJDBCDriver` as the driver class name for the JDBC application.

### Authentication

We support three of the [OAuth authorization flows][oauth authorization flows] provided by Salesforce.
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
properties.put("clientSecret", "${clientSecret}");
```

#### refresh token authentication:

The documentation for refresh token authentication can be found [here][refresh token flow].

```java
Properties properties = new Properties();
properties.put("refreshToken", "${refreshToken}");
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

### Optional configuration

- `dataspace`: The data space to query, defaults to "default"

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
[connection settings]: https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
[connected app overview]: https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5
