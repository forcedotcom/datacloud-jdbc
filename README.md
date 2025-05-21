# Salesforce DataCloud JDBC Driver

With the Salesforce Data Cloud JDBC driver you can efficiently query millions of rows of data with low latency, and perform bulk data extractions.
This driver is read-only, forward-only, and requires Java 8 or greater. It uses the new [Data Cloud Query API SQL syntax](https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/dc-sql-reference/data-cloud-sql-context.html).

## Example usage

We have a suite of tests that demonstrate preferred usage patterns when using APIs that are outside of the JDBC specification.
Please check out the [examples here](jdbc-core/src/test/java/com/salesforce/datacloud/jdbc/examples).

## Getting started

To add the driver to your project, add the following Maven dependency:

```xml
<dependency>
    <groupId>com.salesforce.datacloud</groupId>
    <artifactId>jdbc</artifactId>
    <version>${jdbc.version}</version>
</dependency>
```

The class name for this driver is:

```
com.salesforce.datacloud.jdbc.DataCloudJDBCDriver
```

## Building the driver:

Use the following command to build and test the driver:

```shell
./gradlew clean build
```

To inspect the jars that will be published run and take a look in `build/maven-repo`:

```shell
./gradlew build publishAllPublicationsToRootBuildDirRepository
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
| coreToken    | OAuth token that a connected app uses to request access to a protected resource on behalf of the client application. |
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
properties.put("privateKey", "${privateKey}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

#### refresh token authentication:

The documentation for refresh token authentication can be found [here][refresh token flow].

```java
Properties properties = new Properties();
properties.put("coreToken", "${coreToken}");
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

### Optional configuration

- `dataspace`: The data space to query, defaults to "default"
- `User-Agent`: The User-Agent string identifies the JDBC driver and, optionally, the client application making the database connection. <br />
  By default, the User-Agent string will end with "salesforce-datacloud-jdbc/{version}" and we will prepend any User-Agent provided by the client application. <br />
  For example: "User-Agent: ClientApp/1.2.3 salesforce-datacloud-jdbc/1.0"


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

## Generated assertions

Some of our classes are tested using assertions generated with [the assertj assertions generator][assertion generator].
Due to some transient test-compile issues we experienced, we checked in generated assertions for some of our classes.
If you make changes to any of these classes, you will need to re-run the assertion generator to have the appropriate assertions available for that class.

To find examples of these generated assertions, look for files with the path `**/test/**/*Assert.java`.

To re-generate these assertions execute the following command:

```shell
mvn assertj:generate-assertions
```


[oauth authorization flows]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5
[username flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5
[jwt flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
[refresh token flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm&type=5
[connection settings]: https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
[assertion generator]: https://joel-costigliola.github.io/assertj/assertj-assertions-generator-maven-plugin.html#configuration
[connected app overview]: https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5
