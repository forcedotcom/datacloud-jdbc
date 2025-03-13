# Salesforce Data Cloud JDBC Driver

This driver allows you to efficiently query large datasets in Salesforce Data Cloud with low latency and perform bulk data extractions. It's read-only, forward-only, and requires Java 8 or later. The driver uses the Data Cloud Query API SQL syntax (see [Data Cloud Query API SQL syntax](https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/dc-sql-reference/data-cloud-sql-context.html) for details).

## Getting Started

### Maven Dependency

To use the driver in your Maven project, add the following to your `pom.xml`:

```xml
<dependency>
    <groupId>com.salesforce.datacloud</groupId>
    <artifactId>jdbc</artifactId>
    <version>${jdbc.version}</version>
</dependency>
```

Replace `${jdbc.version}` with the actual version of the driver. It's good practice to manage this version in a properties section of your pom.xml.

### Driver Class

The driver class name is: `com.salesforce.datacloud.jdbc.DataCloudJDBCDriver`

## Building the Driver

To build and test the driver, run:

```bash
mvn clean install
```

## Usage

### Connection String

The connection string format is: `jdbc:salesforce-datacloud://login.salesforce.com`

### Driver Class (Reiterated)

Use `com.salesforce.datacloud.jdbc.DataCloudJDBCDriver` when loading the driver in your JDBC application. For example:

```java
Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
```

### Authentication

The driver supports several OAuth authorization flows provided by Salesforce (see [OAuth authorization flows](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5)). You'll need to configure a connected app (see [Connected App Overview](https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5)).

The following table outlines the properties for each flow:

| Property       | Description                    | Authentication Flow              |
| -------------- | ------------------------------ | -------------------------------- |
| `user`         | Salesforce username.           | Username/Password                |
| `password`     | Salesforce password.           | Username/Password                |
| `clientId`     | Connected app consumer key.    | All                              |
| `clientSecret` | Connected app consumer secret. | Username/Password, Refresh Token |
| `privateKey`   | Connected app private key.     | JWT                              |
| `coreToken`    | OAuth token.                   | Refresh Token                    |
| `refreshToken` | Refresh token.                 | Refresh Token                    |

#### Username/Password Authentication

See [Username/Password Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5) for more information. Example:

```java
Properties properties = new Properties();
properties.put("user", "${userName}");
properties.put("password", "${password}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

#### JWT Authentication

See [JWT Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5). For generating a private key, see [Generating a Private Key](#generating-a-private-key-for-jwt-authentication). Example:

```java
Properties properties = new Properties();
properties.put("privateKey", "${privateKey}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

#### Refresh Token Authentication

See [Refresh Token Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm&type=5). Example:

```java
Properties properties = new Properties();
properties.put("coreToken", "${coreToken}");
properties.put("refreshToken", "${refreshToken}");
properties.put("clientId", "${clientId}");
properties.put("clientSecret", "${clientSecret}");
```

### Connection Settings

Additional connection settings are available (see [Connection Settings](https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings)). Prefix properties with `querySetting.`. For example, to set the locale:

```java
properties.put("querySetting.lc_time", "en_US");
```

## Generating a Private Key for JWT Authentication <a name="generating-a-private-key-for-jwt-authentication"></a>

For JWT authentication, you need a certificate registered with your connected app.

```bash
# Create a key pair:
openssl genrsa -out keypair.key 2048

# Create a digital certificate:
openssl req -new -x509 -nodes -sha256 -days 365 -key keypair.key -out certificate.crt

# Create the private key:
openssl pkcs8 -topk8 -nocrypt -in keypair.key -out private.key
```

## Optional Configuration

- `dataspace`: The data space to query (defaults to "default").
- `User-Agent`: Identifies the JDBC driver and client application. Defaults to `salesforce-datacloud-jdbc/{version}`. You can prepend your application's User-Agent string. Example: `User-Agent: ClientApp/1.2.3 salesforce-datacloud-jdbc/1.0`

## Usage Example

```java
import java.sql.*;
import java.util.Properties;

public class DataCloudQuery {

    public static void executeQuery() throws ClassNotFoundException, SQLException {
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");

        Properties properties = new Properties();
        properties.put("user", "${userName}");
        properties.put("password", "${password}");
        properties.put("clientId", "${clientId}");
        properties.put("clientSecret", "${clientSecret}");

        try (Connection connection = DriverManager.getConnection("jdbc:salesforce-datacloud://login.salesforce.com", properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("${query}")) {

            while (resultSet.next()) {
                // Process the result set. Example:
                System.out.println(resultSet.getString(1)); // Assuming a string in the first column
            }
        }
    }
}
```

Remember to replace placeholders like `${userName}` and `${query}` with your actual values. Consider using parameterized queries to prevent SQL injection vulnerabilities.

## Generated Assertions

Some classes use generated assertions (from [AssertJ Assertions Generator](https://joel-costigliola.github.io/assertj/assertj-assertions-generator-maven-plugin.html#configuration)). If you modify these classes, regenerate the assertions:

```bash
mvn assertj:generate-assertions
```

Find examples in `**/test/**/*Assert.java`.

[oauth authorization flows]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5
[username flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5
[jwt flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
[refresh token flow]: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_refresh_token_flow.htm&type=5
[connection settings]: https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
[assertion generator]: https://joel-costigliola.github.io/assertj/assertj-assertions-generator-maven-plugin.html#configuration
[connected app overview]: https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5
