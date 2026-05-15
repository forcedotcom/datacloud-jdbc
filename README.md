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
> 3. The public methods in DataCloudConnection, DataCloudStatement, DataCloudResultSet, and DataCloudPreparedStatement -- note that these will be refactored to be interfaces that will make the API more obvious in the near future
>
> Usage of any other public classes or methods not listed above should be considered relatively unsafe, though we will strive to not make changes and will use semantic versioning from 1.0.0 and on.

### Connection string

Use `jdbc:salesforce-datacloud://login.salesforce.com`

### JDBC Driver class

Use `com.salesforce.datacloud.jdbc.DataCloudJDBCDriver` as the driver class name for the JDBC application.

### Authentication

We support four of the [OAuth authorization flows][oauth authorization flows] provided by Salesforce.
All of these flows require a connected app or external client app be configured for the driver to authenticate as, see the documentation here: [connected app overview][connected app overview].
Set the following properties appropriately to establish a connection with your chosen OAuth authorization flow:

| Parameter                  | Description                                                                                                          |
|----------------------------|----------------------------------------------------------------------------------------------------------------------|
| user                       | The login name of the user.                                                                                          |
| password                   | The password of the user.                                                                                            |
| clientId                   | The consumer key of the connected app.                                                                               |
| clientSecret               | The consumer secret of the connected app.                                                                            |
| privateKey                 | The private key of the connected app.                                                                                |
| refreshToken               | Token obtained from the web server, user-agent, or hybrid app token flow.                                            |
| authMode                   | Set to `AUTH_CODE_PKCE` to trigger the interactive browser-based authorization-code flow with PKCE.                  |
| oauthScope                 | (Optional, AUTH_CODE_PKCE only) Space-separated OAuth scopes; defaults to `cdp_query_api api refresh_token`.         |
| redirectPort               | (Optional, AUTH_CODE_PKCE only) Loopback port for the OAuth callback; default `0` (pick a free ephemeral port).      |
| browserAuthTimeoutSeconds  | (Optional, AUTH_CODE_PKCE only) How long to wait for the browser dance to complete; default `120`.                   |


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

#### interactive browser authentication (authorization code with PKCE):

The driver can drive the [OAuth web server flow][web server flow] for you: it
opens the user's default browser at the Salesforce login screen, listens on a
loopback HTTP port for the redirect callback, and exchanges the resulting
authorization code for an access token using
[Proof Key for Code Exchange (RFC 7636)][rfc7636]. No long-lived secret needs
to be present in the configuration — the per-login PKCE verifier replaces a
`client_secret` for public clients.

```java
Properties properties = new Properties();
properties.put("authMode", "AUTH_CODE_PKCE");
properties.put("clientId", "${clientId}");
properties.put("redirectPort", "7171"); // must match the ECA's registered Callback URL
// Optional: properties.put("clientSecret", "${clientSecret}"); // for confidential ECAs
```

**Pin the port.** Salesforce External Client Apps match the redirect URI as an
exact string — wildcard ports are *not* supported. Pick a fixed `redirectPort`
(e.g. `7171`) and register `http://127.0.0.1:7171/callback` in the ECA's
Callback URL field. If you don't pin the port, the driver picks a free
ephemeral one, which won't match anything the ECA has registered and
authentication will fail with `redirect_uri_mismatch`.

Headless environments (servers without a display) are supported as a
fallback: the driver prints the authorization URL to stderr so a human can
paste it into a browser manually.

##### Setting up the External Client App for this flow

The ECA needs the following settings. The fastest path is the **Setup UI**;
an automated path using anonymous Apex is described further below.

| Setting | Value | Why |
| --- | --- | --- |
| **Enable OAuth** | checked | gates everything below |
| **Callback URL** | `http://127.0.0.1:7171/callback` (one line per port if multiple) | matched as an exact string by Salesforce |
| **OAuth Scopes** | `cdp_query_api`, `api`, `refresh_token` | Data Cloud Query API plus token refresh |
| **Require Proof Key for Code Exchange (PKCE) Extension for Supported Authorization Flows** | checked | rejects code-exchange requests that don't carry a verifier |
| **Require Secret for Web Server Flow** | unchecked | makes the ECA a *public client* — the driver doesn't ship a `client_secret` |
| **Require Secret for Refresh Token Flow** | unchecked | same reason as above |

In the Setup UI: **Setup → External Client Apps → New External Client App**,
fill in Name / Contact Email / Distribution State, check **Enable OAuth**,
then set the fields above. Save and copy the generated **Consumer Key** —
that's the value you pass as `clientId` to the driver.

##### Headless / scripted ECA creation

There is no `sf eca create` CLI command. The realistic automated path is:

1. Author SFDX-source XML for the three metadata types
   ([`ExternalClientApplication`][eca-meta], [`ExtlClntAppOauthSettings`][eca-oauth-meta],
   [`ExtlClntAppGlobalOauthSettings`][eca-global-oauth-meta]).
2. Deploy the `ExternalClientApplication` header with `sf project deploy start`.
3. Run anonymous Apex that calls `Metadata.Operations.enqueueDeployment` to
   create the OAuth and Global-OAuth records — these two types cannot be
   round-tripped by `sf project deploy` alone today. The
   [flxbl-io ECA guide][flxbl-eca-guide] is the canonical reference and
   includes a working `bootstrap-eca-oauth.apex` script you can adapt.

Minimal `force-app/main/default/externalClientApps/DataCloudJDBC.eca-meta.xml`:

```xml
<ExternalClientApplication xmlns="http://soap.sforce.com/2006/04/metadata">
    <contactEmail>admin@example.com</contactEmail>
    <distributionState>Local</distributionState>
    <isProtected>false</isProtected>
    <label>DataCloudJDBC</label>
</ExternalClientApplication>
```

The OAuth/global-OAuth records the bootstrap Apex creates correspond to:

```xml
<!-- DataCloudJDBC_oauth.ecaOauth-meta.xml -->
<ExtlClntAppOauthSettings xmlns="http://soap.sforce.com/2006/04/metadata">
    <commaSeparatedOauthScopes>CdpQueryApi, Api, RefreshToken</commaSeparatedOauthScopes>
    <externalClientApplication>DataCloudJDBC</externalClientApplication>
    <label>DataCloudJDBC_oauth</label>
</ExtlClntAppOauthSettings>

<!-- DataCloudJDBC_glbloauth.ecaGlblOauth-meta.xml -->
<ExtlClntAppGlobalOauthSettings xmlns="http://soap.sforce.com/2006/04/metadata">
    <callbackUrl>http://127.0.0.1:7171/callback</callbackUrl>
    <externalClientApplication>DataCloudJDBC</externalClientApplication>
    <isConsumerSecretOptional>true</isConsumerSecretOptional>
    <isPkceRequired>true</isPkceRequired>
    <isSecretRequiredForRefreshToken>false</isSecretRequiredForRefreshToken>
    <label>DataCloudJDBC_glbloauth</label>
</ExtlClntAppGlobalOauthSettings>
```

`isConsumerSecretOptional=true` is the public-client toggle and
`isPkceRequired=true` is the matching server-side enforcement.

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
[web server flow]: https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_web_server_flow.htm&type=5
[rfc7636]: https://datatracker.ietf.org/doc/html/rfc7636
[eca-meta]: https://developer.salesforce.com/docs/atlas.en-us.api_meta.meta/api_meta/meta_externalclientapplication.htm
[eca-oauth-meta]: https://developer.salesforce.com/docs/atlas.en-us.api_meta.meta/api_meta/meta_extlclntappoauthsettings.htm
[eca-global-oauth-meta]: https://developer.salesforce.com/docs/atlas.en-us.api_meta.meta/api_meta/meta_extlclntappglobaloauthsettings.htm
[flxbl-eca-guide]: https://github.com/flxbl-io/external-client-apps-guide
[connection settings]: https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
[connected app overview]: https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm&type=5
