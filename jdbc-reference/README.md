# JDBC Reference Generator

This tool generates PostgreSQL JDBC metadata reference files by executing data type queries and capturing column metadata and result values. It's designed to create baseline reference data for testing JDBC driver consistency when comparing to the Postgres JDBC driver.

## Quick Start: Updating the Reference JSON

To regenerate the `reference.json` file with the latest PostgreSQL JDBC metadata & values:

1. **Ensure PostgreSQL is running** with the test database:
   ```bash
   # Default connection: localhost:5432/testdb with user testuser/password
   psql -h localhost -p 5432 -U testuser -d testdb -c "SELECT version();"
   ```

2. **Run the reference generator**:
   ```bash
   ./gradlew :jdbc-reference:run
   ```

3. **The updated `reference.json`** will be written to `jdbc-reference/src/main/resources/reference.json`

That's it! The tool automatically loads test cases from `protocolvalues.json` and generates fresh reference values.

## What This Tool Does

The JDBC Reference Generator:

1. **Loads test data** from `src/main/resources/protocolvalues.json` - a comprehensive set of PostgreSQL data type test cases
3. **Executes SQL queries** against a local PostgreSQL database using JDBC
4. **Captures column metadata & values** - extracts detailed JDBC metadata for each result column and also captures the default result object types and values
5. **Writes reference file** - saves the metadata as JSON to `src/main/resources/reference.json`

The generated reference data can be used for:
- **JDBC driver testing** - validate metadata consistency across driver versions
- **Regression testing** - ensure metadata doesn't change unexpectedly
- **Cross-database validation** - compare metadata behavior between databases

## Generated Reference Data Structure

The `reference.json` file contains an array of reference entries. Each entry includes:

```json
{
  "query" : "select smallint '1'",
  "columnMetadata" : [ {
    "columnName" : "int2",
    "columnLabel" : "int2",
    "columnType" : 5,
    "columnTypeName" : "int2",
    "columnDisplaySize" : 6,
    "precision" : 5,
    "scale" : 0,
    "isNullable" : 2,
    "catalogName" : "",
    "schemaName" : "",
    "tableName" : "",
    "autoIncrement" : false,
    "caseSensitive" : false,
    "currency" : false,
    "definitelyWritable" : false,
    "readOnly" : false,
    "searchable" : true,
    "signed" : true,
    "writable" : true
  } ],
  "returnedValues" : [ [ {
    "stringValue" : "1",
    "javaClassName" : "java.lang.Integer"
  } ] ]
}
```

## Prerequisites

### PostgreSQL Setup

You need a local PostgreSQL server with:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `testdb`
- **Username**: `testuser`
- **Password**: `password`

### Quick PostgreSQL Setup

```bash
# Create the test database and user
sudo -u postgres psql << EOF
CREATE DATABASE testdb;
CREATE USER testuser WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE testdb TO testuser;
EOF

# Test the connection
psql -h localhost -p 5432 -U testuser -d testdb -c "SELECT current_timestamp;"
```

## Configuration

Database connection settings are in `PostgresReferenceGenerator.java`:

```java
public static final String DB_URL = "jdbc:postgresql://localhost:5432/testdb";
public static final String DB_USER = "testuser";
public static final String DB_PASSWORD = "password";
```

Modify these constants to match your PostgreSQL setup.

## Project Structure

```
jdbc-reference/
├── src/main/java/com/salesforce/datacloud/reference/
│   ├── PostgresReferenceGenerator.java  # Main application
│   ├── ReferenceEntry.java              # Data structure for query + metadata + values
│   ├── ColumnMetadata.java              # JDBC column metadata extraction
│   ├── ProtocolValue.java               # Test case data structure
│   └── TypeInfo.java                    # Type information classes
├── src/main/resources/
│   ├── protocolvalues.json              # Test cases (input)
│   └── reference.json                   # Generated reference data (output)
└── build.gradle.kts
```

### Key Classes

- **`PostgresReferenceGenerator`** - Main class that orchestrates the reference generation
- **`ProtocolValue`** - Represents test cases loaded from `protocolvalues.json`
- **`ReferenceEntry`** - Contains a SQL query and its associated column metadata & result values
- **`ColumnMetadata`** - Extracts and serializes JDBC ResultSetMetaData

## Running the Tool

### Using Gradle (Recommended)
```bash
./gradlew :jdbc-reference:run
```

## Customizing Test Data

The tool reads test cases from `src/main/resources/protocolvalues.json`. This file contains:

- **Type definitions** - Data type specifications
- **SQL queries** - SELECT statements for testing each type
- **Expected values** - Expected query results
- **Interestingness levels** - Categorization of test importance

To add new data types or modify existing tests, edit `protocolvalues.json` and rerun the generator.
