/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.hyper;

import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncStreamObserverIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.AttachedDatabase;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * Utility for setting up databases via raw gRPC, independent of the JDBC driver.
 *
 * <p>This can be reused from gRPC-only test scenarios that don't go through JDBC.
 * It uses the HyperService stub directly to execute DDL statements.</p>
 */
@Slf4j
public final class HyperDatabaseSetup {

    private static final long STATEMENT_TIMEOUT_SECONDS = 30;

    private HyperDatabaseSetup() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Executes a single SQL statement via the raw gRPC stub, consuming the full response stream.
     *
     * @param stub the HyperService stub to use
     * @param sql  the SQL statement to execute
     * @throws RuntimeException if execution fails
     */
    public static void executeStatement(HyperServiceGrpc.HyperServiceStub stub, String sql) {
        QueryParam param = QueryParam.newBuilder()
                .setSql(sql)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.SYNC)
                .build();

        AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> iterator =
                new AsyncStreamObserverIterator<>(sql, log);
        stub.executeQuery(param, iterator.getObserver());

        // Drain the response stream
        try {
            while (true) {
                CompletionStage<Optional<ExecuteQueryResponse>> next = iterator.next();
                Optional<ExecuteQueryResponse> response =
                        next.toCompletableFuture().get(STATEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!response.isPresent()) {
                    break;
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to execute: " + sql, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while executing: " + sql, e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out executing: " + sql, e);
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes multiple SQL statements sequentially via the raw gRPC stub.
     *
     * @param stub       the HyperService stub to use
     * @param statements the SQL statements to execute
     * @throws RuntimeException if any execution fails
     */
    public static void executeStatements(HyperServiceGrpc.HyperServiceStub stub, List<String> statements) {
        for (String sql : statements) {
            log.info("Executing setup statement: {}", sql);
            executeStatement(stub, sql);
        }
    }

    /**
     * Creates a new Hyper database file via the raw gRPC stub.
     *
     * <p>The database path is created as a temp file and returned. Subsequent statements
     * that need to operate on this database must attach it via {@link AttachedDatabase}
     * in the {@link QueryParam} (or use {@link com.salesforce.datacloud.jdbc.interceptor.DatabaseAttachInterceptor}).</p>
     *
     * @param stub the HyperService stub to use (without any database attached)
     * @return the absolute path to the created database file
     */
    public static String createDatabase(HyperServiceGrpc.HyperServiceStub stub) {
        try {
            File dbFile = Files.createTempFile("hyper_test_", ".hyper").toFile();
            dbFile.delete(); // Hyper needs to create the file itself
            dbFile.deleteOnExit();
            String dbPath = dbFile.getAbsolutePath();
            log.info("Creating database at: {}", dbPath);
            executeStatement(stub, "CREATE DATABASE \"" + dbPath + "\"");
            return dbPath;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file for database", e);
        }
    }

    /**
     * Executes a single SQL statement with a database attached.
     *
     * @param stub         the HyperService stub to use
     * @param databasePath the path to the attached database
     * @param databaseAlias the alias for the database in SQL
     * @param sql          the SQL statement to execute
     */
    public static void executeStatementWithDatabase(
            HyperServiceGrpc.HyperServiceStub stub, String databasePath, String databaseAlias, String sql) {
        QueryParam param = QueryParam.newBuilder()
                .setSql(sql)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.SYNC)
                .addDatabases(AttachedDatabase.newBuilder()
                        .setPath(databasePath)
                        .setAlias(databaseAlias)
                        .build())
                .build();

        AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> iterator =
                new AsyncStreamObserverIterator<>(sql, log);
        stub.executeQuery(param, iterator.getObserver());

        try {
            while (true) {
                CompletionStage<Optional<ExecuteQueryResponse>> next = iterator.next();
                Optional<ExecuteQueryResponse> response =
                        next.toCompletableFuture().get(STATEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!response.isPresent()) {
                    break;
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to execute: " + sql, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while executing: " + sql, e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out executing: " + sql, e);
        } finally {
            iterator.close();
        }
    }

    /**
     * Creates a database and populates it with the all-types test table.
     *
     * @param stub       the HyperService stub to use
     * @param schemaName the schema name to create inside the database
     * @param tableName  the table name to create
     * @return the absolute path to the created database file
     */
    public static String createAndPopulateDatabase(
            HyperServiceGrpc.HyperServiceStub stub, String schemaName, String tableName) {
        String dbPath = createDatabase(stub);
        for (String sql : allTypesSetupStatements(schemaName, tableName)) {
            log.info("Setup: {}", sql);
            executeStatementWithDatabase(stub, dbPath, "default", sql);
        }
        return dbPath;
    }

    /**
     * Returns DDL statements that create a schema and table covering all Hyper types
     * relevant for metadata testing (pg_class, pg_type, pg_attribute queries).
     *
     * <p>The types are derived from the protocol values used in reference testing
     * and cover the full range of types the JDBC driver maps in QueryMetadataUtil.</p>
     *
     * @param schemaName the schema name to create
     * @param tableName  the table name to create
     * @return list of DDL statements
     */
    public static List<String> allTypesSetupStatements(String schemaName, String tableName) {
        String qualifiedName = schemaName + "." + tableName;
        return Arrays.asList(
                "CREATE SCHEMA IF NOT EXISTS " + schemaName,
                "DROP TABLE IF EXISTS " + qualifiedName,
                "CREATE TABLE " + qualifiedName + " ("
                        + "col_bool             bool,"
                        + "col_smallint         smallint,"
                        + "col_int              int,"
                        + "col_bigint           bigint,"
                        + "col_double           double precision,"
                        + "col_numeric_18_2     numeric(18,2),"
                        + "col_numeric_10_5     numeric(10,5),"
                        + "col_text             text,"
                        + "col_varchar_255      varchar(255),"
                        + "col_char_1           char(1),"
                        + "col_date             date,"
                        + "col_time             time,"
                        + "col_timestamp        timestamp,"
                        + "col_timestamptz      timestamptz,"
                        + "col_interval         interval,"
                        + "col_json             json,"
                        + "col_oid              oid,"
                        + "col_int_array        int[],"
                        + "col_text_array       text[],"
                        + "col_nullable_int     int,"
                        + "col_not_null_int     int NOT NULL"
                        + ")",
                "INSERT INTO " + qualifiedName + " VALUES ("
                        + "true,"              // bool
                        + "42,"                // smallint
                        + "100000,"            // int
                        + "9999999999,"        // bigint
                        + "2.718281828,"        // double precision
                        + "1234567.89,"        // numeric(18,2)
                        + "12345.67890,"       // numeric(10,5)
                        + "'hello world',"     // text
                        + "'test varchar',"    // varchar
                        + "'A',"               // char(1)
                        + "'2024-01-15',"      // date
                        + "'13:45:30',"        // time
                        + "'2024-01-15 13:45:30'," // timestamp
                        + "'2024-01-15 13:45:30+00'," // timestamptz
                        + "'1 year 2 months 3 days'," // interval
                        + "'{\"key\": \"value\"}'," // json
                        + "12345,"             // oid
                        + "ARRAY[1, 2, 3],"   // int[]
                        + "ARRAY['a', 'b'],"  // text[]
                        + "NULL,"              // nullable int
                        + "7"                  // not null int
                        + ")");
    }
}
