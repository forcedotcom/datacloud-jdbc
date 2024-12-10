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

import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPassword;
import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.auth.DataCloudToken;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.config.KeywordResources;
import com.salesforce.datacloud.jdbc.core.model.DataspaceResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.ClientBuilder;
import com.salesforce.datacloud.jdbc.util.Constants;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class DataCloudDatabaseMetadataTest {
    static final int NUM_TABLE_METADATA_COLUMNS = 10;
    static final int NUM_COLUMN_METADATA_COLUMNS = 24;
    static final int NUM_SCHEMA_METADATA_COLUMNS = 2;
    static final int NUM_TABLE_TYPES_METADATA_COLUMNS = 1;
    static final int NUM_CATALOG_METADATA_COLUMNS = 1;
    private static final String FAKE_TOKEN =
            "eyJraWQiOiJDT1JFLjAwRE9LMDAwMDAwOVp6ci4xNzE4MDUyMTU0NDIyIiwidHlwIjoiSldUIiwiYWxnIjoiRVMyNTYifQ.eyJzdWIiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS9pZC8wMERPSzAwMDAwMDlaenIyQUUvMDA1T0swMDAwMDBVeTkxWUFDIiwic2NwIjoiY2RwX3Byb2ZpbGVfYXBpIGNkcF9pbmdlc3RfYXBpIGNkcF9pZGVudGl0eXJlc29sdXRpb25fYXBpIGNkcF9zZWdtZW50X2FwaSBjZHBfcXVlcnlfYXBpIGNkcF9hcGkiLCJpc3MiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS8iLCJvcmdJZCI6IjAwRE9LMDAwMDAwOVp6ciIsImlzc3VlclRlbmFudElkIjoiY29yZS9mYWxjb250ZXN0MS1jb3JlNG9yYTE1LzAwRE9LMDAwMDAwOVp6cjJBRSIsInNmYXBwaWQiOiIzTVZHOVhOVDlUbEI3VmtZY0tIVm5sUUZzWEd6cUJuMGszUC5zNHJBU0I5V09oRU1OdkgyNzNpM1NFRzF2bWl3WF9YY2NXOUFZbHA3VnJnQ3BGb0ZXIiwiYXVkaWVuY2VUZW5hbnRJZCI6ImEzNjAvZmFsY29uZGV2L2E2ZDcyNmE3M2Y1MzQzMjdhNmE4ZTJlMGYzY2MzODQwIiwiY3VzdG9tX2F0dHJpYnV0ZXMiOnsiZGF0YXNwYWNlIjoiZGVmYXVsdCJ9LCJhdWQiOiJhcGkuYTM2MC5zYWxlc2ZvcmNlLmNvbSIsIm5iZiI6MTcyMDczMTAyMSwic2ZvaWQiOiIwMERPSzAwMDAwMDlaenIiLCJzZnVpZCI6IjAwNU9LMDAwMDAwVXk5MSIsImV4cCI6MTcyMDczODI4MCwiaWF0IjoxNzIwNzMxMDgxLCJqdGkiOiIwYjYwMzc4OS1jMGI2LTQwZTMtYmIzNi03NDQ3MzA2MzAxMzEifQ.lXgeAhJIiGoxgNpBi0W5oBWyn2_auB2bFxxajGuK6DMHlkqDhHJAlFN_uf6QPSjGSJCh5j42Ow5SrEptUDJwmQ";
    private static final String FAKE_TENANT_ID = "a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840";

    @Mock
    DataCloudStatement dataCloudStatement;

    @Mock
    TokenProcessor tokenProcessor;

    @Mock
    ResultSet resultSetMock;

    @Mock
    AuthenticationSettings authenticationSettings;

    DataCloudDatabaseMetadata dataCloudDatabaseMetadata;

    @BeforeEach
    public void beforeEach() {
        dataCloudStatement = mock(DataCloudStatement.class);
        tokenProcessor = mock(TokenProcessor.class);
        val properties = propertiesForPassword("un", "pw");
        val client = ClientBuilder.buildOkHttpClient(properties);
        authenticationSettings = mock(AuthenticationSettings.class);
        dataCloudDatabaseMetadata = new DataCloudDatabaseMetadata(
                dataCloudStatement, Optional.ofNullable(tokenProcessor), client, "loginURL", "userName");
    }

    @Test
    public void testAllProceduresAreCallable() {
        assertThat(dataCloudDatabaseMetadata.allProceduresAreCallable()).isFalse();
    }

    @Test
    public void testAllTablesAreSelectable() {
        assertThat(dataCloudDatabaseMetadata.allTablesAreSelectable()).isTrue();
    }

    @Test
    public void testGetURL() {
        assertThat(dataCloudDatabaseMetadata.getURL()).isEqualTo("loginURL");
    }

    @Test
    public void testGetUserName() {
        assertThat(dataCloudDatabaseMetadata.getUserName()).isEqualTo("userName");
    }

    @Test
    public void testIsReadOnly() {
        assertThat(dataCloudDatabaseMetadata.isReadOnly()).isTrue();
    }

    @Test
    public void testNullsAreSortedHigh() {
        assertThat(dataCloudDatabaseMetadata.nullsAreSortedHigh()).isFalse();
    }

    @Test
    public void testNullsAreSortedLow() {
        assertThat(dataCloudDatabaseMetadata.nullsAreSortedLow()).isTrue();
    }

    @Test
    public void testNullsAreSortedAtStart() {
        assertThat(dataCloudDatabaseMetadata.nullsAreSortedAtStart()).isFalse();
    }

    @Test
    public void testNullsAreSortedAtEnd() {
        assertThat(dataCloudDatabaseMetadata.nullsAreSortedAtEnd()).isFalse();
    }

    @Test
    public void testGetDatabaseProductName() {
        assertThat(dataCloudDatabaseMetadata.getDatabaseProductName()).isEqualTo(Constants.DATABASE_PRODUCT_NAME);
    }

    @Test
    public void testGetDatabaseProductVersion() {
        assertThat(dataCloudDatabaseMetadata.getDatabaseProductVersion()).isEqualTo(Constants.DATABASE_PRODUCT_VERSION);
    }

    @Test
    public void testGetDriverName() {
        assertThat(dataCloudDatabaseMetadata.getDriverName()).isEqualTo(Constants.DRIVER_NAME);
    }

    @Test
    public void testGetDriverVersion() {
        assertThat(dataCloudDatabaseMetadata.getDriverVersion()).isEqualTo(Constants.DRIVER_VERSION);
    }

    @Test
    public void testGetDriverMajorVersion() {
        assertThat(dataCloudDatabaseMetadata.getDriverMajorVersion()).isEqualTo(1);
    }

    @Test
    public void testGetDriverMinorVersion() {
        assertThat(dataCloudDatabaseMetadata.getDriverMinorVersion()).isEqualTo(0);
    }

    @Test
    public void testUsesLocalFiles() {
        assertThat(dataCloudDatabaseMetadata.usesLocalFiles()).isFalse();
    }

    @Test
    public void testUsesLocalFilePerTable() {
        assertThat(dataCloudDatabaseMetadata.usesLocalFilePerTable()).isFalse();
    }

    @Test
    public void testSupportsMixedCaseIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.supportsMixedCaseIdentifiers()).isFalse();
    }

    @Test
    public void testStoresUpperCaseIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesUpperCaseIdentifiers()).isFalse();
    }

    @Test
    public void testStoresLowerCaseIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesLowerCaseIdentifiers()).isTrue();
    }

    @Test
    public void testStoresMixedCaseIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesMixedCaseIdentifiers()).isFalse();
    }

    @Test
    public void testSupportsMixedCaseQuotedIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.supportsMixedCaseQuotedIdentifiers())
                .isTrue();
    }

    @Test
    public void testStoresUpperCaseQuotedIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesUpperCaseQuotedIdentifiers()).isFalse();
    }

    @Test
    public void testStoresLowerCaseQuotedIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesLowerCaseQuotedIdentifiers()).isFalse();
    }

    @Test
    public void testStoresMixedCaseQuotedIdentifiers() {
        assertThat(dataCloudDatabaseMetadata.storesMixedCaseQuotedIdentifiers()).isFalse();
    }

    @Test
    public void testGetIdentifierQuoteString() {
        assertThat(dataCloudDatabaseMetadata.getIdentifierQuoteString()).isEqualTo("\"");
    }
    /**
     * The expected output of getSqlKeywords is an alphabetized, all-caps, comma-delimited String made up of all the
     * keywords found in Hyper's SQL Lexer, excluding: those that are also <a
     * href="https://firebirdsql.org/en/iso-9075-sql-standard-keywords-reserved-words/">SQL:2003 keywords</a>
     * pseudo-tokens like "<=", ">=", "==", "=>" tokens ending with "_la" Hyper-Script tokens like "break", "continue",
     * "throw", "var", "while", "yield" To add new keywords, adjust the file
     * src/main/resources/keywords/hyper_sql_lexer_keywords.txt
     */
    @Test
    public void testGetSQLKeywords() {
        val actual = dataCloudDatabaseMetadata.getSQLKeywords().split(",");
        assertThat(actual.length).isGreaterThan(250).isLessThan(300);
        KeywordResources.SQL_2003_KEYWORDS.forEach(k -> assertThat(actual).doesNotContain(k));
        val sorted = Arrays.stream(actual).sorted().collect(Collectors.toList());
        val uppercase = Arrays.stream(actual).map(String::toUpperCase).collect(Collectors.toList());
        val distinct = Arrays.stream(actual).distinct().collect(Collectors.toList());
        assertThat(sorted)
                .withFailMessage("SQL Keywords should be in alphabetical order.")
                .containsExactly(actual);
        assertThat(uppercase)
                .withFailMessage("SQL Keywords should contain uppercase values.")
                .containsExactly(actual);
        assertThat(distinct)
                .withFailMessage("SQL Keywords should have no duplicates.")
                .containsExactly(actual);
    }

    @Test
    public void testGetNumericFunctions() {
        assertThat(dataCloudDatabaseMetadata.getNumericFunctions()).isNull();
    }

    @Test
    public void testGetStringFunctions() {
        assertThat(dataCloudDatabaseMetadata.getStringFunctions()).isNull();
    }

    @Test
    public void testGetSystemFunctions() {
        assertThat(dataCloudDatabaseMetadata.getSystemFunctions()).isNull();
    }

    @Test
    public void testGetTimeDateFunctions() {
        assertThat(dataCloudDatabaseMetadata.getTimeDateFunctions()).isNull();
    }

    @Test
    public void testGetSearchStringEscape() {
        assertThat(dataCloudDatabaseMetadata.getSearchStringEscape()).isEqualTo("\\");
    }

    @Test
    public void testGetExtraNameCharacters() {
        assertThat(dataCloudDatabaseMetadata.getExtraNameCharacters()).isNull();
    }

    @Test
    public void testSupportsAlterTableWithAddColumn() {
        assertThat(dataCloudDatabaseMetadata.supportsAlterTableWithAddColumn()).isFalse();
    }

    @Test
    public void testSupportsAlterTableWithDropColumn() {
        assertThat(dataCloudDatabaseMetadata.supportsAlterTableWithDropColumn()).isFalse();
    }

    @Test
    public void testSupportsColumnAliasing() {
        assertThat(dataCloudDatabaseMetadata.supportsColumnAliasing()).isTrue();
    }

    @Test
    public void testNullPlusNonNullIsNull() {
        assertThat(dataCloudDatabaseMetadata.nullPlusNonNullIsNull()).isFalse();
    }

    @Test
    public void testSupportsConvert() {
        assertThat(dataCloudDatabaseMetadata.supportsConvert()).isTrue();
    }

    @Test
    public void testSupportsConvertFromTypeToType() {
        assertThat(dataCloudDatabaseMetadata.supportsConvert(1, 1)).isTrue();
    }

    @Test
    public void testSupportsTableCorrelationNames() {
        assertThat(dataCloudDatabaseMetadata.supportsTableCorrelationNames()).isTrue();
    }

    @Test
    public void testSupportsDifferentTableCorrelationNames() {
        assertThat(dataCloudDatabaseMetadata.supportsDifferentTableCorrelationNames())
                .isFalse();
    }

    @Test
    public void testSupportsExpressionsInOrderBy() {
        assertThat(dataCloudDatabaseMetadata.supportsExpressionsInOrderBy()).isTrue();
    }

    @Test
    public void testSupportsOrderByUnrelated() {
        assertThat(dataCloudDatabaseMetadata.supportsOrderByUnrelated()).isTrue();
    }

    @Test
    public void testSupportsGroupBy() {
        assertThat(dataCloudDatabaseMetadata.supportsGroupBy()).isTrue();
    }

    @Test
    public void testSupportsGroupByUnrelated() {
        assertThat(dataCloudDatabaseMetadata.supportsGroupByUnrelated()).isTrue();
    }

    @Test
    public void testSupportsGroupByBeyondSelect() {
        assertThat(dataCloudDatabaseMetadata.supportsGroupByBeyondSelect()).isTrue();
    }

    @Test
    public void testSupportsLikeEscapeClause() {
        assertThat(dataCloudDatabaseMetadata.supportsLikeEscapeClause()).isTrue();
    }

    @Test
    public void testSupportsMultipleResultSets() {
        assertThat(dataCloudDatabaseMetadata.supportsMultipleResultSets()).isFalse();
    }

    @Test
    public void testSupportsMultipleTransactions() {
        assertThat(dataCloudDatabaseMetadata.supportsMultipleTransactions()).isFalse();
    }

    @Test
    public void testSupportsNonNullableColumns() {
        assertThat(dataCloudDatabaseMetadata.supportsNonNullableColumns()).isTrue();
    }

    @Test
    public void testSupportsMinimumSQLGrammar() {
        assertThat(dataCloudDatabaseMetadata.supportsMinimumSQLGrammar()).isTrue();
    }

    @Test
    public void testSupportsCoreSQLGrammar() {
        assertThat(dataCloudDatabaseMetadata.supportsCoreSQLGrammar()).isFalse();
    }

    @Test
    public void testSupportsExtendedSQLGrammar() {
        assertThat(dataCloudDatabaseMetadata.supportsExtendedSQLGrammar()).isFalse();
    }

    @Test
    public void testSupportsANSI92EntryLevelSQL() {
        assertThat(dataCloudDatabaseMetadata.supportsANSI92EntryLevelSQL()).isTrue();
    }

    @Test
    public void testSupportsANSI92IntermediateSQL() {
        assertThat(dataCloudDatabaseMetadata.supportsANSI92IntermediateSQL()).isTrue();
    }

    @Test
    public void testSupportsANSI92FullSQL() {
        assertThat(dataCloudDatabaseMetadata.supportsANSI92FullSQL()).isTrue();
    }

    @Test
    public void testSupportsIntegrityEnhancementFacility() {
        assertThat(dataCloudDatabaseMetadata.supportsIntegrityEnhancementFacility())
                .isFalse();
    }

    @Test
    public void testSupportsOuterJoins() {
        assertThat(dataCloudDatabaseMetadata.supportsOuterJoins()).isTrue();
    }

    @Test
    public void testSupportsFullOuterJoins() {
        assertThat(dataCloudDatabaseMetadata.supportsFullOuterJoins()).isTrue();
    }

    @Test
    public void testSupportsLimitedOuterJoins() {
        assertThat(dataCloudDatabaseMetadata.supportsLimitedOuterJoins()).isTrue();
    }

    @Test
    public void testGetSchemaTerm() {
        assertThat(dataCloudDatabaseMetadata.getSchemaTerm()).isEqualTo("schema");
    }

    @Test
    public void testGetProcedureTerm() {
        assertThat(dataCloudDatabaseMetadata.getProcedureTerm()).isEqualTo("procedure");
    }

    @Test
    public void testGetCatalogTerm() {
        assertThat(dataCloudDatabaseMetadata.getCatalogTerm()).isEqualTo("database");
    }

    @Test
    public void testIsCatalogAtStart() {
        assertThat(dataCloudDatabaseMetadata.isCatalogAtStart()).isTrue();
    }

    @Test
    public void testGetCatalogSeparator() {
        assertThat(dataCloudDatabaseMetadata.getCatalogSeparator()).isEqualTo(".");
    }

    @Test
    public void testSupportsSchemasInDataManipulation() {
        assertThat(dataCloudDatabaseMetadata.supportsSchemasInDataManipulation())
                .isFalse();
    }

    @Test
    public void testSupportsSchemasInProcedureCalls() {
        assertThat(dataCloudDatabaseMetadata.supportsSchemasInProcedureCalls()).isFalse();
    }

    @Test
    public void testSupportsSchemasInTableDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsSchemasInTableDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsSchemasInIndexDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsSchemasInIndexDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsSchemasInPrivilegeDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsSchemasInPrivilegeDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsCatalogsInDataManipulation() {
        assertThat(dataCloudDatabaseMetadata.supportsCatalogsInDataManipulation())
                .isFalse();
    }

    @Test
    public void testSupportsCatalogsInProcedureCalls() {
        assertThat(dataCloudDatabaseMetadata.supportsCatalogsInProcedureCalls()).isFalse();
    }

    @Test
    public void testSupportsCatalogsInTableDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsCatalogsInTableDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsCatalogsInIndexDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsCatalogsInIndexDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsCatalogsInPrivilegeDefinitions() {
        assertThat(dataCloudDatabaseMetadata.supportsCatalogsInPrivilegeDefinitions())
                .isFalse();
    }

    @Test
    public void testSupportsPositionedDelete() {
        assertThat(dataCloudDatabaseMetadata.supportsPositionedDelete()).isFalse();
    }

    @Test
    public void testSupportsPositionedUpdate() {
        assertThat(dataCloudDatabaseMetadata.supportsPositionedUpdate()).isFalse();
    }

    @Test
    public void testSupportsSelectForUpdate() {
        assertThat(dataCloudDatabaseMetadata.supportsSelectForUpdate()).isFalse();
    }

    @Test
    public void testSupportsStoredProcedures() {
        assertThat(dataCloudDatabaseMetadata.supportsStoredProcedures()).isFalse();
    }

    @Test
    public void testSupportsSubqueriesInComparisons() {
        assertThat(dataCloudDatabaseMetadata.supportsSubqueriesInComparisons()).isTrue();
    }

    @Test
    public void testSupportsSubqueriesInExists() {
        assertThat(dataCloudDatabaseMetadata.supportsSubqueriesInExists()).isTrue();
    }

    @Test
    public void testSupportsSubqueriesInIns() {
        assertThat(dataCloudDatabaseMetadata.supportsSubqueriesInIns()).isTrue();
    }

    @Test
    public void testSupportsSubqueriesInQuantifieds() {
        assertThat(dataCloudDatabaseMetadata.supportsSubqueriesInQuantifieds()).isTrue();
    }

    @Test
    public void testSupportsCorrelatedSubqueries() {
        assertThat(dataCloudDatabaseMetadata.supportsCorrelatedSubqueries()).isTrue();
    }

    @Test
    public void testSupportsUnion() {
        assertThat(dataCloudDatabaseMetadata.supportsUnion()).isTrue();
    }

    @Test
    public void testSupportsUnionAll() {
        assertThat(dataCloudDatabaseMetadata.supportsUnionAll()).isTrue();
    }

    @Test
    public void testSupportsOpenCursorsAcrossCommit() {
        assertThat(dataCloudDatabaseMetadata.supportsOpenCursorsAcrossCommit()).isFalse();
    }

    @Test
    public void testSupportsOpenCursorsAcrossRollback() {
        assertThat(dataCloudDatabaseMetadata.supportsOpenCursorsAcrossRollback())
                .isFalse();
    }

    @Test
    public void testSupportsOpenStatementsAcrossCommit() {
        assertThat(dataCloudDatabaseMetadata.supportsOpenStatementsAcrossCommit())
                .isFalse();
    }

    @Test
    public void testSupportsOpenStatementsAcrossRollback() {
        assertThat(dataCloudDatabaseMetadata.supportsOpenStatementsAcrossRollback())
                .isFalse();
    }

    @Test
    public void testGetMaxBinaryLiteralLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxBinaryLiteralLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxCharLiteralLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxCharLiteralLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnsInGroupBy() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnsInGroupBy()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnsInIndex() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnsInIndex()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnsInOrderBy() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnsInOrderBy()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnsInSelect() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnsInSelect()).isEqualTo(0);
    }

    @Test
    public void testGetMaxColumnsInTable() {
        assertThat(dataCloudDatabaseMetadata.getMaxColumnsInTable()).isEqualTo(0);
    }

    @Test
    public void testGetMaxConnections() {
        assertThat(dataCloudDatabaseMetadata.getMaxConnections()).isEqualTo(0);
    }

    @Test
    public void testGetMaxCursorNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxCursorNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxIndexLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxIndexLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxSchemaNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxSchemaNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxProcedureNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxProcedureNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxCatalogNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxCatalogNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxRowSize() {
        assertThat(dataCloudDatabaseMetadata.getMaxRowSize()).isEqualTo(0);
    }

    @Test
    public void testDoesMaxRowSizeIncludeBlobs() {
        assertThat(dataCloudDatabaseMetadata.doesMaxRowSizeIncludeBlobs()).isFalse();
    }

    @Test
    public void testGetMaxStatementLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxStatementLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxStatements() {
        assertThat(dataCloudDatabaseMetadata.getMaxStatements()).isEqualTo(0);
    }

    @Test
    public void testGetMaxTableNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxTableNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetMaxTablesInSelect() {
        assertThat(dataCloudDatabaseMetadata.getMaxTablesInSelect()).isEqualTo(0);
    }

    @Test
    public void testGetMaxUserNameLength() {
        assertThat(dataCloudDatabaseMetadata.getMaxUserNameLength()).isEqualTo(0);
    }

    @Test
    public void testGetDefaultTransactionIsolation() {
        assertThat(dataCloudDatabaseMetadata.getDefaultTransactionIsolation())
                .isEqualTo(Connection.TRANSACTION_SERIALIZABLE);
    }

    @Test
    public void testSupportsTransactions() {
        assertThat(dataCloudDatabaseMetadata.supportsTransactions()).isFalse();
    }

    @Test
    public void testSupportsTransactionIsolationLevel() {
        assertThat(dataCloudDatabaseMetadata.supportsTransactionIsolationLevel(1))
                .isFalse();
    }

    @Test
    public void testSupportsDataDefinitionAndDataManipulationTransactions() {
        assertThat(dataCloudDatabaseMetadata.supportsDataDefinitionAndDataManipulationTransactions())
                .isFalse();
    }

    @Test
    public void testSupportsDataManipulationTransactionsOnly() {
        assertThat(dataCloudDatabaseMetadata.supportsDataManipulationTransactionsOnly())
                .isFalse();
    }

    @Test
    public void testDataDefinitionCausesTransactionCommit() {
        assertThat(dataCloudDatabaseMetadata.dataDefinitionCausesTransactionCommit())
                .isFalse();
    }

    @Test
    public void testDataDefinitionIgnoredInTransactions() {
        assertThat(dataCloudDatabaseMetadata.dataDefinitionIgnoredInTransactions())
                .isFalse();
    }

    @Test
    public void testGetProcedures() {
        assertThat(dataCloudDatabaseMetadata.getProcedures(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGetProcedureColumns() {
        assertThat(dataCloudDatabaseMetadata.getProcedureColumns(
                        StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    @SneakyThrows
    public void testGetTables() {
        String[] types = new String[] {};
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet = dataCloudDatabaseMetadata.getTables(null, "schemaName", "tableName", types);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_TABLE_METADATA_COLUMNS);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(3)).isEqualTo("TABLE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(4)).isEqualTo("TABLE_TYPE");
        assertThat(resultSet.getMetaData().getColumnName(5)).isEqualTo("REMARKS");
        assertThat(resultSet.getMetaData().getColumnName(6)).isEqualTo("TYPE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(7)).isEqualTo("TYPE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(8)).isEqualTo("TYPE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(9)).isEqualTo("SELF_REFERENCING_COL_NAME");
        assertThat(resultSet.getMetaData().getColumnName(10)).isEqualTo("REF_GENERATION");

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(3)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(4)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(5)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(6)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(7)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(8)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(9)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(10)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetTablesNullValues() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet = dataCloudDatabaseMetadata.getTables(null, null, null, null);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(10);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(3)).isEqualTo("TABLE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(4)).isEqualTo("TABLE_TYPE");
        assertThat(resultSet.getMetaData().getColumnName(5)).isEqualTo("REMARKS");
        assertThat(resultSet.getMetaData().getColumnName(6)).isEqualTo("TYPE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(7)).isEqualTo("TYPE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(8)).isEqualTo("TYPE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(9)).isEqualTo("SELF_REFERENCING_COL_NAME");
        assertThat(resultSet.getMetaData().getColumnName(10)).isEqualTo("REF_GENERATION");

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(3)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(4)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(5)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(6)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(7)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(8)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(9)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(10)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetTablesEmptyValues() {
        String[] emptyTypes = new String[] {};
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet =
                dataCloudDatabaseMetadata.getTables(null, StringUtils.EMPTY, StringUtils.EMPTY, emptyTypes);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(10);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(3)).isEqualTo("TABLE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(4)).isEqualTo("TABLE_TYPE");
        assertThat(resultSet.getMetaData().getColumnName(5)).isEqualTo("REMARKS");
        assertThat(resultSet.getMetaData().getColumnName(6)).isEqualTo("TYPE_CAT");
        assertThat(resultSet.getMetaData().getColumnName(7)).isEqualTo("TYPE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(8)).isEqualTo("TYPE_NAME");
        assertThat(resultSet.getMetaData().getColumnName(9)).isEqualTo("SELF_REFERENCING_COL_NAME");
        assertThat(resultSet.getMetaData().getColumnName(10)).isEqualTo("REF_GENERATION");

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(3)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(4)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(5)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(6)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(7)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(8)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(9)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(10)).isEqualTo("TEXT");
    }

    @SneakyThrows
    @Test
    public void testGetDataspaces() {
        val mapper = new ObjectMapper();
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        val dataspaceAttributeName = randomString();
        oAuthTokenResponse.setToken(accessToken);
        val dataspaceResponse = new DataspaceResponse();
        val dataspaceAttributes = new DataspaceResponse.DataSpaceAttributes();
        dataspaceAttributes.setName(dataspaceAttributeName);
        dataspaceResponse.setRecords(ImmutableList.of(dataspaceAttributes));

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            Mockito.when(tokenProcessor.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));

            server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(dataspaceResponse)));
            val actual = dataCloudDatabaseMetadata.getDataspaces();
            List<String> expected = ImmutableList.of(dataspaceAttributeName);
            assertThat(actual).isEqualTo(expected);

            val actualRequest = server.takeRequest();
            val query = "SELECT+name+from+Dataspace";
            assertThat(actualRequest.getMethod()).isEqualTo("GET");
            assertThat(actualRequest.getRequestUrl()).isEqualTo(server.url("services/data/v61.0/query/?q=" + query));
            assertThat(actualRequest.getBody().readUtf8()).isBlank();
            assertThat(actualRequest.getHeader("Authorization")).isEqualTo("Bearer " + accessToken);
            assertThat(actualRequest.getHeader("Content-Type")).isEqualTo("application/json");
            assertThat(actualRequest.getHeader("User-Agent")).isEqualTo("cdp/jdbc");
            assertThat(actualRequest.getHeader("enable-stream-flow")).isEqualTo("false");
        }
    }

    @SneakyThrows
    @Test
    public void testGetDataspacesThrowsExceptionWhenCallFails() {
        val oAuthTokenResponse = new OAuthTokenResponse();
        val accessToken = UUID.randomUUID().toString();
        val dataspaceAttributeName = randomString();
        oAuthTokenResponse.setToken(accessToken);
        val dataspaceResponse = new DataspaceResponse();
        val dataspaceAttributes = new DataspaceResponse.DataSpaceAttributes();
        dataspaceAttributes.setName(dataspaceAttributeName);
        dataspaceResponse.setRecords(ImmutableList.of(dataspaceAttributes));

        try (val server = new MockWebServer()) {
            server.start();
            oAuthTokenResponse.setInstanceUrl(server.url("").toString());
            Mockito.when(tokenProcessor.getOAuthToken()).thenReturn(OAuthToken.of(oAuthTokenResponse));

            server.enqueue(new MockResponse().setResponseCode(500));
            Assertions.assertThrows(DataCloudJDBCException.class, () -> dataCloudDatabaseMetadata.getDataspaces());
        }
    }

    @SneakyThrows
    @Test
    public void testGetCatalogs() {
        val mapper = new ObjectMapper();
        val oAuthTokenResponse = new OAuthTokenResponse();
        val dataCloudTokenResponse = new DataCloudTokenResponse();
        val dataSpaceName = randomString();
        oAuthTokenResponse.setToken(FAKE_TOKEN);
        dataCloudTokenResponse.setToken(FAKE_TOKEN);
        dataCloudTokenResponse.setInstanceUrl(FAKE_TENANT_ID);
        dataCloudTokenResponse.setTokenType("token");

        Mockito.when(tokenProcessor.getDataCloudToken()).thenReturn(DataCloudToken.of(dataCloudTokenResponse));
        Mockito.when(tokenProcessor.getSettings()).thenReturn(authenticationSettings);
        Mockito.when(authenticationSettings.getDataspace()).thenReturn(dataSpaceName);

        val actual = dataCloudDatabaseMetadata.getCatalogs();
        assertThat(actual.next()).isTrue();
        assertThat(actual.getString(1)).isEqualTo("lakehouse:" + FAKE_TENANT_ID + ";" + dataSpaceName);
        assertThat(actual.getMetaData().getColumnName(1)).isEqualTo("TABLE_CAT");
        assertThat(actual.next()).isFalse();
    }

    @Test
    @SneakyThrows
    public void testGetTableTypes() {

        ResultSet resultSet = dataCloudDatabaseMetadata.getTableTypes();
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_TABLE_TYPES_METADATA_COLUMNS);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_TYPE");

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetColumnsContainsCorrectMetadata() {
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet = dataCloudDatabaseMetadata.getColumns(null, "schemaName", "tableName", "columnName");
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_COLUMN_METADATA_COLUMNS);

        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_CAT");
        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(3)).isEqualTo("TABLE_NAME");
        assertThat(resultSet.getMetaData().getColumnTypeName(3)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(4)).isEqualTo("COLUMN_NAME");
        assertThat(resultSet.getMetaData().getColumnTypeName(4)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(5)).isEqualTo("DATA_TYPE");
        assertThat(resultSet.getMetaData().getColumnTypeName(5)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(6)).isEqualTo("TYPE_NAME");
        assertThat(resultSet.getMetaData().getColumnTypeName(6)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(7)).isEqualTo("COLUMN_SIZE");
        assertThat(resultSet.getMetaData().getColumnTypeName(7)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(8)).isEqualTo("BUFFER_LENGTH");
        assertThat(resultSet.getMetaData().getColumnTypeName(8)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(9)).isEqualTo("DECIMAL_DIGITS");
        assertThat(resultSet.getMetaData().getColumnTypeName(9)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(10)).isEqualTo("NUM_PREC_RADIX");
        assertThat(resultSet.getMetaData().getColumnTypeName(10)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(11)).isEqualTo("NULLABLE");
        assertThat(resultSet.getMetaData().getColumnTypeName(11)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(12)).isEqualTo("REMARKS");
        assertThat(resultSet.getMetaData().getColumnTypeName(12)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(13)).isEqualTo("COLUMN_DEF");
        assertThat(resultSet.getMetaData().getColumnTypeName(13)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(14)).isEqualTo("SQL_DATA_TYPE");
        assertThat(resultSet.getMetaData().getColumnTypeName(14)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(15)).isEqualTo("SQL_DATETIME_SUB");
        assertThat(resultSet.getMetaData().getColumnTypeName(15)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(16)).isEqualTo("CHAR_OCTET_LENGTH");
        assertThat(resultSet.getMetaData().getColumnTypeName(16)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(17)).isEqualTo("ORDINAL_POSITION");
        assertThat(resultSet.getMetaData().getColumnTypeName(17)).isEqualTo("INTEGER");

        assertThat(resultSet.getMetaData().getColumnName(18)).isEqualTo("IS_NULLABLE");
        assertThat(resultSet.getMetaData().getColumnTypeName(18)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(19)).isEqualTo("SCOPE_CATALOG");
        assertThat(resultSet.getMetaData().getColumnTypeName(19)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(20)).isEqualTo("SCOPE_SCHEMA");
        assertThat(resultSet.getMetaData().getColumnTypeName(20)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(21)).isEqualTo("SCOPE_TABLE");
        assertThat(resultSet.getMetaData().getColumnTypeName(21)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(22)).isEqualTo("SOURCE_DATA_TYPE");
        assertThat(resultSet.getMetaData().getColumnTypeName(22)).isEqualTo("SHORT");

        assertThat(resultSet.getMetaData().getColumnName(23)).isEqualTo("IS_AUTOINCREMENT");
        assertThat(resultSet.getMetaData().getColumnTypeName(23)).isEqualTo("TEXT");

        assertThat(resultSet.getMetaData().getColumnName(24)).isEqualTo("IS_GENERATEDCOLUMN");
        assertThat(resultSet.getMetaData().getColumnTypeName(24)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetColumnsNullValues() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet = dataCloudDatabaseMetadata.getColumns(null, null, null, null);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(24);
        assertThat(resultSet.next()).isTrue();
    }

    @Test
    @SneakyThrows
    public void testGetColumnsEmptyValues() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);

        ResultSet resultSet =
                dataCloudDatabaseMetadata.getColumns(null, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(24);
        assertThat(resultSet.next()).isTrue();
    }

    @Test
    public void testTestTest() throws SQLException {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetMock.getString("nspname")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("relname")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("attname")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("attname")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("datatype")).thenReturn("TEXT");
        Mockito.when(resultSetMock.getBoolean("attnotnull")).thenReturn(true);
        Mockito.when(resultSetMock.getString("description")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("adsrc")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getInt("attnum")).thenReturn(1);
        Mockito.when(resultSetMock.getBoolean("attnotnull")).thenReturn(true);
        Mockito.when(resultSetMock.getString("attidentity")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("adsrc")).thenReturn(StringUtils.EMPTY);
        Mockito.when(resultSetMock.getString("attgenerated")).thenReturn(StringUtils.EMPTY);

        ResultSet columnResultSet = QueryMetadataUtil.createColumnResultSet(
                StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, dataCloudStatement);
        while (columnResultSet.next()) {
            assertThat(columnResultSet.getString("TYPE_NAME")).isEqualTo("VARCHAR");
            assertThat(columnResultSet.getString("DATA_TYPE")).isEqualTo("12");
        }
    }

    @Test
    public void testGetColumnPrivileges() throws SQLException {
        assertExpectedEmptyResultSet(dataCloudDatabaseMetadata.getColumnPrivileges(
                StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetTablePrivileges() {
        assertExpectedEmptyResultSet(
                dataCloudDatabaseMetadata.getTablePrivileges(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetBestRowIdentifier() {
        assertExpectedEmptyResultSet(dataCloudDatabaseMetadata.getBestRowIdentifier(
                StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, 1, true));
    }

    @Test
    @SneakyThrows
    public void testGetVersionColumns() {
        assertExpectedEmptyResultSet(
                dataCloudDatabaseMetadata.getVersionColumns(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetPrimaryKeys() {
        assertExpectedEmptyResultSet(
                dataCloudDatabaseMetadata.getPrimaryKeys(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetImportedKeys() {
        assertExpectedEmptyResultSet(
                dataCloudDatabaseMetadata.getImportedKeys(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetExportedKeys() {
        assertExpectedEmptyResultSet(
                dataCloudDatabaseMetadata.getExportedKeys(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetCrossReference() {
        assertExpectedEmptyResultSet(dataCloudDatabaseMetadata.getCrossReference(
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY));
    }

    @Test
    @SneakyThrows
    public void testGetTypeInfo() {
        assertExpectedEmptyResultSet(dataCloudDatabaseMetadata.getTypeInfo());
    }

    @Test
    @SneakyThrows
    public void testGetIndexInfo() {
        assertExpectedEmptyResultSet(dataCloudDatabaseMetadata.getIndexInfo(
                StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, true, true));
    }

    @Test
    public void testSupportsResultSetType() {
        assertThat(dataCloudDatabaseMetadata.supportsResultSetType(1)).isFalse();
    }

    @Test
    public void testSupportsResultSetConcurrency() {
        assertThat(dataCloudDatabaseMetadata.supportsResultSetConcurrency(1, 1)).isFalse();
    }

    @Test
    public void testOwnUpdatesAreVisible() {
        assertThat(dataCloudDatabaseMetadata.ownUpdatesAreVisible(1)).isFalse();
    }

    @Test
    public void testOwnDeletesAreVisible() {
        assertThat(dataCloudDatabaseMetadata.ownDeletesAreVisible(1)).isFalse();
    }

    @Test
    public void testOwnInsertsAreVisible() {
        assertThat(dataCloudDatabaseMetadata.ownInsertsAreVisible(1)).isFalse();
    }

    @Test
    public void testOthersUpdatesAreVisible() {
        assertThat(dataCloudDatabaseMetadata.othersUpdatesAreVisible(1)).isFalse();
    }

    @Test
    public void testOthersDeletesAreVisible() {
        assertThat(dataCloudDatabaseMetadata.othersDeletesAreVisible(1)).isFalse();
    }

    @Test
    public void testOthersInsertsAreVisible() {
        assertThat(dataCloudDatabaseMetadata.othersInsertsAreVisible(1)).isFalse();
    }

    @Test
    public void testUpdatesAreDetected() {
        assertThat(dataCloudDatabaseMetadata.updatesAreDetected(1)).isFalse();
    }

    @Test
    public void testDeletesAreDetected() {
        assertThat(dataCloudDatabaseMetadata.deletesAreDetected(1)).isFalse();
    }

    @Test
    public void testInsertsAreDetected() {
        assertThat(dataCloudDatabaseMetadata.insertsAreDetected(1)).isFalse();
    }

    @Test
    public void testSupportsBatchUpdates() {
        assertThat(dataCloudDatabaseMetadata.supportsBatchUpdates()).isFalse();
    }

    @Test
    public void testGetUDTs() {
        assertThat(dataCloudDatabaseMetadata.getUDTs(
                        StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, new int[] {}))
                .isNull();
    }

    @Test
    public void testGetConnection() {
        assertThat(dataCloudDatabaseMetadata.getConnection()).isNull();
    }

    @Test
    public void testSupportsSavepoints() {
        assertThat(dataCloudDatabaseMetadata.supportsSavepoints()).isFalse();
    }

    @Test
    public void testSupportsNamedParameters() {
        assertThat(dataCloudDatabaseMetadata.supportsNamedParameters()).isFalse();
    }

    @Test
    public void testSupportsMultipleOpenResults() {
        assertThat(dataCloudDatabaseMetadata.supportsMultipleOpenResults()).isFalse();
    }

    @Test
    public void testSupportsGetGeneratedKeys() {
        assertThat(dataCloudDatabaseMetadata.supportsGetGeneratedKeys()).isFalse();
    }

    @Test
    public void testGetSuperTypes() {
        assertThat(dataCloudDatabaseMetadata.getSuperTypes(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGetSuperTables() {
        assertThat(dataCloudDatabaseMetadata.getSuperTables(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGetAttributes() {
        assertThat(dataCloudDatabaseMetadata.getAttributes(
                        StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testSupportsResultSetHoldability() {
        assertThat(dataCloudDatabaseMetadata.supportsResultSetHoldability(1)).isFalse();
    }

    @Test
    public void testGetResultSetHoldability() {
        assertThat(dataCloudDatabaseMetadata.getResultSetHoldability()).isEqualTo(0);
    }

    @Test
    public void testGetDatabaseMajorVersion() {
        assertThat(dataCloudDatabaseMetadata.getDatabaseMajorVersion()).isEqualTo(1);
    }

    @Test
    public void testGetDatabaseMinorVersion() {
        assertThat(dataCloudDatabaseMetadata.getDatabaseMinorVersion()).isEqualTo(0);
    }

    @Test
    public void testGetJDBCMajorVersion() {
        assertThat(dataCloudDatabaseMetadata.getJDBCMajorVersion()).isEqualTo(1);
    }

    @Test
    public void testGetJDBCMinorVersion() {
        assertThat(dataCloudDatabaseMetadata.getJDBCMinorVersion()).isEqualTo(0);
    }

    @Test
    public void testGetSQLStateType() {
        assertThat(dataCloudDatabaseMetadata.getSQLStateType()).isEqualTo(0);
    }

    @Test
    public void testLocatorsUpdateCopy() {
        assertThat(dataCloudDatabaseMetadata.locatorsUpdateCopy()).isFalse();
    }

    @Test
    public void testSupportsStatementPooling() {
        assertThat(dataCloudDatabaseMetadata.supportsStatementPooling()).isFalse();
    }

    @Test
    public void testGetRowIdLifetime() {
        assertThat(dataCloudDatabaseMetadata.getRowIdLifetime()).isNull();
    }

    @Test
    @SneakyThrows
    public void testGetSchemas() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetMock.getString("TABLE_SCHEM")).thenReturn(null);
        Mockito.when(resultSetMock.getString("TABLE_CATALOG")).thenReturn(null);

        ResultSet resultSet = dataCloudDatabaseMetadata.getSchemas();
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_SCHEMA_METADATA_COLUMNS);

        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_CATALOG");
        while (resultSet.next()) {
            assertThat(resultSet.getString("TABLE_SCHEM")).isEqualTo(null);
            assertThat(resultSet.getString("TABLE_CATALOG")).isEqualTo(null);
        }

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetSchemasCatalogAndSchemaPattern() {
        String schemaPattern = "public";
        String tableCatalog = "catalog";
        Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.getString("TABLE_SCHEM")).thenReturn(schemaPattern);
        Mockito.when(resultSetMock.getString("TABLE_CATALOG")).thenReturn(tableCatalog);

        ResultSet resultSet = dataCloudDatabaseMetadata.getSchemas(null, "schemaName");
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_SCHEMA_METADATA_COLUMNS);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_CATALOG");
        while (resultSet.next()) {
            assertThat(resultSet.getString("TABLE_SCHEM")).isEqualTo(schemaPattern);
            assertThat(resultSet.getString("TABLE_CATALOG")).isEqualTo(tableCatalog);
        }

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetSchemasCatalogAndSchemaPatternNullValues() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetMock.getString("TABLE_SCHEM")).thenReturn(null);
        Mockito.when(resultSetMock.getString("TABLE_CATALOG")).thenReturn(null);

        ResultSet resultSet = dataCloudDatabaseMetadata.getSchemas(null, null);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_SCHEMA_METADATA_COLUMNS);

        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_CATALOG");
        while (resultSet.next()) {
            assertThat(resultSet.getString("TABLE_SCHEM")).isEqualTo(null);
            assertThat(resultSet.getString("TABLE_CATALOG")).isEqualTo(null);
        }

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
    }

    @Test
    @SneakyThrows
    public void testGetSchemasEmptyValues() {
        Mockito.when(dataCloudStatement.executeQuery(anyString())).thenReturn(resultSetMock);
        Mockito.when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetMock.getString("TABLE_SCHEM")).thenReturn(null);
        Mockito.when(resultSetMock.getString("TABLE_CATALOG")).thenReturn(null);

        ResultSet resultSet = dataCloudDatabaseMetadata.getSchemas(StringUtils.EMPTY, StringUtils.EMPTY);
        assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(NUM_SCHEMA_METADATA_COLUMNS);
        assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("TABLE_SCHEM");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("TABLE_CATALOG");
        while (resultSet.next()) {
            assertThat(resultSet.getString("TABLE_SCHEM")).isEqualTo(null);
            assertThat(resultSet.getString("TABLE_CATALOG")).isEqualTo(null);
        }

        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("TEXT");
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("TEXT");
    }

    @Test
    public void testSupportsStoredFunctionsUsingCallSyntax() {
        assertThat(dataCloudDatabaseMetadata.supportsStoredFunctionsUsingCallSyntax())
                .isFalse();
    }

    @Test
    public void testAutoCommitFailureClosesAllResultSets() {
        assertThat(dataCloudDatabaseMetadata.autoCommitFailureClosesAllResultSets())
                .isFalse();
    }

    @Test
    public void testGetClientInfoProperties() {
        assertThat(dataCloudDatabaseMetadata.getClientInfoProperties()).isNull();
    }

    @Test
    public void testGetFunctions() {
        assertThat(dataCloudDatabaseMetadata.getFunctions(StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGetFunctionColumns() {
        assertThat(dataCloudDatabaseMetadata.getFunctionColumns(
                        StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGetPseudoColumns() {
        assertThat(dataCloudDatabaseMetadata.getPseudoColumns(
                        StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY))
                .isNull();
    }

    @Test
    public void testGeneratedKeyAlwaysReturned() {
        assertThat(dataCloudDatabaseMetadata.generatedKeyAlwaysReturned()).isFalse();
    }

    @Test
    public void testUnwrap() {
        try {
            assertThat(dataCloudDatabaseMetadata.unwrap(DataCloudDatabaseMetadata.class))
                    .isInstanceOf(DataCloudDatabaseMetadata.class);
        } catch (Exception e) {
            fail("Uncaught Exception", e);
        }
        val ex = assertThrows(DataCloudJDBCException.class, () -> dataCloudDatabaseMetadata.unwrap(String.class));
    }

    @Test
    public void testIsWrapperFor() {
        try {
            assertThat(dataCloudDatabaseMetadata.isWrapperFor(DataCloudDatabaseMetadata.class))
                    .isTrue();
        } catch (Exception e) {
            fail("Uncaught Exception", e);
        }
    }

    @Test
    public void testQuoteStringLiteral() {
        String unescapedString = "unescaped";
        String actual = QueryMetadataUtil.quoteStringLiteral(unescapedString);
        assertThat(actual).isEqualTo("'unescaped'");
    }

    @Test
    public void testQuoteStringLiteralSingleQuote() {
        char singleQuote = '\'';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(singleQuote)))
                .isEqualTo("''''");
    }

    @Test
    public void testQuoteStringLiteralBackslash() {
        char backslash = '\\';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(backslash)))
                .isEqualTo("E'\\\\'");
    }

    @Test
    public void testQuoteStringLiteralNewline() {
        char newLine = '\n';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(newLine)))
                .isEqualTo("E'\\n'");
    }

    @Test
    public void testQuoteStringLiteralCarriageReturn() {
        char carriageReturn = '\r';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(carriageReturn)))
                .isEqualTo("E'\\r'");
    }

    @Test
    public void testQuoteStringLiteralTab() {
        char tab = '\t';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(tab))).isEqualTo("E'\\t'");
    }

    @Test
    public void testQuoteStringLiteralBackspace() {
        char backspace = '\b';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(backspace)))
                .isEqualTo("E'\\b'");
    }

    @Test
    public void testQuoteStringLiteralFormFeed() {
        char formFeed = '\f';
        assertThat(QueryMetadataUtil.quoteStringLiteral(String.valueOf(formFeed)))
                .isEqualTo("E'\\f'");
    }

    @SneakyThrows
    private void assertExpectedEmptyResultSet(ResultSet resultSet) {
        assertThat(resultSet).isNotNull();
        assertFalse(resultSet.next());
    }
}
