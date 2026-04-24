/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.datacloud.jdbc.core.metadata.MetadataResultSets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Smoke-test the Arrow-backed metadata path: empty metadata result sets expose the standard
 * JDBC shape (row=0, closeable, forward-only, holdability, etc.).
 */
class DataCloudMetadataResultSetTest {
    ResultSet metadataResultSet;

    @BeforeEach
    public void init() throws SQLException {
        metadataResultSet = MetadataResultSets.empty(MetadataSchemas.COLUMNS);
    }

    @Test
    void getRow() throws SQLException {
        assertThat(metadataResultSet.getRow()).isEqualTo(0);

        metadataResultSet.close();
        assertThrows(SQLException.class, () -> metadataResultSet.next());
    }

    @Test
    void next() throws SQLException {
        metadataResultSet.close();
        assertThrows(SQLException.class, () -> metadataResultSet.next());
    }

    @Test
    void isClosed() throws SQLException {
        assertFalse(metadataResultSet.isClosed());
        metadataResultSet.close();
        assertTrue(metadataResultSet.isClosed());
    }

    @Test
    void getStatement() throws SQLException {
        assertThat(metadataResultSet.getStatement()).isNull();
    }

    @Test
    void unwrap() throws SQLException {
        assertThrows(SQLException.class, () -> metadataResultSet.unwrap(ResultSetMetaData.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        // StreamingResultSet implements DataCloudResultSet / ResultSet; it is not a wrapper for
        // arbitrary unrelated types.
        assertThat(metadataResultSet.isWrapperFor(ResultSetMetaData.class)).isFalse();
    }

    @Test
    void getHoldability() throws SQLException {
        assertThat(metadataResultSet.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    void getFetchSize() throws SQLException {
        assertThat(metadataResultSet.getFetchSize()).isEqualTo(0);
    }

    @Test
    void setFetchSize() throws SQLException {
        // StreamingResultSet controls its own fetch size and ignores caller-supplied hints.
        metadataResultSet.setFetchSize(0);
    }

    @SneakyThrows
    @Test
    void getWarnings() {
        assertThat((Iterable<? extends Throwable>) metadataResultSet.getWarnings())
                .isNull();
    }

    @Test
    void getConcurrency() throws SQLException {
        assertThat(metadataResultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    void getType() throws SQLException {
        assertThat(metadataResultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    void getFetchDirection() throws SQLException {
        assertThat(metadataResultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
    }
}
