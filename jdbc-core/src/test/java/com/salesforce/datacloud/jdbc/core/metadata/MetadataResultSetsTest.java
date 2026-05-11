/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.datacloud.jdbc.core.MetadataSchemas;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MetadataResultSets}. Two slices:
 * <ul>
 *   <li><b>Arity contract</b> — rows must match the schema column count; null rows are allowed
 *       as the all-nulls shape (matching the legacy {@code coerceRows} convention).
 *   <li><b>JDBC ResultSet shape</b> — empty metadata result sets expose the standard JDBC shape
 *       (row=0, closeable, forward-only, holdability, etc.).
 * </ul>
 */
class MetadataResultSetsTest {

    private static final List<ColumnMetadata> THREE_COLUMNS = Arrays.asList(
            new ColumnMetadata("a", HyperType.varcharUnlimited(true)),
            new ColumnMetadata("b", HyperType.int32(true)),
            new ColumnMetadata("c", HyperType.bool(true)));

    // --- Arity contract ---

    @Test
    void shortRowRejected() {
        val rows = Collections.singletonList(Arrays.<Object>asList("only-one"));
        assertThatThrownBy(() -> MetadataResultSets.of(THREE_COLUMNS, rows))
                .hasMessageContaining("3 columns")
                .hasMessageContaining("1 elements");
    }

    @Test
    void longRowRejected() {
        val rows = Collections.singletonList(Arrays.<Object>asList("a", 1, true, "extra"));
        assertThatThrownBy(() -> MetadataResultSets.of(THREE_COLUMNS, rows))
                .hasMessageContaining("3 columns")
                .hasMessageContaining("4 elements");
    }

    @Test
    void rightArityAccepted() throws Exception {
        val rows = Collections.singletonList(Arrays.<Object>asList("a", 1, true));
        try (val rs = MetadataResultSets.of(THREE_COLUMNS, rows)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("a");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getBoolean(3)).isTrue();
        }
    }

    @Test
    void nullRowAcceptedAsAllNulls() throws Exception {
        val rows = Collections.<List<Object>>singletonList(null);
        try (val rs = MetadataResultSets.of(THREE_COLUMNS, rows)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNull();
            rs.getInt(2);
            assertThat(rs.wasNull()).isTrue();
            rs.getBoolean(3);
            assertThat(rs.wasNull()).isTrue();
        }
    }

    @Test
    void emptyRowsAccepted() throws Exception {
        try (val rs = MetadataResultSets.of(THREE_COLUMNS, Collections.emptyList())) {
            assertThat(rs.next()).isFalse();
        }
    }

    // --- JDBC ResultSet shape on an empty metadata result set ---

    private ResultSet emptyMetadataResultSet;

    @BeforeEach
    public void initEmptyMetadataResultSet() throws SQLException {
        emptyMetadataResultSet = MetadataResultSets.empty(MetadataSchemas.COLUMNS);
    }

    @Test
    void getRow() throws SQLException {
        assertThat(emptyMetadataResultSet.getRow()).isEqualTo(0);

        emptyMetadataResultSet.close();
        assertThrows(SQLException.class, () -> emptyMetadataResultSet.next());
    }

    @Test
    void next() throws SQLException {
        emptyMetadataResultSet.close();
        assertThrows(SQLException.class, () -> emptyMetadataResultSet.next());
    }

    @Test
    void isClosed() throws SQLException {
        assertFalse(emptyMetadataResultSet.isClosed());
        emptyMetadataResultSet.close();
        assertTrue(emptyMetadataResultSet.isClosed());
    }

    @Test
    void getStatement() throws SQLException {
        assertThat(emptyMetadataResultSet.getStatement()).isNull();
    }

    @Test
    void unwrap() {
        assertThrows(SQLException.class, () -> emptyMetadataResultSet.unwrap(ResultSetMetaData.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        // StreamingResultSet implements DataCloudResultSet / ResultSet; it is not a wrapper for
        // arbitrary unrelated types.
        assertThat(emptyMetadataResultSet.isWrapperFor(ResultSetMetaData.class)).isFalse();
    }

    @Test
    void getHoldability() throws SQLException {
        assertThat(emptyMetadataResultSet.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    void getFetchSize() throws SQLException {
        assertThat(emptyMetadataResultSet.getFetchSize()).isEqualTo(0);
    }

    @Test
    void setFetchSize() throws SQLException {
        // StreamingResultSet controls its own fetch size and ignores caller-supplied hints.
        emptyMetadataResultSet.setFetchSize(0);
    }

    @SneakyThrows
    @Test
    void getWarnings() {
        assertThat((Iterable<? extends Throwable>) emptyMetadataResultSet.getWarnings())
                .isNull();
    }

    @Test
    void getConcurrency() throws SQLException {
        assertThat(emptyMetadataResultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    void getType() throws SQLException {
        assertThat(emptyMetadataResultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    void getFetchDirection() throws SQLException {
        assertThat(emptyMetadataResultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
    }
}
