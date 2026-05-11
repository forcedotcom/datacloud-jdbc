/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.val;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link MetadataResultSets#of} arity contract: rows must match the schema column
 * count; null rows are allowed as the all-nulls shape (matching the legacy {@code coerceRows}
 * convention). Generic JDBC {@link java.sql.ResultSet} shape (closeable, forward-only,
 * holdability, etc.) is exercised by {@code StreamingResultSetMethodTest} since metadata
 * result sets share the {@link com.salesforce.datacloud.jdbc.core.StreamingResultSet} plumbing.
 */
class MetadataResultSetsTest {

    private static final List<ColumnMetadata> THREE_COLUMNS = Arrays.asList(
            new ColumnMetadata("a", HyperType.varcharUnlimited(true)),
            new ColumnMetadata("b", HyperType.int32(true)),
            new ColumnMetadata("c", HyperType.bool(true)));

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
}
