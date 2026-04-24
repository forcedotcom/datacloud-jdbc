/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamingResultSetMethodTest {

    @RegisterExtension
    static RootAllocatorTestExtension ext = new RootAllocatorTestExtension();

    private static final String QUERY_ID = "test-query-id";

    @SneakyThrows
    private StreamingResultSet createResultSet() {
        // Build a single-row VARCHAR batch, serialise to IPC bytes, and wrap in an
        // ArrowStreamReader. Using a fresh RootAllocator so the result set owns its own
        // allocator lifecycle (independent of the shared test extension allocator).
        val writeAllocator = ext.getRootAllocator();
        val vector = new VarCharVector("col1", writeAllocator);
        vector.allocateNew();
        vector.set(0, "hello".getBytes(StandardCharsets.UTF_8));
        vector.setValueCount(1);

        val out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(vector.getField()), Arrays.asList(vector))) {
            root.setRowCount(1);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }

        RootAllocator readerAllocator = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), readerAllocator);
        return StreamingResultSet.of(reader, readerAllocator, QUERY_ID);
    }

    // --- Unsupported methods ---

    @FunctionalInterface
    interface ResultSetMethod {
        void invoke(StreamingResultSet rs) throws SQLException;
    }

    static Stream<Arguments> unsupportedMethods() {
        return Stream.of(
                Arguments.of(Named.of("getAsciiStream", (ResultSetMethod) rs -> rs.getAsciiStream(1))),
                Arguments.of(Named.of("getUnicodeStream", (ResultSetMethod) rs -> rs.getUnicodeStream(1))),
                Arguments.of(Named.of("getBinaryStream", (ResultSetMethod) rs -> rs.getBinaryStream(1))),
                Arguments.of(Named.of("getCharacterStream", (ResultSetMethod) rs -> rs.getCharacterStream(1))),
                Arguments.of(Named.of("getRef", (ResultSetMethod) rs -> rs.getRef(1))),
                Arguments.of(Named.of("getBlob", (ResultSetMethod) rs -> rs.getBlob(1))),
                Arguments.of(Named.of("getClob", (ResultSetMethod) rs -> rs.getClob(1))),
                Arguments.of(Named.of("getStruct", (ResultSetMethod) rs -> rs.getStruct(1))),
                Arguments.of(Named.of("getURL", (ResultSetMethod) rs -> rs.getURL(1))),
                Arguments.of(Named.of("getRowId", (ResultSetMethod) rs -> rs.getRowId(1))),
                Arguments.of(Named.of("getSQLXML", (ResultSetMethod) rs -> rs.getSQLXML(1))),
                Arguments.of(Named.of("getNString", (ResultSetMethod) rs -> rs.getNString(1))),
                Arguments.of(Named.of("getNCharacterStream", (ResultSetMethod) rs -> rs.getNCharacterStream(1))),
                Arguments.of(Named.of("getCursorName", (ResultSetMethod) StreamingResultSet::getCursorName)));
    }

    @ParameterizedTest
    @MethodSource("unsupportedMethods")
    void unsupportedMethodThrows(ResultSetMethod method) throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            assertThatThrownBy(() -> method.invoke(rs)).isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }

    // --- Column index bounds ---

    @Test
    void getAccessorThrowsOnZeroIndex() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            assertThatThrownBy(() -> rs.getString(0))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("out of bounds");
        }
    }

    @Test
    void getAccessorThrowsOnTooLargeIndex() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            assertThatThrownBy(() -> rs.getString(99))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("out of bounds");
        }
    }

    // --- Lifecycle and navigation ---

    @Test
    void closeAndIsClosed() throws Exception {
        val rs = createResultSet();
        assertThat(rs.isClosed()).isFalse();
        rs.close();
        assertThat(rs.isClosed()).isTrue();
        // double close is a no-op
        rs.close();
        assertThat(rs.isClosed()).isTrue();
    }

    @Test
    void methodsThrowAfterClose() throws Exception {
        val rs = createResultSet();
        rs.close();

        assertThatThrownBy(rs::next).isInstanceOf(SQLException.class).hasMessageContaining("closed");
        assertThatThrownBy(rs::getRow).isInstanceOf(SQLException.class).hasMessageContaining("closed");
        assertThatThrownBy(rs::getMetaData).isInstanceOf(SQLException.class).hasMessageContaining("closed");
        assertThatThrownBy(rs::wasNull).isInstanceOf(SQLException.class).hasMessageContaining("closed");
        assertThatThrownBy(() -> rs.getString(1))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> rs.findColumn("col1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void queryId() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.getQueryId()).isEqualTo(QUERY_ID);
        }
    }

    @Test
    void getRowReturnsRowsSeen() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.getRow()).isEqualTo(0);
            rs.next();
            assertThat(rs.getRow()).isEqualTo(1);
        }
    }

    @Test
    void nextReturnsFalseWhenExhausted() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    // --- Metadata and column lookup ---

    @Test
    void getMetaData() throws Exception {
        try (val rs = createResultSet()) {
            val meta = rs.getMetaData();
            assertThat(meta.getColumnCount()).isEqualTo(1);
            assertThat(meta.getColumnLabel(1)).isEqualTo("col1");
        }
    }

    @Test
    void findColumn() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.findColumn("col1")).isEqualTo(1);
            assertThatThrownBy(() -> rs.findColumn("nonexistent")).isInstanceOf(SQLException.class);
        }
    }

    // --- wasNull tracking ---

    @Test
    void wasNullIsFalseAfterNonNullValue() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            val value = rs.getString(1);
            assertThat(value).isEqualTo("hello");
            assertThat(rs.wasNull()).isFalse();
        }
    }

    // --- Getters that work on VarChar ---

    @Test
    void getStringReturnsValue() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("hello");
        }
    }

    @Test
    void getObjectReturnsValue() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            assertThat((String) rs.getObject(1)).isEqualTo("hello");
        }
    }

    @Test
    void getBytesReturnsValue() throws Exception {
        try (val rs = createResultSet()) {
            rs.next();
            val bytes = rs.getBytes(1);
            assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("hello");
        }
    }

    // --- Miscellaneous ---

    @Test
    void getStatementReturnsNull() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.getStatement()).isNull();
        }
    }

    @Test
    void getHoldability() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        }
    }

    @Test
    void fetchSizeIsNoOp() throws Exception {
        try (val rs = createResultSet()) {
            rs.setFetchSize(100);
            assertThat(rs.getFetchSize()).isEqualTo(0);
        }
    }

    @Test
    void getWarningsReturnsNull() throws Exception {
        try (val rs = createResultSet()) {
            assertThat((Object) rs.getWarnings()).isNull();
            rs.clearWarnings(); // no-op, should not throw
            assertThat((Object) rs.getWarnings()).isNull();
        }
    }

    @Test
    void unwrapAndIsWrapperFor() throws Exception {
        try (val rs = createResultSet()) {
            assertThat(rs.isWrapperFor(StreamingResultSet.class)).isTrue();
            assertThat(rs.isWrapperFor(DataCloudResultSet.class)).isTrue();
            assertThat(rs.isWrapperFor(String.class)).isFalse();

            assertThat(rs.unwrap(StreamingResultSet.class)).isSameAs(rs);
            assertThatThrownBy(() -> rs.unwrap(String.class))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Cannot unwrap");
        }
    }
}
