/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ArrowStreamReaderCursorTest {
    @Mock
    protected ArrowStreamReader reader;

    @Mock
    protected VectorSchemaRoot root;

    @Mock
    protected BufferAllocator allocator;

    @Test
    @SneakyThrows
    void closesReaderAndAllocator() {
        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());
        sut.close();
        // Reader-before-allocator ordering is load-bearing: reader.close releases the buffers
        // accounted against the allocator so the allocator's closing budget check passes.
        // Reversing the order would trip the leak detector at runtime.
        val order = inOrder(reader, allocator);
        order.verify(reader).close();
        order.verify(allocator).close();
    }

    /**
     * When both reader.close() and allocator.close() throw, the cursor must close the allocator
     * even after the reader's close raised, and surface the reader's exception as primary with
     * the allocator's exception attached as suppressed. The reader exception is the
     * diagnostically interesting one (the leak detector firing on allocator.close is usually a
     * symptom of the reader's failure to release buffers); plain try/finally would silently
     * replace it with the allocator exception.
     */
    @Test
    @SneakyThrows
    void closeAttachesAllocatorErrorAsSuppressedWhenReaderCloseAlsoThrows() {
        val readerError = new IOException("reader close failed");
        val allocatorError = new IllegalStateException("allocator leak detected");
        doThrow(readerError).when(reader).close();
        doThrow(allocatorError).when(allocator).close();

        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());

        assertThatThrownBy(sut::close).isSameAs(readerError).hasSuppressedException(allocatorError);
        verify(reader, times(1)).close();
        verify(allocator, times(1)).close();
    }

    @Test
    @SneakyThrows
    void incrementsInternalIndexUntilRowsExhaustedThenLoadsNextBatch() {
        val times = 5;
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        // First batch has rows; loadNextBatch is consulted only after the per-batch index is
        // exhausted. Return false on that single call so the cursor terminates rather than
        // looping forever inside loadNextNonEmptyBatch.
        when(reader.loadNextBatch()).thenReturn(false);
        when(root.getRowCount()).thenReturn(times);

        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());
        IntStream.range(0, times + 1).forEach(i -> sut.next());

        // Each next() inspects rowCount once on the per-batch index check. loadNextNonEmptyBatch
        // is reached on the (times+1)-th call but only inspects rowCount inside its loop body if
        // loadNextBatch returns true; here it returns false, so getRowCount is observed times+1
        // times in total.
        verify(root, times(times + 1)).getRowCount();
        verify(reader, times(1)).loadNextBatch();
    }

    @Test
    @SneakyThrows
    void firstNextReturnsTrueWhenInitialBatchHasRows() {
        when(root.getRowCount()).thenReturn(1);
        when(reader.getVectorSchemaRoot()).thenReturn(root);

        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());

        assertThat(sut.next()).isTrue();
    }

    @Test
    @SneakyThrows
    void firstNextReturnsFalseWhenStreamHasNoBatches() {
        when(root.getRowCount()).thenReturn(0);
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        when(reader.loadNextBatch()).thenReturn(false);

        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());

        assertThat(sut.next()).isFalse();
    }

    /**
     * Pin behavior on a real Arrow IPC stream that emits a zero-row batch followed by a non-empty
     * batch. The cursor must skip the zero-row batch (it is valid Arrow IPC, e.g. async queries
     * with empty initial chunks or schema-only metadata streams) rather than reporting a phantom
     * row, and then surface the actual data on the next call.
     */
    @Test
    @SneakyThrows
    void skipsZeroRowBatchAndYieldsSubsequentNonEmptyRows() {
        val field = new Field("a", new FieldType(true, new ArrowType.Int(32, true), null), null);
        val schema = new Schema(Collections.singletonList(field));

        byte[] ipc;
        try (RootAllocator writeAlloc = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot writeRoot = VectorSchemaRoot.create(schema, writeAlloc)) {
            try (val out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(writeRoot, null, out)) {
                writer.start();
                writeRoot.allocateNew();
                writeRoot.setRowCount(0);
                writer.writeBatch();
                writeRoot.allocateNew();
                ((org.apache.arrow.vector.IntVector) writeRoot.getVector("a")).setSafe(0, 7);
                writeRoot.setRowCount(1);
                writer.writeBatch();
                writer.end();
                ipc = out.toByteArray();
            }
        }

        try (RootAllocator readAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(ipc), readAlloc)) {
            val sut = new ArrowStreamReaderCursor(streamReader, readAlloc, ZoneId.systemDefault());

            assertThat(sut.next())
                    .as("skips zero-row batch, advances to row in second batch")
                    .isTrue();
            assertThat(((org.apache.arrow.vector.IntVector)
                                    streamReader.getVectorSchemaRoot().getVector("a"))
                            .get(0))
                    .isEqualTo(7);
            assertThat(sut.next()).as("only one real row across the stream").isFalse();
        }
    }

    /**
     * Pin behavior on a stream containing only a zero-row batch. The cursor must not report any
     * row.
     */
    @Test
    @SneakyThrows
    void zeroRowOnlyBatchYieldsNoRows() {
        val field = new Field("a", new FieldType(true, new ArrowType.Int(32, true), null), null);
        val schema = new Schema(Collections.singletonList(field));

        byte[] ipc;
        try (RootAllocator writeAlloc = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot writeRoot = VectorSchemaRoot.create(schema, writeAlloc)) {
            writeRoot.allocateNew();
            writeRoot.setRowCount(0);
            try (val out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(writeRoot, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
                ipc = out.toByteArray();
            }
        }

        try (RootAllocator readAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(ipc), readAlloc)) {
            val sut = new ArrowStreamReaderCursor(streamReader, readAlloc, ZoneId.systemDefault());
            assertThat(sut.next()).isFalse();
        }
    }
    // --- Position methods: isBeforeFirst / isAfterLast / isFirst ---

    /** Serialises one Arrow IPC batch per entry, each holding that many consecutive int rows. */
    @SneakyThrows
    private static byte[] ipcWithBatches(int... rowsPerBatch) {
        val field = new Field("a", new FieldType(true, new ArrowType.Int(32, true), null), null);
        val schema = new Schema(Collections.singletonList(field));
        try (RootAllocator writeAlloc = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot writeRoot = VectorSchemaRoot.create(schema, writeAlloc);
                val out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(writeRoot, null, out)) {
            writer.start();
            int next = 0;
            for (int rows : rowsPerBatch) {
                writeRoot.allocateNew();
                val vector = (org.apache.arrow.vector.IntVector) writeRoot.getVector("a");
                for (int i = 0; i < rows; i++) {
                    vector.setSafe(i, next++);
                }
                writeRoot.setRowCount(rows);
                writer.writeBatch();
            }
            writer.end();
            writer.close();
            return out.toByteArray();
        }
    }

    @SneakyThrows
    private static void withCursor(byte[] ipc, java.util.function.Consumer<ArrowStreamReaderCursor> body) {
        try (RootAllocator readAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(ipc), readAlloc)) {
            streamReader.getVectorSchemaRoot();
            body.accept(new ArrowStreamReaderCursor(streamReader, readAlloc, ZoneId.systemDefault()));
        }
    }

    @Test
    void positionMethodsOnEmptyStream() {
        withCursor(ipcWithBatches(), sut -> {
            assertThat(sut.isBeforeFirst()).as("no rows to be before").isFalse();
            assertThat(sut.isAfterLast()).isFalse();
            assertThat(sut.isFirst()).isFalse();
            assertThat(sut.next()).isFalse();
            assertThat(sut.isBeforeFirst()).isFalse();
            assertThat(sut.isAfterLast())
                    .as("empty result has no last row to be after")
                    .isFalse();
            assertThat(sut.getRowsSeen()).isZero();
        });
    }

    @Test
    void positionMethodsOnZeroRowBatchOnlyStream() {
        withCursor(ipcWithBatches(0, 0), sut -> {
            assertThat(sut.isBeforeFirst()).isFalse();
            assertThat(sut.next()).isFalse();
            assertThat(sut.isAfterLast()).isFalse();
        });
    }

    @Test
    void positionMethodsAcrossSingleRowLifecycle() {
        withCursor(ipcWithBatches(1), sut -> {
            assertThat(sut.isBeforeFirst()).isTrue();
            assertThat(sut.isAfterLast()).isFalse();
            assertThat(sut.isFirst()).isFalse();

            assertThat(sut.next()).isTrue();
            assertThat(sut.isBeforeFirst()).isFalse();
            assertThat(sut.isFirst()).isTrue();
            assertThat(sut.isAfterLast()).isFalse();

            assertThat(sut.next()).isFalse();
            assertThat(sut.isBeforeFirst()).isFalse();
            assertThat(sut.isFirst()).isFalse();
            assertThat(sut.isAfterLast()).isTrue();
        });
    }

    @Test
    void positionMethodsAcrossBatches() {
        withCursor(ipcWithBatches(2, 1), sut -> {
            assertThat(sut.isBeforeFirst()).isTrue();
            assertThat(sut.next()).isTrue();
            assertThat(sut.isFirst()).isTrue();
            assertThat(sut.next()).isTrue();
            assertThat(sut.isFirst()).isFalse();
            assertThat(sut.isAfterLast()).isFalse();
            assertThat(sut.next()).as("first row of the second batch").isTrue();
            assertThat(sut.isAfterLast()).isFalse();
            assertThat(sut.next()).isFalse();
            assertThat(sut.isAfterLast()).isTrue();
            assertThat(sut.getRowsSeen()).isEqualTo(3);
        });
    }

    @Test
    void positionMethodsSkipLeadingZeroRowBatch() {
        withCursor(ipcWithBatches(0, 2), sut -> {
            assertThat(sut.isBeforeFirst()).isTrue();
            assertThat(sut.next()).isTrue();
            assertThat(sut.next()).isTrue();
            assertThat(sut.next()).isFalse();
        });
    }

    /** Peeking must not consume or skip a row: repeated isBeforeFirst() calls still leave row 0 for next(). */
    @Test
    @SneakyThrows
    void isBeforeFirstPeekDoesNotConsumeFirstRow() {
        val ipc = ipcWithBatches(3);
        try (RootAllocator readAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(ipc), readAlloc)) {
            val sut = new ArrowStreamReaderCursor(streamReader, readAlloc, ZoneId.systemDefault());
            val accessors = sut.createAccessors();

            assertThat(sut.isBeforeFirst()).isTrue();
            assertThat(sut.isBeforeFirst()).isTrue();
            assertThat(sut.getRowsSeen()).as("peeking does not count as a row").isZero();

            for (int expected = 0; expected < 3; expected++) {
                assertThat(sut.next()).isTrue();
                assertThat(accessors.get(0).getInt()).isEqualTo(expected);
            }
            assertThat(sut.next()).isFalse();
        }
    }

    @Test
    @SneakyThrows
    void isBeforeFirstDoesNotReadStreamAgainAfterEndOfStream() {
        when(reader.loadNextBatch()).thenReturn(false);
        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());

        assertThat(sut.isBeforeFirst()).isFalse();
        assertThat(sut.isBeforeFirst()).isFalse();
        assertThat(sut.next()).isFalse();
        assertThat(sut.next()).isFalse();

        verify(reader, times(1)).loadNextBatch();
    }

    @Test
    @SneakyThrows
    void isBeforeFirstWrapsStreamFailureAsSqlException() {
        when(reader.loadNextBatch()).thenThrow(new IOException("stream broke"));
        val sut = new ArrowStreamReaderCursor(reader, allocator, ZoneId.systemDefault());

        assertThatThrownBy(sut::isBeforeFirst)
                .isInstanceOf(java.sql.SQLException.class)
                .hasMessageContaining("stream broke");
    }
}
