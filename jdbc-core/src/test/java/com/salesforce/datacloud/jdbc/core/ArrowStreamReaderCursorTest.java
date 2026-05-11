/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.ZoneId;
import java.util.Collections;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
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

    @Test
    @SneakyThrows
    void closesTheReader() {
        val sut = new ArrowStreamReaderCursor(reader, ZoneId.systemDefault());
        sut.close();
        verify(reader, times(1)).close();
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

        val sut = new ArrowStreamReaderCursor(reader, ZoneId.systemDefault());
        IntStream.range(0, times + 1).forEach(i -> sut.next());

        verify(root, times(times + 1)).getRowCount();
        verify(reader, times(1)).loadNextBatch();
    }

    @Test
    @SneakyThrows
    void firstNextReturnsTrueWhenInitialBatchHasRows() {
        when(root.getRowCount()).thenReturn(1);
        when(reader.getVectorSchemaRoot()).thenReturn(root);

        val sut = new ArrowStreamReaderCursor(reader, ZoneId.systemDefault());

        assertThat(sut.next()).isTrue();
    }

    @Test
    @SneakyThrows
    void firstNextReturnsFalseWhenStreamHasNoBatches() {
        when(root.getRowCount()).thenReturn(0);
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        when(reader.loadNextBatch()).thenReturn(false);

        val sut = new ArrowStreamReaderCursor(reader, ZoneId.systemDefault());

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
            val sut = new ArrowStreamReaderCursor(streamReader, ZoneId.systemDefault());

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
            val sut = new ArrowStreamReaderCursor(streamReader, ZoneId.systemDefault());
            assertThat(sut.next()).isFalse();
        }
    }
}
