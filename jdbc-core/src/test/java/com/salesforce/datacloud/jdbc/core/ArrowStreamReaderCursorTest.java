/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZoneId;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ArrowStreamReaderCursorTest {
    @Mock
    protected ArrowStreamReader reader;

    @Mock
    protected VectorSchemaRoot root;

    @Mock
    protected AutoCloseable ownedResources;

    @Test
    @SneakyThrows
    void closesOwnedResources() {
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        val sut = ArrowStreamReaderCursor.streaming(reader, ownedResources, ZoneId.systemDefault());
        sut.close();
        verify(ownedResources, times(1)).close();
    }

    @Test
    @SneakyThrows
    void incrementsInternalIndexUntilRowsExhaustedThenLoadsNextBatch() {
        val times = 5;
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        when(reader.loadNextBatch()).thenReturn(true);
        when(root.getRowCount()).thenReturn(times);

        val sut = ArrowStreamReaderCursor.streaming(reader, ownedResources, ZoneId.systemDefault());
        IntStream.range(0, times + 1).forEach(i -> sut.next());

        verify(root, times(times + 1)).getRowCount();
        verify(reader, times(1)).loadNextBatch();
    }

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(booleans = {true, false})
    void forwardsLoadNextBatch(boolean result) {
        when(root.getRowCount()).thenReturn(-10);
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        when(reader.loadNextBatch()).thenReturn(result);

        val sut = ArrowStreamReaderCursor.streaming(reader, ownedResources, ZoneId.systemDefault());

        assertThat(sut.next()).isEqualTo(result);
    }

    @Test
    @SneakyThrows
    void inMemoryCursorReportsFalseWhenRowCountExhausted() {
        when(root.getRowCount()).thenReturn(2);

        val sut = ArrowStreamReaderCursor.inMemory(root, ownedResources, ZoneId.systemDefault());

        assertThat(sut.next()).isTrue();
        assertThat(sut.next()).isTrue();
        assertThat(sut.next()).isFalse();
    }
}
