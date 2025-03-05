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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Assertions;
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

    @Test
    void createGetterIsUnsupported() {
        val sut = new ArrowStreamReaderCursor(reader);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> sut.createGetter(0));
    }

    @Test
    @SneakyThrows
    void closesTheReader() {
        val sut = new ArrowStreamReaderCursor(reader);
        sut.close();
        verify(reader, times(1)).close();
    }

    @Test
    @SneakyThrows
    void incrementsInternalIndexUntilRowsExhaustedThenLoadsNextBatch() {
        val times = 5;
        when(reader.getVectorSchemaRoot()).thenReturn(root);
        when(reader.loadNextBatch()).thenReturn(true);
        when(root.getRowCount()).thenReturn(times);

        val sut = new ArrowStreamReaderCursor(reader);
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

        val sut = new ArrowStreamReaderCursor(reader);

        assertThat(sut.next()).isEqualTo(result);
    }
}
