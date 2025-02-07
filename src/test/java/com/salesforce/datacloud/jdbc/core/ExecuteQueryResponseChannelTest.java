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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@Slf4j
class ExecuteQueryResponseChannelTest {
    @Test
    void isNotEmptyDetectsEmpty() {
        val empty = ByteBuffer.allocateDirect(0);
        assertThat(ExecuteQueryResponseChannel.isNotEmpty(empty)).isFalse();
    }

    @Test
    void isNotEmptyDetectsNotEmpty() {
        val notEmpty = ByteBuffer.wrap("not empty".getBytes(StandardCharsets.UTF_8));
        assertThat(ExecuteQueryResponseChannel.isNotEmpty(notEmpty)).isTrue();
    }

    @Test
    @SneakyThrows
    void isOpenDetectsIfIteratorIsExhausted() {
        try (val channel = ExecuteQueryResponseChannel.of(empty())) {
            assertThat(channel.isOpen()).isFalse();
        }
    }

    @Test
    @SneakyThrows
    void isOpenDetectsIfIteratorHasRemaining() {
        try (val channel = ExecuteQueryResponseChannel.of(some())) {
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void readReturnsNegativeOneOnIteratorExhaustion() {
        try (val channel = ExecuteQueryResponseChannel.of(empty())) {
            assertThat(channel.read(ByteBuffer.allocateDirect(0))).isEqualTo(-1);
        }
    }

    @SneakyThrows
    @Test
    void readIsLazy() {
        val first = ByteBuffer.allocate(5);
        val second = ByteBuffer.allocate(5);
        val seen = new ArrayList<QueryResult>();

        val stream = infiniteStream().peek(seen::add);

        val channel = new ReadChannel(ExecuteQueryResponseChannel.of(stream));

        channel.readFully(first);
        assertThat(seen).hasSize(5);
        channel.readFully(second);
        assertThat(seen).hasSize(10);

        assertThat(new String(first.array(), StandardCharsets.UTF_8)).isEqualTo("01234");
        assertThat(new String(second.array(), StandardCharsets.UTF_8)).isEqualTo("56789");
    }

    private static Stream<QueryResult> some() {
        return infiniteStream();
    }

    private static Stream<QueryResult> empty() {
        return infiniteStream().limit(0);
    }

    private static Stream<QueryResult> infiniteStream() {
        return Stream.iterate(0, i -> i + 1)
                .map(i -> Integer.toString(i))
                .map(ExecuteQueryResponseChannelTest::toMessage);
    }

    private static QueryResult toMessage(String string) {
        val byteString = ByteString.copyFromUtf8(string);
        val binaryPart = QueryResultPartBinary.newBuilder().setData(byteString);
        return QueryResult.newBuilder().setBinaryPart(binaryPart).build();
    }
}
