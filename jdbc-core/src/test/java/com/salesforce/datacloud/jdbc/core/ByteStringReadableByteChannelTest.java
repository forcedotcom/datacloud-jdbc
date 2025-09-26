/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.junit.jupiter.api.Test;

@Slf4j
class ByteStringReadableByteChannelTest {
    @Test
    @SneakyThrows
    void isOpenFollowsNioSemantics() {
        try (val channel = new ByteStringReadableByteChannel(empty())) {
            assertThat(channel.isOpen()).isTrue(); // Channel starts open regardless of data availability
            // Even with no data, channel remains open until explicitly closed
            assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(-1); // End-of-stream
            assertThat(channel.isOpen()).isTrue(); // Still open after end-of-stream

            channel.close(); // Explicitly close it
            assertThat(channel.isOpen()).isFalse(); // Now it should be closed
        }
    }

    @Test
    @SneakyThrows
    void isOpenDetectsIfIteratorHasRemaining() {
        try (val channel = new ByteStringReadableByteChannel(some())) {
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void readThrowsClosedChannelExceptionWhenClosed() {
        val channel = new ByteStringReadableByteChannel(some());
        channel.close();
        assertThat(channel.isOpen()).isFalse();

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1)))
                .isInstanceOf(java.nio.channels.ClosedChannelException.class);
    }

    @Test
    @SneakyThrows
    void readReturnsNegativeOneOnIteratorExhaustion() {
        try (val channel = new ByteStringReadableByteChannel(empty())) {
            assertThat(channel.read(ByteBuffer.allocateDirect(2))).isEqualTo(-1);
        }
    }

    @SneakyThrows
    @Test
    void readIsLazy() {
        val first = ByteBuffer.allocate(5);
        val second = ByteBuffer.allocate(5);
        val seen = new ArrayList<ByteString>();

        val iterator = infiniteStream().peek(seen::add).iterator();

        try (val channel = new ReadChannel(new ByteStringReadableByteChannel(iterator))) {
            channel.readFully(first);
            assertThat(seen).hasSize(5);
            channel.readFully(second);
            assertThat(seen).hasSize(10);

            assertThat(new String(first.array(), StandardCharsets.UTF_8)).isEqualTo("01234");
            assertThat(new String(second.array(), StandardCharsets.UTF_8)).isEqualTo("56789");
        }
    }

    private static Iterator<ByteString> some() {
        return infiniteStream().iterator();
    }

    private static Iterator<ByteString> empty() {
        return infiniteStream().limit(0).iterator();
    }

    private static Stream<ByteString> infiniteStream() {
        return Stream.iterate(0, i -> i + 1).map(i -> Integer.toString(i)).map(ByteString::copyFromUtf8);
    }
}
