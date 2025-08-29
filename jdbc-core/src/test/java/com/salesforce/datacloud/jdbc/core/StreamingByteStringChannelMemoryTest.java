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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@Slf4j
class StreamingByteStringChannelMemoryTest {

    @Test
    void testMemoryAllocationPattern() throws Exception {
        val dataSize = 1024 * 1024; // 1MB per chunk
        val numChunks = 50;

        val testData = createTestData(numChunks, dataSize);
        log.warn("Created {} chunks of {} bytes each", numChunks, dataSize);

        warmupAndForceGC();

        val memoryBean = ManagementFactory.getMemoryMXBean();
        val allocatedBefore = memoryBean.getHeapMemoryUsage().getUsed();

        long totalBytesRead;
        try (val channel = StreamingByteStringChannel.ofResults(testData.iterator())) {
            totalBytesRead = 0;

            // Read all data in small chunks to simulate realistic usage
            ByteBuffer buffer = ByteBuffer.allocate(8192); // 8KB buffer
            int bytesRead;
            while ((bytesRead = channel.read(buffer)) != -1) {
                totalBytesRead += bytesRead;
                buffer.clear();
            }
        }

        forceGC();

        val allocatedAfter = memoryBean.getHeapMemoryUsage().getUsed();
        val memoryDelta = allocatedAfter - allocatedBefore;

        log.warn("Total bytes read: {}", totalBytesRead);
        log.warn("Memory before: {} MB", allocatedBefore / (1024.0 * 1024.0));
        log.warn("Memory after: {} MB", allocatedAfter / (1024.0 * 1024.0));
        log.warn("Memory delta: {} MB", memoryDelta / (1024.0 * 1024.0));

        // Verify we read all the data
        val expectedTotal = (long) numChunks * dataSize;
        assertThat(totalBytesRead).isEqualTo(expectedTotal);

        val memoryEfficiencyRatio = (double) memoryDelta / expectedTotal;
        log.warn("Memory efficiency ratio: {}", memoryEfficiencyRatio);

        // For zero-copy: ratio should be < 0.1 (10% overhead)
        // For copy-based: ratio might be 1.0+ (100%+ overhead)
        assertThat(memoryEfficiencyRatio).isLessThan(0.1);
    }

    @Test
    void testMemoryPressureWithGCMonitoring() throws Exception {
        int dataSize = 512 * 1024;
        int numChunks = 100;

        val gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        val gcCountBefore = gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                .sum();
        val gcTimeBefore = gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .sum();

        warmupAndForceGC();

        val testData = createTestData(numChunks, dataSize);
        long totalRead;
        try (val channel = StreamingByteStringChannel.ofResults(testData.iterator())) {

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            totalRead = 0;
            int bytesRead;

            while ((bytesRead = channel.read(buffer)) != -1) {
                totalRead += bytesRead;
                buffer.clear();

                if (totalRead % (10 * dataSize) == 0) {
                    Thread.sleep(1);
                }
            }
        }

        val gcCountAfter = gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                .sum();
        val gcTimeAfter = gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .sum();

        val gcCount = gcCountAfter - gcCountBefore;
        val gcTime = gcTimeAfter - gcTimeBefore;

        log.warn("Total data processed: {} MB", totalRead / (1024.0 * 1024.0));
        log.warn("GC collections: {}", gcCount);
        log.warn("GC time: {} ms", gcTime);
        log.warn("GC time per MB: {} ms/MB", gcTime / (totalRead / (1024.0 * 1024.0)));

        assertThat(totalRead).isEqualTo((long) numChunks * dataSize);

        double gcTimePerMB = gcTime / (totalRead / (1024.0 * 1024.0));
        assertThat(gcTimePerMB).isLessThan(50.0);
    }

    @Test
    void testVariableChunkSizeMemoryPattern() throws Exception {
        warmupAndForceGC();

        val variableData = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> {
                    int size = 1024 * i * i;
                    return createTestData(5, size);
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        log.warn("Created {} chunks with variable sizes", variableData.size());

        val memoryBean = ManagementFactory.getMemoryMXBean();
        val before = memoryBean.getHeapMemoryUsage().getUsed();

        long totalRead;
        try (val channel = StreamingByteStringChannel.ofResults(variableData.iterator())) {
            ByteBuffer buffer = ByteBuffer.allocate(1024); // Small buffer

            totalRead = 0;
            int bytesRead;
            while ((bytesRead = channel.read(buffer)) != -1) {
                totalRead += bytesRead;
                buffer.clear();
            }
        }

        forceGC();
        val after = memoryBean.getHeapMemoryUsage().getUsed();

        log.warn("Variable chunk test - Total read: {} KB", totalRead / 1024);
        log.warn("Memory delta: {} KB", (after - before) / 1024);

        assertThat(totalRead).isGreaterThan(0);
    }

    private List<QueryResult> createTestData(int numChunks, int chunkSize) {
        return IntStream.range(0, numChunks)
                .mapToObj(chunkIndex -> createQueryResult(chunkIndex, chunkSize))
                .collect(Collectors.toList());
    }

    private QueryResult createQueryResult(int chunkIndex, int chunkSize) {
        val data = new byte[chunkSize];
        IntStream.range(0, chunkSize).forEach(i -> data[i] = (byte) ((chunkIndex + i) % 256));

        val byteString = ByteString.copyFrom(data);
        val binaryPart = QueryResultPartBinary.newBuilder().setData(byteString).build();
        return QueryResult.newBuilder().setBinaryPart(binaryPart).build();
    }

    private void warmupAndForceGC() {
        IntStream.range(0, 3).forEach(i -> {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        // Create and discard some objects to stabilize memory
        IntStream.range(0, 100).mapToObj(i -> new byte[1024]).collect(Collectors.toList());

        forceGC();
    }

    @SneakyThrows
    private void forceGC() {
        System.gc();
        Thread.sleep(100);
        System.gc();
    }
}
