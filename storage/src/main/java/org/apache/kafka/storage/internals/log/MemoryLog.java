/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Simple in-memory log implementation without segments.
 * For testing purposes only - no persistence, no file I/O.
 * Implements only the core DataLog interface, not the file-specific FileLog interface.
 */
public class MemoryLog implements DataLog {

    private final TopicPartition topicPartition;
    private LogConfig config;
    private final Time time;
    private final Scheduler scheduler;

    // Core storage: list of all record batches
    private final List<RecordBatch> batches = new ArrayList<>();

    // Offset to batch index mapping for fast lookups
    private final NavigableMap<Long, Integer> offsetIndex = new ConcurrentSkipListMap<>();

    // Current log end offset
    private volatile long logEndOffset;

    // Metadata
    private volatile long recoveryPoint;
    private volatile long lastFlushTime;

    public MemoryLog(TopicPartition topicPartition,
                     LogConfig config,
                     Time time,
                     Scheduler scheduler,
                     long initialOffset) {
        this.topicPartition = topicPartition;
        this.config = config;
        this.time = time;
        this.scheduler = scheduler;
        this.logEndOffset = initialOffset;
        this.recoveryPoint = initialOffset;
        this.lastFlushTime = time.milliseconds();
    }

    // ========== Core Operations ==========

    @Override
    public synchronized void append(long lastOffset, MemoryRecords records) {
        int batchIndex = batches.size();

        for (RecordBatch batch : records.batches()) {
            batches.add(batch);
            // Index the first offset of this batch
            offsetIndex.put(batch.baseOffset(), batchIndex++);
        }

        logEndOffset = lastOffset + 1;
    }

    @Override
    public synchronized FetchDataInfo read(long startOffset,
                                           int maxLength,
                                           boolean minOneMessage,
                                           LogOffsetMetadata maxOffsetMetadata,
                                           boolean includeAbortedTxns) {
        // Find the batch containing or after startOffset
        var entry = offsetIndex.floorEntry(startOffset);

        if (entry == null || startOffset >= logEndOffset) {
            return new FetchDataInfo(
                new LogOffsetMetadata(startOffset),
                MemoryRecords.EMPTY,
                false,
                includeAbortedTxns ? Optional.of(Collections.emptyList()) : Optional.empty()
            );
        }

        // Collect batches up to maxLength
        ByteBuffer buffer = ByteBuffer.allocate(maxLength);
        int startIdx = entry.getValue();
        boolean firstBatch = true;

        for (int i = startIdx; i < batches.size() && buffer.hasRemaining(); i++) {
            RecordBatch batch = batches.get(i);

            // Skip batches before startOffset
            if (batch.lastOffset() < startOffset) {
                continue;
            }

            int batchSize = batch.sizeInBytes();

            // Check if batch fits
            if (!firstBatch && batchSize > buffer.remaining()) {
                break;
            }

            // Ensure we honor minOneMessage constraint
            if (firstBatch && batchSize > maxLength && minOneMessage) {
                buffer = ByteBuffer.allocate(batchSize);
            }

            // Write batch to buffer
            batch.writeTo(buffer);
            firstBatch = false;
        }

        buffer.flip();
        MemoryRecords fetchedRecords = MemoryRecords.readableRecords(buffer);

        return new FetchDataInfo(
            new LogOffsetMetadata(startOffset),
            fetchedRecords,
            false,
            includeAbortedTxns ? Optional.of(Collections.emptyList()) : Optional.empty()
        );
    }

    @Override
    public void flush(long offset) {
        // No-op for in-memory (no files to flush)
        lastFlushTime = time.milliseconds();
    }

    @Override
    public void close() {
        // No resources to close for in-memory
    }

    // ========== Metadata ==========

    @Override
    public long logEndOffset() {
        return logEndOffset;
    }

    @Override
    public LogOffsetMetadata logEndOffsetMetadata() {
        return new LogOffsetMetadata(logEndOffset);
    }

    @Override
    public long recoveryPoint() {
        return recoveryPoint;
    }

    @Override
    public void updateRecoveryPoint(long offset) {
        this.recoveryPoint = offset;
    }

    @Override
    public long lastFlushTime() {
        return lastFlushTime;
    }

    @Override
    public long unflushedMessages() {
        // All messages are always "flushed" in memory
        return 0;
    }

    @Override
    public void markFlushed(long offset) {
        // No-op for in-memory
    }

    // ========== Configuration ==========

    @Override
    public LogConfig config() {
        return config;
    }

    @Override
    public void updateConfig(LogConfig config) {
        this.config = config;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public String name() {
        return topicPartition.toString();
    }

    @Override
    public boolean isFuture() {
        return false;
    }
}
