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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;

import java.io.IOException;

/**
 * Core interface for log data storage operations.
 * This interface defines the fundamental operations that any log implementation must support,
 * regardless of the underlying storage mechanism (file-based, in-memory, remote, etc.).
 */
public interface DataLog {

    // ========== Core Operations ==========

    /**
     * Append records to the log
     *
     * @param lastOffset The last offset in the records being appended
     * @param records The records to append
     * @throws IOException If an I/O error occurs
     */
    void append(long lastOffset, MemoryRecords records) throws IOException;

    /**
     * Read messages from the log starting at the given offset
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength The maximum number of bytes to read
     * @param minOneMessage If true, the first message will be returned even if it exceeds maxLength
     * @param maxOffsetMetadata The metadata of the maximum offset to be fetched
     * @param includeAbortedTxns If true, aborted transactions are included
     * @return The fetch data information including fetch starting offset metadata and messages read
     * @throws IOException If an I/O error occurs
     */
    FetchDataInfo read(long startOffset,
                       int maxLength,
                       boolean minOneMessage,
                       LogOffsetMetadata maxOffsetMetadata,
                       boolean includeAbortedTxns) throws IOException;

    /**
     * Flush all messages up to the given offset
     *
     * @param offset The offset to flush up to
     * @throws IOException If an I/O error occurs
     */
    void flush(long offset) throws IOException;

    /**
     * Close the log and release resources
     */
    void close();

    // ========== Offset Management ==========

    /**
     * Get the current log end offset
     *
     * @return The log end offset
     */
    long logEndOffset();

    /**
     * Get the log end offset metadata
     *
     * @return The log end offset metadata
     */
    LogOffsetMetadata logEndOffsetMetadata();

    /**
     * Get the recovery point offset
     *
     * @return The recovery point offset
     */
    long recoveryPoint();

    /**
     * Update the recovery point
     *
     * @param offset The new recovery point offset
     */
    void updateRecoveryPoint(long offset);

    /**
     * Get the last time the log was flushed
     *
     * @return The timestamp of the last flush
     */
    long lastFlushTime();

    /**
     * Get the number of unflushed messages
     *
     * @return The count of unflushed messages
     */
    long unflushedMessages();

    /**
     * Mark messages as flushed up to the given offset
     *
     * @param offset The offset up to which messages are flushed
     */
    void markFlushed(long offset);

    // ========== Configuration & Metadata ==========

    /**
     * Get the log configuration
     *
     * @return The log configuration
     */
    LogConfig config();

    /**
     * Update the log configuration
     *
     * @param config The new log configuration
     */
    void updateConfig(LogConfig config);

    /**
     * Get the topic partition this log belongs to
     *
     * @return The topic partition
     */
    TopicPartition topicPartition();

    /**
     * Get the time instance
     *
     * @return The time instance
     */
    Time time();

    /**
     * Get the scheduler
     *
     * @return The scheduler
     */
    Scheduler scheduler();

    /**
     * Get the log name
     *
     * @return The log name
     */
    String name();

    /**
     * Check if this is a future log
     *
     * @return true if this is a future log, false otherwise
     */
    boolean isFuture();
}
