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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Interface for file-based log storage.
 * Extends DataLog with file-specific operations like segment management and file I/O.
 */
public interface FileLog extends DataLog {

    // ========== Local Log Operations ==========

    /**
     * Roll the log to a new active segment starting at the given offset
     */
    LogSegment roll(Long expectedNextOffset);

    // ========== Segments ==========

    /**
     * Get the log segments
     */
    LogSegments segments();

    // ========== Transaction Support ==========

    /**
     * Collect aborted transactions in the given offset range
     */
    List<AbortedTxn> collectAbortedTransactions(long logStartOffset, long startOffset, long upperBoundOffset);

    /**
     * Convert offset to offset metadata, throwing exception if not found
     */
    LogOffsetMetadata convertToOffsetMetadataOrThrow(long offset) throws IOException;

    // ========== Segment Management ==========

    /**
     * Remove and delete the given segments
     */
    void removeAndDeleteSegments(Collection<LogSegment> segments,
                                  boolean asyncDelete,
                                  SegmentDeletionReason reason) throws IOException;

    /**
     * Truncate the log to the given offset
     */
    Collection<LogSegment> truncateTo(long targetOffset) throws IOException;

    /**
     * Truncate the log fully and start at the given offset
     */
    List<LogSegment> truncateFullyAndStartAt(long newOffset);

    /**
     * Delete all segments
     */
    List<LogSegment> deleteAllSegments() throws IOException;

    // ========== File Operations (Optional for in-memory) ==========

    /**
     * Get the log directory
     */
    File dir();

    /**
     * Get the parent directory name
     */
    String parentDir();

    /**
     * Get the parent directory file
     */
    File parentDirFile();

    /**
     * Rename the log directory
     */
    boolean renameDir(String name) throws IOException;

    /**
     * Close file handlers
     */
    void closeHandlers();

    /**
     * Delete empty directory
     */
    void deleteEmptyDir() throws IOException;

    /**
     * Check if memory mapped buffer is closed
     */
    void checkIfMemoryMappedBufferClosed();

    /**
     * Get the log dir failure channel
     */
    LogDirFailureChannel logDirFailureChannel();


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
     * Check if this is a future log
     *
     * @return true if this is a future log, false otherwise
     */
    boolean isFuture();
}
