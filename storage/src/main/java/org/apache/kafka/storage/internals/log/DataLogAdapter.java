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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Adapter that wraps a DataLog implementation and provides FileLog compatibility.
 * This allows non-file-based DataLog implementations (like MemoryLog) to be used
 * with UnifiedLog, which expects FileLog operations.
 *
 * <p>Smart delegation strategy:</p>
 * <ul>
 *   <li>If the delegate is a FileLog: all methods are delegated directly to it</li>
 *   <li>If the delegate is DataLog only: core operations are delegated, file-specific
 *       operations return sensible defaults/no-ops</li>
 * </ul>
 */
public class DataLogAdapter implements FileLog {

    protected final DataLog delegate;
    private final boolean delegateIsFileLog;

    /**
     * Create an adapter wrapping the given DataLog implementation.
     *
     * @param delegate The DataLog implementation to wrap
     */
    public DataLogAdapter(DataLog delegate) {
        this.delegate = delegate;
        this.delegateIsFileLog = delegate instanceof FileLog;
    }

    /**
     * Get the underlying DataLog instance.
     *
     * @return The wrapped DataLog
     */
    public DataLog getDelegate() {
        return delegate;
    }

    // ========== Core Operations (delegated to DataLog) ==========

    @Override
    public void append(long lastOffset, MemoryRecords records) throws IOException {
        delegate.append(lastOffset, records);
    }

    @Override
    public FetchDataInfo read(long startOffset,
                              int maxLength,
                              boolean minOneMessage,
                              LogOffsetMetadata maxOffsetMetadata,
                              boolean includeAbortedTxns) throws IOException {
        return delegate.read(startOffset, maxLength, minOneMessage, maxOffsetMetadata, includeAbortedTxns);
    }

    @Override
    public void flush(long offset) throws IOException {
        delegate.flush(offset);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public long logEndOffset() {
        return delegate.logEndOffset();
    }

    @Override
    public LogOffsetMetadata logEndOffsetMetadata() {
        return delegate.logEndOffsetMetadata();
    }

    @Override
    public long recoveryPoint() {
        return delegate.recoveryPoint();
    }

    @Override
    public void updateRecoveryPoint(long offset) {
        delegate.updateRecoveryPoint(offset);
    }

    @Override
    public long lastFlushTime() {
        return delegate.lastFlushTime();
    }

    @Override
    public long unflushedMessages() {
        return delegate.unflushedMessages();
    }

    @Override
    public void markFlushed(long offset) {
        delegate.markFlushed(offset);
    }

    @Override
    public LogConfig config() {
        return delegate.config();
    }

    @Override
    public void updateConfig(LogConfig config) {
        delegate.updateConfig(config);
    }

    @Override
    public TopicPartition topicPartition() {
        return delegate.topicPartition();
    }

    @Override
    public Time time() {
        return delegate.time();
    }

    @Override
    public Scheduler scheduler() {
        return delegate.scheduler();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public boolean isFuture() {
        return delegate.isFuture();
    }

    // ========== FileLog-specific operations ==========

    /**
     * Roll operation - delegates to FileLog if available, otherwise returns null.
     */
    @Override
    public LogSegment roll(Long expectedNextOffset) {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).roll(expectedNextOffset);
        }
        return null;
    }

    /**
     * Get log segments - delegates to FileLog if available, otherwise returns empty segments.
     */
    @Override
    public LogSegments segments() {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).segments();
        }
        return new LogSegments(delegate.topicPartition());
    }

    /**
     * Collect aborted transactions - delegates to FileLog if available, otherwise returns empty list.
     */
    @Override
    public List<AbortedTxn> collectAbortedTransactions(long logStartOffset, long startOffset, long upperBoundOffset) {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).collectAbortedTransactions(logStartOffset, startOffset, upperBoundOffset);
        }
        return Collections.emptyList();
    }

    /**
     * Convert offset to metadata - delegates to FileLog if available, otherwise returns simple metadata.
     */
    @Override
    public LogOffsetMetadata convertToOffsetMetadataOrThrow(long offset) throws IOException {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).convertToOffsetMetadataOrThrow(offset);
        }
        return new LogOffsetMetadata(offset);
    }

    /**
     * Remove and delete segments - delegates to FileLog if available, otherwise no-op.
     */
    @Override
    public void removeAndDeleteSegments(Collection<LogSegment> segments,
                                        boolean asyncDelete,
                                        SegmentDeletionReason reason) throws IOException {
        if (delegateIsFileLog) {
            ((FileLog) delegate).removeAndDeleteSegments(segments, asyncDelete, reason);
        }
    }

    /**
     * Truncate to offset - delegates to FileLog if available, otherwise returns empty list.
     */
    @Override
    public Collection<LogSegment> truncateTo(long targetOffset) throws IOException {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).truncateTo(targetOffset);
        }
        return Collections.emptyList();
    }

    /**
     * Truncate fully and start at new offset - delegates to FileLog if available, otherwise returns empty list.
     */
    @Override
    public List<LogSegment> truncateFullyAndStartAt(long newOffset) {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).truncateFullyAndStartAt(newOffset);
        }
        return Collections.emptyList();
    }

    /**
     * Delete all segments - delegates to FileLog if available, otherwise returns empty list.
     */
    @Override
    public List<LogSegment> deleteAllSegments() throws IOException {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).deleteAllSegments();
        }
        return Collections.emptyList();
    }

    // ========== File Operations ==========

    /**
     * Get log directory - delegates to FileLog if available, otherwise returns null.
     */
    @Override
    public File dir() {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).dir();
        }
        return null;
    }

    /**
     * Get parent directory - delegates to FileLog if available, otherwise returns empty string.
     */
    @Override
    public String parentDir() {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).parentDir();
        }
        return "";
    }

    /**
     * Get parent directory file - delegates to FileLog if available, otherwise returns null.
     */
    @Override
    public File parentDirFile() {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).parentDirFile();
        }
        return null;
    }

    /**
     * Rename directory - delegates to FileLog if available, otherwise returns false.
     */
    @Override
    public boolean renameDir(String name) throws IOException {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).renameDir(name);
        }
        return false;
    }

    /**
     * Close file handlers - delegates to FileLog if available, otherwise no-op.
     */
    @Override
    public void closeHandlers() {
        if (delegateIsFileLog) {
            ((FileLog) delegate).closeHandlers();
        }
    }

    /**
     * Delete empty directory - delegates to FileLog if available, otherwise no-op.
     */
    @Override
    public void deleteEmptyDir() throws IOException {
        if (delegateIsFileLog) {
            ((FileLog) delegate).deleteEmptyDir();
        }
    }

    /**
     * Check if memory mapped buffer is closed - delegates to FileLog if available, otherwise no-op.
     */
    @Override
    public void checkIfMemoryMappedBufferClosed() {
        if (delegateIsFileLog) {
            ((FileLog) delegate).checkIfMemoryMappedBufferClosed();
        }
    }

    /**
     * Get log dir failure channel - delegates to FileLog if available, otherwise returns null.
     */
    @Override
    public LogDirFailureChannel logDirFailureChannel() {
        if (delegateIsFileLog) {
            return ((FileLog) delegate).logDirFailureChannel();
        }
        return null;
    }
}
