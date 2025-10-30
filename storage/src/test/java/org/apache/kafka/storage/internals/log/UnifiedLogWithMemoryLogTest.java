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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for UnifiedLog using MemoryLog as backing storage instead of file-based LocalLog.
 * Demonstrates that UnifiedLog can work with different DataLog implementations through
 * the DataLogAdapter pattern.
 */
public class UnifiedLogWithMemoryLogTest {

    private static final MockTime MOCK_TIME = new MockTime();
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("test-topic", 0);
    private static final long INITIAL_OFFSET = 0L;

    private UnifiedLog unifiedLog;
    private LogConfig logConfig;
    private BrokerTopicStats brokerTopicStats;

    @BeforeEach
    public void setUp() {
        logConfig = new LogConfig(new Properties());
        brokerTopicStats = new BrokerTopicStats();
        unifiedLog = createUnifiedLogWithMemoryLog(
                TOPIC_PARTITION,
                logConfig,
                MOCK_TIME,
                INITIAL_OFFSET,
                brokerTopicStats
        );
    }

    @AfterEach
    public void tearDown() {
        if (unifiedLog != null) {
            unifiedLog.close();
        }
        if (brokerTopicStats != null) {
            brokerTopicStats.close();
        }
    }

    @Test
    public void testAppendAsLeaderAndRead() throws IOException {
        // Create test records
        List<SimpleRecord> recordList = new ArrayList<>();
        recordList.add(new SimpleRecord(MOCK_TIME.milliseconds(), "key1".getBytes(), "value1".getBytes()));
        recordList.add(new SimpleRecord(MOCK_TIME.milliseconds(), "key2".getBytes(), "value2".getBytes()));
        recordList.add(new SimpleRecord(MOCK_TIME.milliseconds(), "key3".getBytes(), "value3".getBytes()));

        // Append records as leader
        MemoryRecords records = MemoryRecords.withRecords(
                INITIAL_OFFSET,
                Compression.NONE,
                0,
                recordList.toArray(new SimpleRecord[0])
        );

        LogAppendInfo appendInfo = unifiedLog.appendAsLeader(records, 0);

        // Verify append
        assertEquals(INITIAL_OFFSET, appendInfo.firstOffset());
        assertEquals(INITIAL_OFFSET + recordList.size() - 1, appendInfo.lastOffset());

        // Update high watermark so we can read
        unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata());

        // Read records
        FetchDataInfo fetchDataInfo = unifiedLog.read(
                INITIAL_OFFSET,
                Integer.MAX_VALUE,
                FetchIsolation.HIGH_WATERMARK,
                false
        );

        assertNotNull(fetchDataInfo);
        assertNotNull(fetchDataInfo.records);

        // Verify read records
        List<Record> readRecords = new ArrayList<>();
        for (Record record : fetchDataInfo.records.records()) {
            readRecords.add(record);
        }

        assertEquals(recordList.size(), readRecords.size());
        for (int i = 0; i < recordList.size(); i++) {
            SimpleRecord expected = recordList.get(i);
            Record actual = readRecords.get(i);

            byte[] expectedKey = new byte[expected.key().remaining()];
            expected.key().duplicate().get(expectedKey);
            byte[] actualKey = new byte[actual.key().remaining()];
            actual.key().duplicate().get(actualKey);

            byte[] expectedValue = new byte[expected.value().remaining()];
            expected.value().duplicate().get(expectedValue);
            byte[] actualValue = new byte[actual.value().remaining()];
            actual.value().duplicate().get(actualValue);

            assertEquals(
                    new String(expectedKey, StandardCharsets.UTF_8),
                    new String(actualKey, StandardCharsets.UTF_8)
            );
            assertEquals(
                    new String(expectedValue, StandardCharsets.UTF_8),
                    new String(actualValue, StandardCharsets.UTF_8)
            );
        }
    }

    @Test
    public void testMultipleAppendsAndReads() throws IOException {
        // First append - UnifiedLog will assign offsets starting from INITIAL_OFFSET
        SimpleRecord record1 = new SimpleRecord("key1".getBytes(), "value1".getBytes());
        MemoryRecords batch1 = MemoryRecords.withRecords(0L, Compression.NONE, 0, record1);
        LogAppendInfo appendInfo1 = unifiedLog.appendAsLeader(batch1, 0);

        assertEquals(INITIAL_OFFSET, appendInfo1.firstOffset());
        assertEquals(INITIAL_OFFSET, appendInfo1.lastOffset());
        assertEquals(INITIAL_OFFSET + 1, unifiedLog.logEndOffset());

        // Second append - UnifiedLog will assign the next offset
        SimpleRecord record2 = new SimpleRecord("key2".getBytes(), "value2".getBytes());
        MemoryRecords batch2 = MemoryRecords.withRecords(0L, Compression.NONE, 0, record2);
        LogAppendInfo appendInfo2 = unifiedLog.appendAsLeader(batch2, 0);

        assertEquals(INITIAL_OFFSET + 1, appendInfo2.firstOffset());
        assertEquals(INITIAL_OFFSET + 1, appendInfo2.lastOffset());
        assertEquals(INITIAL_OFFSET + 2, unifiedLog.logEndOffset());

        // Update high watermark
        unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata());

        // Read all records
        FetchDataInfo fetchDataInfo = unifiedLog.read(
                INITIAL_OFFSET,
                Integer.MAX_VALUE,
                FetchIsolation.HIGH_WATERMARK,
                false
        );

        List<Record> readRecords = new ArrayList<>();
        for (Record record : fetchDataInfo.records.records()) {
            readRecords.add(record);
        }

        assertEquals(2, readRecords.size());
    }

    @Test
    public void testLogEndOffset() throws IOException {
        // Initially, log end offset should be at initial offset
        assertEquals(INITIAL_OFFSET, unifiedLog.logEndOffset());

        // Append some records
        SimpleRecord record = new SimpleRecord("key".getBytes(), "value".getBytes());
        MemoryRecords batch = MemoryRecords.withRecords(INITIAL_OFFSET, Compression.NONE, 0, record);
        unifiedLog.appendAsLeader(batch, 0);

        // Log end offset should be incremented
        assertEquals(INITIAL_OFFSET + 1, unifiedLog.logEndOffset());
    }

    @Test
    public void testFlush() throws IOException {
        // Append records
        SimpleRecord record = new SimpleRecord("key".getBytes(), "value".getBytes());
        MemoryRecords batch = MemoryRecords.withRecords(INITIAL_OFFSET, Compression.NONE, 0, record);
        unifiedLog.appendAsLeader(batch, 0);

        // Flush should work without errors (no-op for MemoryLog)
        unifiedLog.flush(false);

        // Verify data is still readable after flush
        unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata());
        FetchDataInfo fetchDataInfo = unifiedLog.read(
                INITIAL_OFFSET,
                Integer.MAX_VALUE,
                FetchIsolation.HIGH_WATERMARK,
                false
        );

        assertNotNull(fetchDataInfo);
        assertNotNull(fetchDataInfo.records);
        assertTrue(fetchDataInfo.records.records().iterator().hasNext());
    }

    @Test
    public void testHighWatermark() throws IOException {
        // Initially, high watermark should be at initial offset
        assertEquals(INITIAL_OFFSET, unifiedLog.highWatermark());

        // Append some records
        SimpleRecord record = new SimpleRecord("key".getBytes(), "value".getBytes());
        MemoryRecords batch = MemoryRecords.withRecords(INITIAL_OFFSET, Compression.NONE, 0, record);
        unifiedLog.appendAsLeader(batch, 0);

        // High watermark should not change until explicitly updated
        assertEquals(INITIAL_OFFSET, unifiedLog.highWatermark());

        // Update high watermark
        unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata());
        assertEquals(INITIAL_OFFSET + 1, unifiedLog.highWatermark());
    }

    @Test
    public void testClose() throws IOException {
        // Append some data
        SimpleRecord record = new SimpleRecord("key".getBytes(), "value".getBytes());
        MemoryRecords batch = MemoryRecords.withRecords(INITIAL_OFFSET, Compression.NONE, 0, record);
        unifiedLog.appendAsLeader(batch, 0);

        // Close should work without errors
        unifiedLog.close();

        // Mark as null to avoid double-close in tearDown
        unifiedLog = null;
    }

    /**
     * Helper method to create a UnifiedLog with MemoryLog as backing storage.
     * This demonstrates how to configure UnifiedLog to use in-memory storage
     * instead of file-based LocalLog for easier testing.
     *
     * @param topicPartition The topic partition
     * @param config The log configuration
     * @param time The time instance
     * @param initialOffset The initial log start offset
     * @param brokerTopicStats The broker topic stats
     * @return A UnifiedLog instance backed by MemoryLog
     */
    public static UnifiedLog createUnifiedLogWithMemoryLog(
            TopicPartition topicPartition,
            LogConfig config,
            MockTime time,
            long initialOffset,
            BrokerTopicStats brokerTopicStats) {

        // Create MemoryLog
        MemoryLog memoryLog = new MemoryLog(
                topicPartition,
                config,
                time,
                time.scheduler,
                initialOffset
        );

        // Create LogDirFailureChannel for the adapter
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);

        // Wrap in DataLogAdapter to provide FileLog interface
        DataLogAdapter adapter = new DataLogAdapter(memoryLog, logDirFailureChannel);

        // Create LeaderEpochCache with a no-op checkpoint file
        File tempCheckpointFile = null;
        try {
            tempCheckpointFile = File.createTempFile("leader-epoch-checkpoint", ".tmp");
            tempCheckpointFile.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LeaderEpochCheckpointFile checkpointFile;
        try {
            checkpointFile = new LeaderEpochCheckpointFile(tempCheckpointFile, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LeaderEpochFileCache leaderEpochCache = new LeaderEpochFileCache(
                topicPartition,
                checkpointFile,
                time.scheduler
        );

        // Create ProducerStateManager
        ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT,
                false
        );
        ProducerStateManager producerStateManager;
        try {
            // Use a temp directory for producer state snapshots
            File tempDir = File.createTempFile("producer-state", ".tmp");
            tempDir.delete();
            tempDir.mkdir();
            tempDir.deleteOnExit();

            producerStateManager = new ProducerStateManager(
                    topicPartition,
                    tempDir,
                    60 * 60 * 1000, // maxTransactionTimeoutMs
                    producerStateManagerConfig,
                    time
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Create UnifiedLog
        try {
            return new UnifiedLog(
                    initialOffset,                                         // logStartOffset
                    adapter,                                                // localLog (FileLog interface)
                    brokerTopicStats,                                       // brokerTopicStats
                    TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, // producerIdExpirationCheckIntervalMs
                    leaderEpochCache,                                       // leaderEpochCache
                    producerStateManager,                                   // producerStateManager
                    Optional.empty(),                                       // topicId (empty for test)
                    false,                                                  // remoteStorageSystemEnable
                    LogOffsetsListener.NO_OP_OFFSETS_LISTENER              // logOffsetsListener
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
