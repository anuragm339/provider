package com.example.messaging.storage.db.rocks.duplicate;


import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.models.Message;
import com.example.messaging.storage.db.rocks.config.RocksConfigFactory;
import com.example.messaging.storage.db.rocks.config.RocksProperties;
import com.example.messaging.storage.db.rocks.model.DeletionEntry;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.storage.db.rocks.model.DuplicateResult;
import org.rocksdb.*;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class BidirectionalDuplicateHandler implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BidirectionalDuplicateHandler.class);
    private static final int MAX_QUEUE_SIZE = 100_000;
    private static final int OVERFLOW_BATCH_SIZE = 1000;
    private static final int CLEANUP_BATCH_SIZE = 1000;

    private final RocksDB dedupeStore;
    private ColumnFamilyHandle keyToOffsetHandle;
    private ColumnFamilyHandle offsetToKeyHandle;
    private final BlockingQueue<DeletionEntry> deletionQueue;
    private final ConcurrentMap<String, AtomicLong> keyVersionMap;
    private final AtomicLong queuedForDeletionCount;
    private final MessageStore messageStore;
    private final ExecutorService executorService;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final AtomicBoolean isRunning;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;

    static {
        RocksDB.loadLibrary();
    }

    public BidirectionalDuplicateHandler(
            RocksConfigFactory configFactory,
            RocksProperties properties,
            MessageStore messageStore,
            ExecutorService executorService) {
        this.messageStore = messageStore;
        this.executorService = executorService;
        this.deletionQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        this.keyVersionMap = new ConcurrentHashMap<>();
        this.queuedForDeletionCount = new AtomicLong(0);
        this.isRunning = new AtomicBoolean(true);
        this.processedCount = new AtomicLong(0);
        this.errorCount = new AtomicLong(0);

        try {
            this.dedupeStore = initializeRocksDB(properties.getStorage().getDbPath());
            this.writeOptions = configFactory.createWriteOptions();
            this.readOptions = configFactory.createReadOptions();
        } catch (RocksDBException e) {
            throw new ProcessingException(
                    "Failed to initialize RocksDB",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    false,
                    e
            );
        }
    }

    private RocksDB initializeRocksDB(String dbPath) throws RocksDBException {
        File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        List<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();
        columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

        List<byte[]> existingFamilies = RocksDB.listColumnFamilies(new Options(), dbPath);

        if (existingFamilies != null) {
            for (byte[] family : existingFamilies) {
                if (!Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                    columnFamilies.add(new ColumnFamilyDescriptor(family));
                }
            }
        }

        if (existingFamilies == null || !containsFamily(existingFamilies, "key_to_offset")) {
            columnFamilies.add(new ColumnFamilyDescriptor("key_to_offset".getBytes()));
        }
        if (existingFamilies == null || !containsFamily(existingFamilies, "offset_to_key")) {
            columnFamilies.add(new ColumnFamilyDescriptor("offset_to_key".getBytes()));
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundJobs(4)
                .setMaxBackgroundCompactions(4);

        RocksDB db = RocksDB.open(dbOptions, dbPath, columnFamilies, columnFamilyHandles);

        for (int i = 0; i < columnFamilyHandles.size(); i++) {
            String familyName = new String(columnFamilies.get(i).getName());
            if (familyName.equals("key_to_offset")) {
                this.keyToOffsetHandle = columnFamilyHandles.get(i);
            } else if (familyName.equals("offset_to_key")) {
                this.offsetToKeyHandle = columnFamilyHandles.get(i);
            }
        }

        return db;
    }

    public DuplicateResult checkAndUpdate(Message message) {
        String messageKeyString = new String(generateMessageKey(message));
        byte[] messageKey = messageKeyString.getBytes();
        byte[] newOffset = longToBytes(message.getMsgOffset());

        AtomicLong versionCounter = keyVersionMap.computeIfAbsent(
                messageKeyString,
                k -> new AtomicLong(0)
        );

        try {
            byte[] existingOffset = dedupeStore.get(keyToOffsetHandle, readOptions, messageKey);

            if (existingOffset != null) {
                long oldOffset = bytesToLong(existingOffset);

                if (message.getMsgOffset() > oldOffset) {
                    long newVersion = versionCounter.incrementAndGet();

                    if (queuedForDeletionCount.get() >= MAX_QUEUE_SIZE) {
                        handleQueueOverflow();
                    }

                    queueForDeletion(messageKey, oldOffset, newVersion);
                    updateMappings(messageKey, newOffset);
                }

                processedCount.incrementAndGet();
                return new DuplicateResult(true, existingOffset);
            }

            versionCounter.set(1);
            updateMappings(messageKey, newOffset);
            processedCount.incrementAndGet();
            return new DuplicateResult(false, null);

        } catch (RocksDBException e) {
            errorCount.incrementAndGet();
            throw new ProcessingException(
                    "Failed to check/update duplicate",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    private void updateMappings(byte[] key, byte[] offset) throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(keyToOffsetHandle, key, offset);
            batch.put(offsetToKeyHandle, offset, key);
            dedupeStore.write(writeOptions, batch);
        }
    }

    private void queueForDeletion(byte[] key, long offset, long version) {
        DeletionEntry entry = new DeletionEntry(key, offset);

        if (!deletionQueue.offer(entry)) {
            handleQueueOverflow();
            if (!deletionQueue.offer(entry)) {
                logger.warn("Failed to queue deletion entry after overflow handling");
            } else {
                queuedForDeletionCount.incrementAndGet();
            }
        } else {
            queuedForDeletionCount.incrementAndGet();
        }
    }

    @Scheduled(fixedDelay = "1m")
    public void performCleanup() {
        if (!isRunning.get()) return;

        try {
            List<DeletionEntry> batch = new ArrayList<>();
            Set<Long> offsetsToDelete = new HashSet<>();

            while (batch.size() < CLEANUP_BATCH_SIZE && !deletionQueue.isEmpty()) {
                DeletionEntry entry = deletionQueue.poll();
                if (entry != null) {
                    batch.add(entry);
                    offsetsToDelete.add(entry.getOffset());
                    queuedForDeletionCount.decrementAndGet();
                }
            }

            if (!batch.isEmpty()) {
                deleteFromRocksDB(batch);
                messageStore.deleteMessagesWithOffsets(offsetsToDelete)
                        .whenComplete((count, error) -> {
                            if (error != null) {
                                logger.error("Failed to delete messages {} ", error);
                                requeueCriticalEntries(batch);
                            } else {
                                logger.info("Successfully deleted {} messages", count);
                            }
                        });
            }
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }

    private void handleQueueOverflow() {
        logger.info("Handling queue overflow - current size: {}", queuedForDeletionCount.get());

        List<DeletionEntry> batch = new ArrayList<>();
        Set<Long> offsetsToDelete = new HashSet<>();
        int processed = 0;

        while (processed < OVERFLOW_BATCH_SIZE && !deletionQueue.isEmpty()) {
            DeletionEntry entry = deletionQueue.poll();
            if (entry != null) {
                batch.add(entry);
                offsetsToDelete.add(entry.getOffset());
                processed++;
                queuedForDeletionCount.decrementAndGet();
            }
        }

        if (!batch.isEmpty()) {
            try {
                deleteFromRocksDB(batch);
                CompletableFuture<Integer> deleteFuture =
                        messageStore.deleteMessagesWithOffsets(offsetsToDelete);
                deleteFuture.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failed to process overflow batch", e);
                requeueCriticalEntries(batch);
            }
        }
    }

    private void deleteFromRocksDB(List<DeletionEntry> batch) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (DeletionEntry entry : batch) {
                writeBatch.delete(keyToOffsetHandle, entry.getKey());
                writeBatch.delete(offsetToKeyHandle, longToBytes(entry.getOffset()));
            }
            dedupeStore.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new ProcessingException(
                    "Failed to delete from RocksDB",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    private void requeueCriticalEntries(List<DeletionEntry> batch) {
        Map<String, DeletionEntry> latestVersions = new HashMap<>();

        for (DeletionEntry entry : batch) {
            String key = new String(entry.getKey());
            DeletionEntry existing = latestVersions.get(key);

            if (existing == null || entry.getOffset() > existing.getOffset()) {
                latestVersions.put(key, entry);
            }
        }

        latestVersions.values().forEach(entry -> {
            if (deletionQueue.offer(entry)) {
                queuedForDeletionCount.incrementAndGet();
            }
        });
    }

    private byte[] generateMessageKey(Message message) {
        return (message.getType() + "_" +
                String.valueOf(Arrays.hashCode(message.getData())))
                .getBytes();
    }

    private byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    private boolean containsFamily(List<byte[]> families, String familyName) {
        byte[] searchName = familyName.getBytes();
        return families.stream()
                .anyMatch(family -> Arrays.equals(family, searchName));
    }

    public Map<String, Long> getStatistics() {
        return Map.of(
                "processed_count", processedCount.get(),
                "error_count", errorCount.get(),
                "queued_for_deletion", queuedForDeletionCount.get()
        );
    }

    @Override
    public void close() {
        isRunning.set(false);
        if (writeOptions != null) writeOptions.close();
        if (readOptions != null) readOptions.close();
        if (keyToOffsetHandle != null) keyToOffsetHandle.close();
        if (offsetToKeyHandle != null) offsetToKeyHandle.close();
        if (dedupeStore != null) dedupeStore.close();
    }

}
