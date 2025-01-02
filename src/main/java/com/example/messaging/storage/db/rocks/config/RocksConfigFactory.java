package com.example.messaging.storage.db.rocks.config;

import org.rocksdb.*;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class RocksConfigFactory {
    private final RocksProperties properties;

    public RocksConfigFactory(RocksProperties properties) {
        this.properties = properties;
    }

    public Options createOptions() {
        Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setWriteBufferSize(properties.getPerformance().getWriteBufferSize())
                .setMaxWriteBufferNumber(properties.getPerformance().getMaxWriteBufferNumber())
                .setMaxBackgroundCompactions(properties.getPerformance().getMaxBackgroundCompactions())
                .setCompressionType(getCompressionType())
                .setTargetFileSizeBase(properties.getTuning().getTargetFileSizeBase())
                .setMaxBytesForLevelBase(properties.getTuning().getMaxBytesForLevelBase())
                .setNumLevels(properties.getTuning().getNumLevels())
                .setLevelCompactionDynamicLevelBytes(true)
                .setMaxBackgroundFlushes(2)
                .setMaxBackgroundJobs(4)
                .setAllowConcurrentMemtableWrite(true)
                .setEnableWriteThreadAdaptiveYield(true);

        options.setTableFormatConfig(createTableConfig());
        configureCompression(options);
        return options;
    }

    private void configureCompression(Options options) {
        if (properties.getStorage().isCompression()) {
            options.setBottommostCompressionType(getCompressionType())
                    .setCompressionPerLevel(getCompressionLevels());
        }
    }

    private List<CompressionType> getCompressionLevels() {
        CompressionType[] compressionLevels = new CompressionType[properties.getTuning().getNumLevels()];
        CompressionType mainCompression = getCompressionType();

        // No compression for level 0-1 for better write performance
        compressionLevels[0] = CompressionType.NO_COMPRESSION;
        compressionLevels[1] = CompressionType.NO_COMPRESSION;

        // Use configured compression for other levels
        for (int i = 2; i < compressionLevels.length; i++) {
            compressionLevels[i] = mainCompression;
        }

        return List.of(compressionLevels);
    }

    private BlockBasedTableConfig createTableConfig() {
        Cache blockCache = new LRUCache(properties.getPerformance().getBlockCacheSize());

        return new BlockBasedTableConfig()
                .setBlockSize(properties.getPerformance().getBlockSize())
                .setBlockCache(blockCache)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setFormatVersion(5)
                .setFilterPolicy(new BloomFilter(10, false))
                .setWholeKeyFiltering(true)
                .setChecksumType(ChecksumType.kxxHash)
                .setIndexType(IndexType.kTwoLevelIndexSearch)
                .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch)
                .setBlockRestartInterval(16);
    }

    public WriteOptions createWriteOptions() {
        return new WriteOptions()
                .setSync(false)
                .setDisableWAL(!properties.getStorage().isEnableWAL())
                .setNoSlowdown(false)
                .setLowPri(false);
    }

    public ReadOptions createReadOptions() {
        return new ReadOptions()
                .setVerifyChecksums(properties.getPerformance().isVerifyChecksums())
                .setFillCache(true)
                .setReadaheadSize(128 * 1024) // 128KB readahead
                .setMaxSkippableInternalKeys(10000)
                //.setAsync(true)
                .setPrefixSameAsStart(false)
                .setTotalOrderSeek(false);
                //.setAutoReadaheadSize(true);
    }

    public FlushOptions createFlushOptions() {
        return new FlushOptions()
                .setWaitForFlush(true)
                .setAllowWriteStall(true);
    }

    private CompressionType getCompressionType() {
        String type = properties.getStorage().getCompressionType().toUpperCase();
        return switch (type) {
            case "LZ4" -> CompressionType.LZ4_COMPRESSION;
            case "SNAPPY" -> CompressionType.SNAPPY_COMPRESSION;
            case "ZSTD" -> CompressionType.ZSTD_COMPRESSION;
            case "NONE" -> CompressionType.NO_COMPRESSION;
            default -> CompressionType.LZ4_COMPRESSION;
        };
    }

    public CompactionOptions createCompactionOptions() {
        return new CompactionOptions()
                .setMaxSubcompactions(properties.getMaintenance().getMaxCompactionThreads());
                //.setCompressionType(getCompressionType());
    }

    public IngestExternalFileOptions createIngestOptions() {
        return new IngestExternalFileOptions()
                .setMoveFiles(true)
                .setSnapshotConsistency(true)
                .setAllowGlobalSeqNo(true)
                .setWriteGlobalSeqno(true)
                .setIngestBehind(false);
    }
}
