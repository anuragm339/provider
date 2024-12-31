package com.example.messaging.core.compression;

import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.models.Message;
import com.example.messaging.monitoring.health.CompressionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class MessageCompression {
    private static final Logger logger = LoggerFactory.getLogger(MessageCompression.class);
    private static final int COMPRESSION_THRESHOLD_BYTES = 1024; // 1KB
    private static final int BUFFER_SIZE = 8192; // 8KB

    private final AtomicLong totalCompressedMessages = new AtomicLong(0);
    private final AtomicLong totalCompressionTime = new AtomicLong(0);
    private final AtomicLong totalOriginalSize = new AtomicLong(0);
    private final AtomicLong totalCompressedSize = new AtomicLong(0);

    public boolean shouldCompress(Message message) {
        return message.getData() != null &&
                message.getData().length > COMPRESSION_THRESHOLD_BYTES;
    }

    public byte[] compressData(byte[] input) {
        if (input == null || input.length == 0) {
            return input;
        }

        long startTime = System.currentTimeMillis();
        try {
            int compressionLevel = chooseCompressionLevel(input.length);
            Deflater deflater = new Deflater(compressionLevel);
            deflater.setInput(input);
            deflater.finish();

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(input.length);
            byte[] buffer = new byte[BUFFER_SIZE];

            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }

            deflater.end();
            byte[] compressed = outputStream.toByteArray();

            updateCompressionStats(input.length, compressed.length,
                    System.currentTimeMillis() - startTime);

            return compressed;
        } catch (Exception e) {
            logger.error("Failed to compress data", e);
            throw new ProcessingException(
                    "Data compression failed",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public byte[] decompressData(byte[] compressedData) {
        if (compressedData == null || compressedData.length == 0) {
            return compressedData;
        }

        try {
            Inflater inflater = new Inflater();
            inflater.setInput(compressedData);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedData.length);
            byte[] buffer = new byte[BUFFER_SIZE];

            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }

            inflater.end();
            return outputStream.toByteArray();
        } catch (Exception e) {
            logger.error("Failed to decompress data", e);
            throw new ProcessingException(
                    "Data decompression failed",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    private int chooseCompressionLevel(int dataSize) {
        if (dataSize < 10 * 1024) { // < 10KB
            return Deflater.BEST_SPEED;
        } else if (dataSize > 1024 * 1024) { // > 1MB
            return Deflater.BEST_COMPRESSION;
        } else {
            return Deflater.DEFAULT_COMPRESSION;
        }
    }

    private void updateCompressionStats(long originalSize, long compressedSize, long timeMs) {
        totalCompressedMessages.incrementAndGet();
        totalCompressionTime.addAndGet(timeMs);
        totalOriginalSize.addAndGet(originalSize);
        totalCompressedSize.addAndGet(compressedSize);
    }

    public CompressionStats getCompressionStats() {
        long totalMessages = totalCompressedMessages.get();
        if (totalMessages == 0) {
            return new CompressionStats(0, 0, 0, 1.0);
        }

        double avgCompressionTime = (double) totalCompressionTime.get() / totalMessages;
        double avgOriginalSize = (double) totalOriginalSize.get() / totalMessages;
        double compressionRatio = (double) totalCompressedSize.get() / totalOriginalSize.get();

        return new CompressionStats(
                totalMessages,
                avgCompressionTime,
                avgOriginalSize,
                compressionRatio
        );
    }
}
