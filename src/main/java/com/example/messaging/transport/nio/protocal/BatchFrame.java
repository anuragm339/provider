package com.example.messaging.transport.nio.protocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32;

public class BatchFrame {
    private static final Logger logger = LoggerFactory.getLogger(BatchFrame.class);

    // Batch Frame Format:
    // | MAGIC (2) | VERSION (1) | TYPE (1) | BATCH_ID (16) | MESSAGE_COUNT (4) |
    // | TOTAL_SIZE (4) | MESSAGES (...) | CRC (4) |

    private static final short BATCH_MAGIC_NUMBER = (short) 0xBACE;
    private static final byte CURRENT_VERSION = 1;
    private static final int HEADER_SIZE = 28; // Magic(2) + Version(1) + Type(1) + BatchId(16) + Count(4) + Size(4)
    private static final int CRC_SIZE = 4;
    private static final int MAX_BATCH_SIZE = 1000; // Maximum messages in a batch

    private final String batchId;
    private final byte version;
    private final BatchType type;
    private final List<MessageFrame> messages;
    private final int crc;

    public enum BatchType {
        DATA((byte) 1),
        ACK((byte) 2),
        RETRY((byte) 3);

        private final byte value;

        BatchType(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static BatchType fromValue(byte value) {
            for (BatchType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown batch type: " + value);
        }
    }

    private BatchFrame(String batchId, byte version, BatchType type, List<MessageFrame> messages, int crc) {
        this.batchId = batchId;
        this.version = version;
        this.type = type;
        this.messages = messages;
        this.crc = crc;
    }

    public static ByteBuffer createBatch(List<MessageFrame> messages, BatchType type) {
        if (messages.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Batch size exceeds maximum limit");
        }

        // Calculate total size needed
        int totalSize = 0;
        for (MessageFrame message : messages) {
            totalSize += message.getSerializedSize();
        }

        ByteBuffer batch = ByteBuffer.allocate(HEADER_SIZE + totalSize + CRC_SIZE);

        // Write header
        batch.putShort(BATCH_MAGIC_NUMBER);
        batch.put(CURRENT_VERSION);
        batch.put(type.getValue());

        // Generate and write batch ID (UUID)
        UUID uuid = UUID.randomUUID();
        batch.putLong(uuid.getMostSignificantBits());
        batch.putLong(uuid.getLeastSignificantBits());

        batch.putInt(messages.size());
        batch.putInt(totalSize);

        // Write messages
        CRC32 crc32 = new CRC32();
        for (MessageFrame message : messages) {
            ByteBuffer serialized = message.serialize();
            batch.put(serialized);
            crc32.update(serialized.array());
        }

        // Write CRC
        batch.putInt((int) crc32.getValue());

        batch.flip();
        return batch;
    }

    public static boolean hasCompleteBatch(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            return false;
        }

        buffer.mark();
        try {
            // Check magic number
            short magic = buffer.getShort();
            if (magic != BATCH_MAGIC_NUMBER) {
                return false;
            }

            // Skip version and type
            buffer.position(buffer.position() + 18); // Skip version, type, and batch ID

            // Get message count and total size
            int messageCount = buffer.getInt();
            int totalSize = buffer.getInt();

            if (messageCount < 0 || messageCount > MAX_BATCH_SIZE ||
                    totalSize < 0 || totalSize > 100_000_000) { // 100MB max batch size
                return false;
            }

            return buffer.remaining() >= totalSize + CRC_SIZE;

        } finally {
            buffer.reset();
        }
    }

    public static BatchFrame fromBuffer(ByteBuffer buffer) {
        if (!hasCompleteBatch(buffer)) {
            return null;
        }

        try {
            // Read header
            short magic = buffer.getShort();
            byte version = buffer.get();
            BatchType type = BatchType.fromValue(buffer.get());

            // Read batch ID
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            String batchId = new UUID(mostSigBits, leastSigBits).toString();

            int messageCount = buffer.getInt();
            int totalSize = buffer.getInt();

            // Read messages
            List<MessageFrame> messages = new ArrayList<>(messageCount);
            byte[] messageData = new byte[totalSize];
            buffer.get(messageData);

            // Verify CRC
            int receivedCrc = buffer.getInt();
            CRC32 crc32 = new CRC32();
            crc32.update(messageData);
            int calculatedCrc = (int) crc32.getValue();

            if (receivedCrc != calculatedCrc) {
                logger.warn("Batch CRC mismatch. BatchId: {}", batchId);
                return null;
            }

            // Parse individual messages
            ByteBuffer messageBuffer = ByteBuffer.wrap(messageData);
            while (messageBuffer.hasRemaining()) {
                MessageFrame message = MessageFrame.fromBuffer(messageBuffer);
                if (message != null) {
                    messages.add(message);
                }
            }

            return new BatchFrame(batchId, version, type, messages, receivedCrc);

        } catch (Exception e) {
            logger.error("Error parsing batch frame", e);
            return null;
        }
    }

    // Getters
    public String getBatchId() { return batchId; }
    public byte getVersion() { return version; }
    public BatchType getType() { return type; }
    public List<MessageFrame> getMessages() { return messages; }
    public int getCrc() { return crc; }
    public int getMessageCount() { return messages.size(); }
}
