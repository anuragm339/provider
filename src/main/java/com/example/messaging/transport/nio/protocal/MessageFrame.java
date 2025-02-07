package com.example.messaging.transport.nio.protocal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class MessageFrame {
    private static final Logger logger = LoggerFactory.getLogger(MessageFrame.class);

    // Frame format:
    // | MAGIC (2) | VERSION (1) | TYPE (1) | LENGTH (4) | PAYLOAD (...) | CRC (4) |
    private static final short MAGIC_NUMBER = (short) 0xCAFE;
    private static final byte CURRENT_VERSION = 1;
    private static final int HEADER_SIZE = 8; // Magic(2) + Version(1) + Type(1) + Length(4)
    private static final int CRC_SIZE = 4;
    private static final int MIN_FRAME_SIZE = HEADER_SIZE + CRC_SIZE;

    private final byte version;
    private final MessageType type;
    private final byte[] payload;
    private final int crc;

    public enum MessageType {
        CONNECT((byte) 1),
        CONNECT_ACK((byte) 2),
        MESSAGE((byte) 3),
        MESSAGE_ACK((byte) 4),
        PING((byte) 5),
        PONG((byte) 6),
        DISCONNECT((byte) 7);

        private final byte value;

        MessageType(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static MessageType fromValue(byte value) {
            for (MessageType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown message type: " + value);
        }
    }

    private MessageFrame(byte version, MessageType type, byte[] payload, int crc) {
        this.version = version;
        this.type = type;
        this.payload = payload;
        this.crc = crc;
    }

    public int getSerializedSize() {
        return HEADER_SIZE + (payload != null ? payload.length : 0) + CRC_SIZE;
    }

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getSerializedSize());
        buffer.putShort(MAGIC_NUMBER);
        buffer.put(version);
        buffer.put(type.getValue());
        buffer.putInt(payload != null ? payload.length : 0);
        if (payload != null) {
            buffer.put(payload);
        }
        buffer.putInt(crc);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer wrapMessage(ByteBuffer message) {
        int messageLength = message.remaining();
        ByteBuffer frame = ByteBuffer.allocate(MIN_FRAME_SIZE + messageLength);

        // Write header
        frame.putShort(MAGIC_NUMBER);
        frame.put(CURRENT_VERSION);
        frame.put(MessageType.MESSAGE.getValue());
        frame.putInt(messageLength);

        // Write payload
        byte[] payload = new byte[messageLength];
        message.get(payload);
        frame.put(payload);

        // Calculate and write CRC
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        frame.putInt((int) crc32.getValue());

        frame.flip();
        return frame;
    }

    public static boolean hasCompleteMessage(ByteBuffer buffer) {
        if (buffer.remaining() < MIN_FRAME_SIZE) {
            return false;
        }

        buffer.mark();
        try {
            // Check magic number
            short magic = buffer.getShort();
            if (magic != MAGIC_NUMBER) {
                logger.warn("Invalid magic number: 0x{}", Integer.toHexString(magic & 0xFFFF));
                return false;
            }

            // Skip version and type
            buffer.position(buffer.position() + 2);

            // Get payload length
            int length = buffer.getInt();
            if (length < 0 || length > 10_000_000) { // 10MB max message size
                logger.warn("Invalid message length: {}", length);
                return false;
            }

            // Check if we have the complete message including payload and CRC
            return buffer.remaining() >= length + CRC_SIZE;

        } finally {
            buffer.reset();
        }
    }

    public static MessageFrame fromBuffer(ByteBuffer buffer) {
        if (!hasCompleteMessage(buffer)) {
            return null;
        }

        try {
            // Read header
            short magic = buffer.getShort();
            byte version = buffer.get();
            MessageType type = MessageType.fromValue(buffer.get());
            int length = buffer.getInt();

            // Read payload
            byte[] payload = new byte[length];
            buffer.get(payload);

            // Read and verify CRC
            int receivedCrc = buffer.getInt();
            CRC32 crc32 = new CRC32();
            crc32.update(payload);
            int calculatedCrc = (int) crc32.getValue();

            if (receivedCrc != calculatedCrc) {
                logger.warn("CRC mismatch. Expected: {}, Got: {}",
                        calculatedCrc, receivedCrc);
                return null;
            }

            return new MessageFrame(version, type, payload, receivedCrc);

        } catch (Exception e) {
            logger.error("Error parsing message frame", e);
            return null;
        }
    }

    // Getters
    public byte getVersion() { return version; }
    public MessageType getType() { return type; }
    public byte[] getPayload() { return payload; }
    public int getCrc() { return crc; }

    // Helper method to create specific message types
    public static ByteBuffer createConnectMessage(byte[] payload) {
        return createMessage(MessageType.CONNECT, payload);
    }

    public static ByteBuffer createDisconnectMessage() {
        return createMessage(MessageType.DISCONNECT, new byte[0]);
    }

    public static ByteBuffer createPingMessage() {
        return createMessage(MessageType.PING, new byte[0]);
    }

    public static ByteBuffer createPongMessage() {
        return createMessage(MessageType.PONG, new byte[0]);
    }

    private static ByteBuffer createMessage(MessageType type, byte[] payload) {
        ByteBuffer frame = ByteBuffer.allocate(MIN_FRAME_SIZE + payload.length);
        frame.putShort(MAGIC_NUMBER);
        frame.put(CURRENT_VERSION);
        frame.put(type.getValue());
        frame.putInt(payload.length);
        frame.put(payload);

        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        frame.putInt((int) crc32.getValue());

        frame.flip();
        return frame;
    }
}
