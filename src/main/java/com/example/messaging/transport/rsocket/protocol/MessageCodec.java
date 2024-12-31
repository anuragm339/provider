package com.example.messaging.transport.rsocket.protocol;

import com.example.messaging.exceptions.TransportException;
import com.example.messaging.transport.rsocket.model.ConsumerRequest;
import com.example.messaging.transport.rsocket.model.ReplayRequest;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class MessageCodec {
    private static final Logger logger = LoggerFactory.getLogger(MessageCodec.class);
    private final ObjectMapper objectMapper;

    public MessageCodec() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());  // Add support for Java 8 date/time types
        logger.info("MessageCodec initialized with Java Time support");
    }

    public ByteBuf encodeMessage(TransportMessage message) {
        try {
            logger.debug("Encoding message: {}", message.getMessageId());
            byte[] data = objectMapper.writeValueAsBytes(message);
            logger.debug("Successfully encoded message: {}", message.getMessageId());
            return Unpooled.wrappedBuffer(data);
        } catch (Exception e) {
            logger.error("Failed to encode message: {}", message.getMessageId(), e);
            throw new TransportException("Failed to encode message", "ENCODE_ERROR", e);
        }
    }

    public TransportMessage decodeMessage(ByteBuf data) {
        try {
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            TransportMessage message = objectMapper.readValue(bytes, TransportMessage.class);
            logger.debug("Successfully decoded message: {}", message.getMessageId());
            return message;
        } catch (Exception e) {
            logger.error("Failed to decode message", e);
            throw new TransportException("Failed to decode message", "DECODE_ERROR", e);
        } finally {
            data.release();
        }
    }

    public ConsumerRequest decodeRequest(Payload payload) {
        try {
            String requestData = payload.getDataUtf8();
            logger.debug("Decoding request: {}", requestData);

            // First decode as generic request to get type
            ConsumerRequest baseRequest = objectMapper.readValue(requestData, ConsumerRequest.class);

            // If it's a replay request, decode as replay request
            if (baseRequest.getType() == RequestType.REPLAY) {
                return objectMapper.readValue(requestData, ReplayRequest.class);
            }

            return baseRequest;
        } catch (Exception e) {
            logger.error("Failed to decode request payload", e);
            throw new TransportException(
                    "Failed to decode request payload",
                    "DECODE_ERROR",
                    e
            );
        }
    }
}
