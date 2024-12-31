package com.example.messaging.transport.rsocket.protocol;

import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.TransportException;
import com.example.messaging.transport.rsocket.model.ConsumerRequest;
import com.example.messaging.transport.rsocket.model.ReplayRequest;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import jakarta.inject.Singleton;

@Singleton
public class MessageCodec {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ByteBuf encodeMessage(TransportMessage message) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(message);
            return Unpooled.wrappedBuffer(data);
        } catch (JsonProcessingException e) {
            throw new TransportException("Failed to encode message", ErrorCode.ENCODING_FAILED.getCode(),e);
        }
    }

    public static TransportMessage decodeMessage(ByteBuf data) {
        try {
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            return objectMapper.readValue(bytes, TransportMessage.class);
        } catch (IOException e) {
            throw new TransportException("Failed to decode message", ErrorCode.DECODING_FAILED.getCode(),e);
        }
    }
    public ConsumerRequest decodeRequest(Payload payload) {
        try {
            String requestData = payload.getDataUtf8();

            // First decode as generic request to get type
            ConsumerRequest baseRequest = objectMapper.readValue(requestData, ConsumerRequest.class);

            // If it's a replay request, decode as replay request
            if (baseRequest.getType() == RequestType.REPLAY) {
                return objectMapper.readValue(requestData, ReplayRequest.class);
            }

            return baseRequest;
        } catch (IOException e) {
            throw new TransportException(
                    "Failed to decode request payload",
                    ErrorCode.DECODING_FAILED.getCode(),
                    e
            );
        }
    }
}
