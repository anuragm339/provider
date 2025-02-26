package com.example.messaging.storage.service;

import com.example.messaging.models.Message;
import com.example.messaging.core.pipeline.service.ProcessingResult;
import com.example.messaging.models.MessageState;
import io.netty.util.internal.StringUtil;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface MessageStore {
    /**
     * Store a message
     * @param message The message to store
     * @return Future containing the stored message offset
     */
    CompletableFuture<Long> store(Message message);

    /**
     * Store multiple messages in a batch
     * @param messages List of messages to store
     * @return Future containing list of stored message offsets
     */
    CompletableFuture<List<Long>> storeBatch(List<Message> messages);

    /**
     * Retrieve a message by its offset
     * @param offset The message offset
     * @return Future containing the message if found
     */
    CompletableFuture<Optional<Message>> getMessage(long offset);

    /**
     * Store processing result for a message
     * @param result The processing result to store
     */
    CompletableFuture<Void> storeProcessingResult(ProcessingResult result);

    /**
     * Get processing result for a message
     * @param offset The message offset
     * @return Future containing the processing result if found
     */
    CompletableFuture<Optional<ProcessingResult>> getProcessingResult(long offset);

    /**
     * Delete messages older than specified offset
     * @param offset Messages before this offset will be deleted
     * @return Future containing number of messages deleted
     */
    CompletableFuture<Integer> deleteMessagesBeforeOffset(long offset);

    /**
     * Get the current highest offset
     * @return The highest message offset in the store
     */
    CompletableFuture<Long> getCurrentOffset();

    /**
     * Check if store can accept more messages
     * @return true if store can accept messages
     */
    boolean canAccept();

    /**
     * Check store health
     * @return true if store is healthy
     */
    boolean isHealthy();

    CompletableFuture<Integer> deleteMessagesWithOffsets(Set<Long> offsetsToDelete);

    CompletableFuture<Void> storeProcessingResultBatch(List<ProcessingResult> result);

    CompletableFuture<List<Message>> getMessagesAfterOffset(long offset,String type);

    /**
     * Retrieve messages by their offsets
     * @param offsets List of message offsets to retrieve
     * @return Future containing list of messages
     */
    CompletableFuture<List<Message>> getMessagesByOffsets(List<Long> offsets);

    CompletableFuture<Void> updateMessageStatus(List<Long> offset, MessageState messageState, String consumerId);

    /**
     * Get messages for a group in a specific state
     * @param groupId The consumer group ID
     * @param state The message state to query
     * @return Future containing list of messages in the specified state
     */
    CompletableFuture<List<Message>> getMessagesInState(String groupId, MessageState state);
}
