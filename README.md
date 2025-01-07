# Message Provider Service

A high-performance, scalable messaging service that supports batch processing, message persistence, and real-time message delivery using RSocket protocol.

## Architecture Overview

### Core Components

1. **Pipeline Management**
    - PipelineManager Interface
    - DefaultPipelineManager Implementation
    - Batch Processing Support
    - Message Validation

2. **Storage Layer**
    - SQLite Message Store
    - Database Maintenance
    - Message Compression
    - Performance Monitoring

3. **Transport Layer**
    - RSocket Server
    - Consumer Registry
    - Connection Management
    - Message Distribution

## Features

- Batch message processing
- Real-time message delivery
- Message persistence with SQLite
- Automatic database maintenance
- Consumer group support
- Message compression
- Performance monitoring
- Error handling and retries
- Connection management

## Technical Stack

- Java 17
- RSocket for communication
- SQLite for storage
- Reactive programming (Project Reactor)
- Micronaut framework

## Architecture Details

### Component Diagram
Shows the high-level structure and relationships between components:

```mermaid

graph TB
    Client[Client/API] --> |HTTP/REST| Controller[MessageController]
    Controller --> |Submit Message| Pipeline[PipelineManager]
    Pipeline --> |Store| DB[(Message Store)]
    Pipeline --> |Process| MessageProcessor[Message Processor]
    Pipeline --> |Batch Process| BatchProcessor[Batch Processor]
    MessageProcessor --> |Validate| Validator[Message Validator]
    MessageProcessor --> |Compress| Compression[Message Compression]
    Pipeline --> |Queue| TypeQueues[Type-Based Queues]
    Pipeline --> |Monitor| HealthCheck[Health Monitor]
    Pipeline --> |Transport| RSocket[RSocket Server]
    RSocket --> |Publish| Consumers[Consumer Groups]
    
    subgraph Storage
        DB
        DupeHandler[Duplicate Handler]
    end
    
    subgraph Monitoring
        HealthCheck
        Stats[Statistics]
        Maintenance[Maintenance]
    end
    
    subgraph Consumers
        Consumer1[Consumer 1]
        Consumer2[Consumer 2]
        ConsumerN[Consumer N]
    end

    classDef primary fill:#f9f,stroke:#333,stroke-width:4px;
    classDef secondary fill:#bbf,stroke:#333,stroke-width:2px;
    class Pipeline,MessageProcessor primary;
    class Controller,DB,RSocket secondary;
```

### Class Diagram
Detailed class structure and relationships:

```mermaid
classDiagram
    class PipelineManager {
        <<interface>>
        +start()
        +stop()
        +submitMessage(Message)
        +submitBatch(List~Message~)
        +getStatus()
        +canAccept()
    }
    
    class DefaultPipelineManager {
        -MessageProcessor messageProcessor
        -BatchProcessor batchProcessor
        -MessageStore messageStore
        -Map~String,ByteSizedBlockingQueue~ typeBasedQueues
        -QueueConfiguration queueConfig
        +processMessagesParallel()
        -processMessageAsync(Message)
        -monitorMessageCompletion(Message)
    }
    
    class ByteSizedBlockingQueue {
        -LinkedBlockingQueue~Message~ queue
        -AtomicLong currentSizeBytes
        -long maxSizeBytes
        +offer(Message, timeout)
        +poll()
        +getCurrentSizeBytes()
        +contains(Message)
    }
    
    class Message {
        -long msgOffset
        -String type
        -Instant createdUtc
        -byte[] data
        -MessageState state
    }
    
    class MessageProcessor {
        <<interface>>
        +processMessage(Message)
        +verifyProcessing(offset, token)
        +canAccept()
        +isOperational()
    }
    
    class MessageStore {
        <<interface>>
        +store(Message)
        +storeBatch(List~Message~)
        +getMessage(offset)
        +storeProcessingResult(result)
        +getProcessingResult(offset)
    }

    PipelineManager <|-- DefaultPipelineManager
    DefaultPipelineManager --> MessageProcessor
    DefaultPipelineManager --> MessageStore
    DefaultPipelineManager --> ByteSizedBlockingQueue
    DefaultPipelineManager ..> Message
    MessageProcessor ..> Message
    MessageStore ..> Message
```

### Sequence Diagram
Shows the message flow and component interactions:

```mermaid
sequenceDiagram
    participant C as Client
    participant MC as MessageController
    participant PM as PipelineManager
    participant Q as TypeQueue
    participant MP as MessageProcessor
    participant MS as MessageStore
    participant RS as RSocketServer
    participant CO as Consumer

    C->>MC: Submit Message
    activate MC
    MC->>PM: submitMessage(message)
    activate PM
    
    PM->>Q: offer(message)
    activate Q
    Q-->>PM: message queued
    deactivate Q
    
    par Process Message
        PM->>MP: processMessage(message)
        activate MP
        MP->>MS: store(message)
        MS-->>MP: stored
        MP->>MS: storeProcessingResult(result)
        MS-->>MP: stored
        MP-->>PM: processing complete
        deactivate MP
    and Monitor Completion
        PM->>MS: getProcessingResult(offset)
        MS-->>PM: result
    end
    
    PM->>RS: publishMessage(message)
    activate RS
    RS->>CO: deliver message
    CO-->>RS: acknowledge
    RS-->>PM: published
    deactivate RS
    
    PM-->>MC: message processed
    deactivate PM
    MC-->>C: success response
    deactivate MC
```

### message submission flow class diagram
Shows the message submission and class interactions:

````mermaid
classDiagram
    class MessageController {
        -PipelineManager pipelineManager
        +submitMessage(MessageRequest)
        +submitBatch(List~MessageRequest~)
    }

    class MessageRequest {
        -long offset
        -String type
        -String data
        +getOffset()
        +getType()
        +getData()
    }

    class Message {
        -long msgOffset
        -String type
        -Instant createdUtc
        -byte[] data
        -MessageState state
        +builder()
    }

    class DefaultPipelineManager {
        -MessageProcessor messageProcessor
        -MessageStore messageStore
        -Map~String,ByteSizedBlockingQueue~ typeBasedQueues
        +submitMessage(Message)
        -processMessageAsync(Message)
        -monitorMessageCompletion(Message)
    }

    class ByteSizedBlockingQueue {
        -LinkedBlockingQueue~Message~ queue
        -AtomicLong currentSizeBytes
        +offer(Message)
        +poll()
        +contains(Message)
    }

    class DefaultMessageProcessor {
        -MessageValidator validator
        -MessageStore messageStore
        -MessagePublisher messagePublisher
        +processMessage(Message)
        -doProcessMessage(Message)
        -calculateChecksum(Message)
    }

    class MessageValidator {
        +validate(Message)
        -validateData(byte[])
        -validateTimestamp(Instant)
    }

    class MessageCompression {
        +compressData(byte[])
        +decompressData(byte[])
        +shouldCompress(Message)
    }

    class SQLiteMessageStore {
        -DataSource dataSource
        +store(Message)
        +storeProcessingResult(ProcessingResult)
        +getMessage(long)
        +getProcessingResult(long)
    }

    class ProcessingResult {
        -long offset
        -String checksum
        -boolean successful
        -long processingTimestamp
        +builder()
    }

    class MessagePublisher {
        -ConsumerRegistry consumerRegistry
        +publishMessage(Message, String)
    }

    class ConsumerRegistry {
        -Map~String, ConsumerConnection~ consumerConnections
        +broadcastToGroup(String, TransportMessage)
    }

    MessageController ..> MessageRequest
    MessageController --> DefaultPipelineManager
    MessageRequest ..> Message : converts to
    DefaultPipelineManager --> ByteSizedBlockingQueue
    DefaultPipelineManager --> DefaultMessageProcessor
    DefaultPipelineManager --> SQLiteMessageStore
    DefaultMessageProcessor --> MessageValidator
    DefaultMessageProcessor --> MessageCompression
    DefaultMessageProcessor --> MessagePublisher
    DefaultMessageProcessor ..> ProcessingResult : creates
    MessagePublisher --> ConsumerRegistry
    SQLiteMessageStore ..> ProcessingResult : stores

    note for DefaultPipelineManager "1. Receives message\n2. Queues by type\n3. Processes async\n4. Monitors completion"
    note for DefaultMessageProcessor "1. Validates\n2. Compresses\n3. Processes\n4. Publishes"
    note for SQLiteMessageStore "Handles message\npersistence and\nresult storage"
````


### message submission flow component diagram
Shows the message submission and component interactions
```mermaid
flowchart TB
    subgraph Client["Client Layer"]
        REST[REST API Client]
    end

    subgraph API["API Layer"]
        MC[Message Controller]
        VL[Request Validator]
    end

    subgraph Pipeline["Pipeline Management"]
        PM[Pipeline Manager]
        QM[Queue Manager]
        MM[Message Monitor]
        subgraph Queues["Type-Based Queues"]
            Q1[Queue Type A]
            Q2[Queue Type B]
            Q3[Queue Type N]
        end
    end

    subgraph Processing["Processing Layer"]
        MP[Message Processor]
        MV[Message Validator]
        MC1[Message Compression]
        DH[Duplicate Handler]
    end

    subgraph Storage["Storage Layer"]
        MS[Message Store]
        RS[Result Store]
    end

    subgraph Transport["Transport Layer"]
        PUB[Publisher]
        RS1[RSocket Server]
        CR[Consumer Registry]
    end

    REST -->|HTTP POST| MC
    MC -->|Validate| VL
    MC -->|Submit| PM
    PM -->|Queue| QM
    QM -->|Type A| Q1
    QM -->|Type B| Q2
    QM -->|Type N| Q3
    PM -->|Monitor| MM
    PM -->|Process| MP
    MP -->|Validate| MV
    MP -->|Compress| MC1
    MP -->|Check Duplicate| DH
    MP -->|Store| MS
    MP -->|Store Result| RS
    MP -->|Publish| PUB
    PUB -->|Register| CR
    PUB -->|Send| RS1

    classDef primary fill:#f9f,stroke:#333,stroke-width:2px;
    classDef secondary fill:#bbf,stroke:#333,stroke-width:1px;
    class PM,MP primary;
    class MC,MS,RS1 secondary;

```
# Message Provider Service

## Key Interactions

### 1. Message Processing Flow
- Messages enter through PipelineManager
- Validated by MessageValidator
- Stored in MessageStore
- Processed by BatchProcessor if applicable
- Distributed to consumers via ConsumerRegistry

### 2. Consumer Management Flow
- Consumer connects through RSocket
- ConsumerRegistry manages registration
- ConsumerConnection handles individual connections
- Messages distributed to consumer groups
- Acknowledgments tracked and managed

### 3. Storage Management Flow
- Messages persisted to SQLite
- Regular maintenance performed
- Compression applied when beneficial
- Performance metrics collected
- Cleanup handled automatically

[Rest of the README content remains the same...]
## Getting Started

### Prerequisites
- Java 17 or higher
- Gradle 7.x or higher
- SQLite 3.x

### Configuration

```java
// Configure PipelineManager
PipelineConfig config = new PipelineConfig.Default() {
    @Override
    public int getMaxConcurrentMessages() {
        return 100;
    }

    @Override
    public int getMaxQueueSize() {
        return 1000;
    }
};

// Configure Storage
SQLiteConfig sqliteConfig = SQLiteConfig.builder()
    .dbPath("message_store.db")
    .maxMessages(10000)
    .maxStorageSize(100 * 1024 * 1024) // 100MB
    .retentionPeriodMs(24 * 60 * 60 * 1000L) // 24 hours
    .build();
```

### Usage Example

```java
// Initialize components
MessageStore messageStore = createMessageStore(dataSource, sqliteConfig);
PipelineManager pipelineManager = createPipelineManager(messageStore);
RSocketServer rSocketServer = createRSocketServer(rSocketConfig);

// Start the pipeline
pipelineManager.start();

// Submit a message
Message message = Message.builder()
    .type("TEST")
    .data("Test message content".getBytes())
    .build();

pipelineManager.submitMessage(message)
    .thenAccept(offset -> System.out.println("Message stored with offset: " + offset));
```

## Message Flow

1. Message Submission
    - Message validation
    - Storage in SQLite
    - Batch processing if applicable
    - Distribution to consumers

2. Consumer Management
    - Consumer registration
    - Group management
    - Connection monitoring
    - Message delivery

3. Storage Management
    - Automatic cleanup
    - Database maintenance
    - Performance optimization
    - Message compression

## Error Handling

The system handles various types of errors:
- Connection failures
- Storage errors
- Processing failures
- Consumer disconnections

Each error type has specific handling and retry strategies.

## Monitoring

Available metrics include:
- Message throughput
- Storage utilization
- Consumer connections
- Processing performance
- Error rates

## Scaling Considerations

The service supports scaling through:
- Batch processing
- Connection pooling
- Message compression
- Efficient storage management

## Best Practices

1. Message Processing:
    - Use appropriate batch sizes
    - Monitor processing performance
    - Handle errors properly

2. Storage Management:
    - Regular maintenance
    - Monitor disk usage
    - Set appropriate retention periods

3. Consumer Management:
    - Monitor consumer health
    - Handle disconnections gracefully
    - Implement proper error handling

## License

[Your License Here]
