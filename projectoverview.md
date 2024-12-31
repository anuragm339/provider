Project Overview:
A high-throughput, memory-efficient messaging system with support for complete data resynchronization, using RSocket for communication and built with Micronaut framework.Complete System OverviewClick to open diagram
Repository Structure:

A. message-common:

Message protocols
Data models
Common utilities
Version compatibility

B. message-provider:

Core message handling
Storage management
Client connection handling
Bootstrap/recovery logic

C. message-consumer-sdk:

Client implementation
Local caching
Connection management
Example implementations


Core Features:

A. Message Types:

Regular messages
Bootstrap initiation
Reset commands
Status updates

B. Protocol Features:

Sequence tracking
Acknowledgments
Back pressure
Error handling


Technical Stack:

A. Server Side:

Micronaut Framework
RSocket for communication
SQLite with RocksDB engine
Redis for caching

B. Client Side:

Java SDK
Local SQLite cache
RSocket client
Reactive streams


Key Processes:

A. Normal Operation:
CopyClient → Connect → Subscribe → Receive Messages → Acknowledge
B. Bootstrap Process:
CopyRequest Bootstrap → Clear Local DB → Receive Data → Verify → Resume Normal
C. Recovery:
CopyDetect Missing → Request Range → Receive Missing → Sync → Continue

Data Management:

A. Storage:

Efficient write patterns
Segment-based storage
Automatic cleanup
Index management

B. Memory:

Minimal memory usage
Efficient caching
Back pressure control
Resource limits


Performance Features:

A. Scalability:

Horizontal scaling
Load balancing
Connection pooling
Resource management

B. Reliability:

Message persistence
Guaranteed delivery
Error recovery
Connection resilience


Development Practices:

A. Code Quality:

Unit tests
Integration tests
Performance tests
Code reviews

B. Deployment:

CI/CD pipelines
Version management
Deployment strategies
Monitoring setup


Monitoring:

A. Metrics:

Message throughput
Error rates
Resource usage
Client health

B. Alerts:

Connection issues
Processing delays
Resource constraints
Error thresholds


Future Considerations:

A. Scalability:

Multi-region support
Enhanced caching
Improved cleanup
Better compression

B. Features:

Message prioritization
Enhanced security
Better monitoring
More client languages

Message Reception and Processing:


Provider receives message from upstream
Validates message format
Assigns sequence number
Stores in SQLite/RocksDB
Identifies target consumers
Manages delivery state


Delivery Responsibilities:


Track active consumers
Maintain delivery order
Handle back pressure
Ensure acknowledgments
Retry on failures
Manage delivery windows


Consumer State Management:


Track consumer connections
Monitor consumer health
Handle disconnections
Manage consumer groups
Track delivery progress
Maintain checkpoints


Delivery Scenarios:

A. Normal Flow:
CopyReceive → Store → Queue → Deliver → Wait for Ack → Confirm
B. Slow Consumer:
CopyReceive → Store → Apply Back Pressure → Deliver when ready
C. Disconnected Consumer:
CopyStore → Track Missing → Deliver on Reconnect

Edge Cases to Handle:


Consumer crashes mid-delivery
Network partitions
Message ordering issues
Duplicate messages
Lost acknowledgments
Partial failures


Delivery Guarantees:


At-least-once delivery
Ordered delivery (per consumer)
Delivery confirmation
Data consistency
No message loss

