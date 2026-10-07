# Requirements Document

## Introduction

Messaging Core defines the foundational, mode-agnostic structure for a configurable messaging library. Messaging Core is a shared .NET library (plain `Messaging` namespace root with sub-packages such as Messaging.Abstractions, Messaging.MassTransit, Messaging.Kafka, Messaging.Batch, and Messaging.Contracts) that is embedded by producer and consumer applications, not a standalone service. A single library is embedded by multiple applications, each configured at startup to operate in exactly one messaging mode (Publish-Subscribe, Message Queue, Streaming, or Batch Processing). The concrete mechanics of each mode are deliberately out of scope for this spec and will be defined in separate, later specs.

This spec establishes only the shared contracts, extension points, and cross-cutting concerns that let a concrete mode be plugged in and selected through configuration. The goal is a pluggable architecture (ports and adapters) with loose coupling and high cohesion: the core depends on abstractions, and each mode is a swappable adapter resolved at startup based on validated configuration.

Scope boundary (what this spec covers):
- The abstractions a mode must implement (publishing, consuming, lifecycle).
- Configuration-driven mode selection and fail-fast validation at startup.
- Cross-cutting concerns applied uniformly across modes: reliable publishing (outbox), retry policies and delivery semantics, dead-letter / poison-message handling, idempotency, and observability.
- The provider/adapter registration and resolution mechanism.

Scope boundary (what this spec does NOT cover):
- Internal mechanics, tuning, or broker-specific behavior of any single mode.
- Choice of a specific abstraction library (e.g., MassTransit) as a committed decision — this spec defines the contract such a library would sit behind; the tradeoff is recorded in the design phase.
- Concrete message schemas for business features.

## Glossary

- **Messaging_Core**: The deployable .NET library artifact (the "Messaging Core" library) embedded in an application that, per application/instance, operates in exactly one configured messaging mode.
- **Messaging_Mode**: A category of messaging behavior. One of: Publish-Subscribe, Message Queue, Streaming, Batch Processing.
- **Messaging_Provider**: A pluggable adapter that implements the core messaging contracts for one Messaging_Mode (and, where relevant, one underlying technology such as RabbitMQ, SQS, Azure Service Bus, Kafka, or in-memory).
- **Producer**: An external or internal source that submits a message to the Messaging_Core for publishing.
- **Consumer**: A component that receives and processes a message delivered by the Messaging_Core.
- **Message_Envelope**: The transport-neutral representation of a message carrying payload plus metadata (message id, correlation id, type, timestamp, headers).
- **Message_Publisher**: The core abstraction (port) through which application code publishes a Message_Envelope without knowing the active Messaging_Mode.
- **Message_Consumer_Host**: The core abstraction (port) that manages the lifecycle of consuming messages for the active Messaging_Mode.
- **Provider_Registry**: The component that maps a configured Messaging_Mode to its Messaging_Provider implementation and resolves the active provider at startup.
- **Messaging_Configuration**: The validated set of configuration values that selects the Messaging_Mode and supplies mode-independent settings (retry, DLQ, observability, outbox).
- **Configuration_Validator**: The component that validates Messaging_Configuration at startup before the service begins processing.
- **Outbox**: A persistence-backed buffer that records an outgoing message in the same transaction as a state change, enabling reliable publishing.
- **Outbox_Dispatcher**: The component that reads pending Outbox entries and hands them to the Message_Publisher.
- **Dead_Letter_Sink**: The destination for messages that cannot be processed successfully after exhausting the Retry_Policy.
- **Retry_Policy**: The configured strategy (attempt count, backoff, delays) governing reprocessing of transiently failed messages.
- **Poison_Message**: A message that repeatedly fails processing and is routed to the Dead_Letter_Sink.
- **Correlation_Id**: An identifier propagated across the Producer, the Messaging_Core, and the Consumer to trace a single logical operation.
- **Idempotency_Key**: A business or message key used by consumers to detect and discard duplicate deliveries.
- **Observability_Context**: The combined structured logging, metrics, and distributed tracing instrumentation applied uniformly across modes.

## Requirements

### Requirement 1: Configuration-Driven Mode Selection

**User Story:** As a service operator, I want to select the messaging mode through configuration before the service runs, so that I can deploy the same artifact in different messaging roles without code changes.

#### Acceptance Criteria

1. WHEN the Messaging_Core starts, THE Messaging_Core SHALL read the active Messaging_Mode from Messaging_Configuration before accepting any messaging requests.
2. IF no Messaging_Mode is specified in Messaging_Configuration at startup, THEN THE Messaging_Core SHALL fail startup and report an error indicating that the Messaging_Mode is missing.
3. IF the Messaging_Mode specified in Messaging_Configuration is not one of the recognized values (Publish-Subscribe, Message Queue, Streaming, Batch Processing), THEN THE Messaging_Core SHALL fail startup and report an error indicating the unrecognized Messaging_Mode value.
4. WHERE a valid Messaging_Mode is specified in Messaging_Configuration, THE Messaging_Core SHALL operate in exactly one Messaging_Mode for the lifetime of the instance and SHALL reject any request to change the Messaging_Mode after startup.
5. WHEN the Messaging_Core starts, THE Provider_Registry SHALL resolve the Messaging_Provider that corresponds to the configured Messaging_Mode.
6. IF the configured Messaging_Mode has no registered Messaging_Provider, THEN THE Messaging_Core SHALL fail startup and report an error indicating the unresolved Messaging_Mode.
7. WHEN the Messaging_Core completes startup successfully, THE Messaging_Core SHALL expose the active Messaging_Mode through a readiness or informational endpoint.

### Requirement 2: Startup Configuration Validation

**User Story:** As a service operator, I want configuration validated at startup, so that misconfiguration fails fast instead of surfacing during message processing.

#### Acceptance Criteria

1. WHEN the Messaging_Core starts, THE Configuration_Validator SHALL validate Messaging_Configuration and complete all validation checks before the Messaging_Core accepts or processes any message.
2. IF one or more required configuration values are missing, THEN THE Configuration_Validator SHALL prevent the Messaging_Core from entering a processing-ready state and report every missing value by its configuration name in a single startup failure, without reporting only the first missing value.
3. IF a configuration value is outside its permitted set or permitted numeric range, THEN THE Configuration_Validator SHALL prevent the Messaging_Core from entering a processing-ready state and report the invalid value by its configuration name together with the permitted set or permitted range.
4. IF the configured Messaging_Mode is not one of the recognized Messaging_Mode values, THEN THE Configuration_Validator SHALL prevent the Messaging_Core from entering a processing-ready state and report the complete list of recognized Messaging_Mode values.
5. WHERE a Messaging_Provider declares provider-specific required settings, THE Configuration_Validator SHALL validate those provider-specific settings during startup validation and apply the same missing-value and invalid-value reporting defined for all configuration values.
6. WHEN startup configuration validation completes with zero missing values and zero invalid values, THE Configuration_Validator SHALL permit the Messaging_Core to enter a processing-ready state.

### Requirement 3: Pluggable Provider Abstraction

**User Story:** As a developer, I want each messaging mode behind common contracts, so that a new mode can be added without changing core or application code.

#### Acceptance Criteria

1. THE Messaging_Core SHALL define a Message_Publisher abstraction that publishes a Message_Envelope independently of the active Messaging_Mode.
2. WHEN a Message_Publisher receives a Message_Envelope whose required fields are absent or malformed, THE Messaging_Core SHALL reject the publish operation, return an error indicating the validation failure, and leave the Message_Envelope undelivered.
3. THE Messaging_Core SHALL define a Message_Consumer_Host abstraction that manages the consumption lifecycle states of started, running, and stopped independently of the active Messaging_Mode.
4. THE Provider_Registry SHALL map each supported Messaging_Mode to exactly one Messaging_Provider implementation.
5. IF the Provider_Registry resolves a Messaging_Mode that maps to zero registered Messaging_Provider implementations or to more than one Messaging_Provider implementation, THEN THE Messaging_Core SHALL fail initialization and return an error indicating the unresolved or ambiguous Messaging_Mode mapping.
6. WHERE a new Messaging_Provider is registered, THE Provider_Registry SHALL make that provider selectable through Messaging_Configuration without changes to application code.
7. IF Messaging_Configuration specifies a Messaging_Mode that has no registered Messaging_Provider in the Provider_Registry, THEN THE Messaging_Core SHALL fail initialization and return an error indicating the unsupported Messaging_Mode.
8. THE Message_Publisher abstraction and the Message_Consumer_Host abstraction SHALL reside in an inward layer with no dependency on any concrete Messaging_Provider implementation.

### Requirement 4: Transport-Neutral Message Envelope

**User Story:** As a developer, I want a consistent message envelope across modes, so that payload and metadata are handled uniformly regardless of transport.

#### Acceptance Criteria

1. THE Message_Envelope SHALL carry a unique message id, a Correlation_Id, a message type identifier, a creation timestamp recorded in UTC, and a header collection, where the header collection supports between 0 and 64 entries.
2. WHEN a Producer submits a message without a Correlation_Id, THE Messaging_Core SHALL generate a globally unique Correlation_Id and attach the Correlation_Id to the Message_Envelope.
3. IF a Producer submits a message with a Correlation_Id that is empty or exceeds 128 characters, THEN THE Messaging_Core SHALL reject the message and SHALL return an error indicating the Correlation_Id is invalid, without publishing the Message_Envelope.
4. WHEN a Message_Envelope is published, THE Message_Publisher SHALL preserve the message id, Correlation_Id, message type identifier, creation timestamp, and all header entries unchanged through to delivery.
5. WHEN a Consumer receives a Message_Envelope, THE Messaging_Core SHALL bind the payload to a known message contract identified by the message type identifier.
6. IF a received Message_Envelope carries a payload that does not match any known message contract, THEN THE Messaging_Core SHALL reject the Message_Envelope and SHALL return an error indicating the message contract is unknown, without delivering the payload to the Consumer.
7. THE Message_Envelope contract SHALL support additive versioning such that a Consumer bound to an earlier contract version continues to bind successfully when new optional fields are added, and such that unrecognized fields are ignored by that Consumer.

### Requirement 5: Reliable Publishing via Outbox

**User Story:** As a developer, I want state changes and outgoing messages to be consistent, so that a published message is never lost and never sent without its state change.

#### Acceptance Criteria

1. WHERE the Outbox is enabled in Messaging_Configuration, THE Messaging_Core SHALL record an outgoing Message_Envelope in the Outbox within the same database transaction as the originating state change, such that either both the state change and the Outbox entry are committed together or neither is committed.
2. WHEN an Outbox entry transitions to the pending state after transaction commit, THE Outbox_Dispatcher SHALL publish the recorded Message_Envelope through the Message_Publisher.
3. WHEN the Outbox_Dispatcher successfully publishes an Outbox entry, THE Outbox_Dispatcher SHALL mark that entry as dispatched and SHALL exclude it from subsequent dispatch attempts.
4. IF publishing an Outbox entry fails, THEN THE Outbox_Dispatcher SHALL retain the entry in the pending state, SHALL leave the originating state change unchanged, and SHALL make the entry eligible for a subsequent dispatch attempt.
5. WHILE an Outbox entry remains in the pending state, THE Outbox_Dispatcher SHALL retry dispatch up to a maximum of 10 attempts, after which THE Outbox_Dispatcher SHALL mark the entry as failed and exclude it from further automatic dispatch attempts.
6. THE Outbox_Dispatcher SHALL dispatch each committed Outbox entry at least once, such that an entry is only marked as dispatched after the Message_Publisher confirms publication.

### Requirement 6: Retry Policy and Delivery Semantics

**User Story:** As a service operator, I want configurable retries with backoff, so that transient failures recover automatically without overwhelming downstream systems.

#### Acceptance Criteria

1. THE Messaging_Core SHALL read Retry_Policy settings from Messaging_Configuration, including a maximum attempt count bounded from 1 to 100 (default 5) and a backoff strategy enumerated as fixed, exponential, or exponential-with-jitter (default exponential).
2. IF processing a message fails with a transient error, where transient errors are limited to timeout, connection failure, and downstream-unavailable, THEN THE Messaging_Core SHALL retry processing according to the configured Retry_Policy up to the configured maximum attempt count.
3. IF processing a message fails with a non-transient error, where non-transient errors include validation failure and malformed payload, THEN THE Messaging_Core SHALL route the message to the Dead_Letter_Sink without performing any retry attempt.
4. WHILE retrying a failed message, THE Messaging_Core SHALL apply the configured backoff delay between consecutive attempts, where the per-attempt delay is bounded from 1 second to 300 seconds.
5. WHEN the configured maximum attempt count is reached without a successful attempt, THE Messaging_Core SHALL route the message to the Dead_Letter_Sink.
6. WHEN a message is routed to the Dead_Letter_Sink, THE Messaging_Core SHALL preserve the original message payload, the Idempotency_Key, and the failure reason with the dead-lettered message.
7. THE Messaging_Core SHALL document the delivery semantics as at-least-once.
8. THE Messaging_Core SHALL expose the Idempotency_Key to Consumers for duplicate detection.

### Requirement 7: Idempotent Consumption

**User Story:** As a developer, I want consumers to tolerate duplicate deliveries, so that at-least-once delivery does not cause duplicate side effects.

#### Acceptance Criteria

1. THE Messaging_Core SHALL expose an Idempotency_Key, derived deterministically from the Message_Envelope such that two Message_Envelopes representing the same logical message produce an identical Idempotency_Key, to each Consumer.
2. WHEN a Consumer receives a Message_Envelope whose Idempotency_Key matches a previously recorded processed Idempotency_Key, THE Messaging_Core SHALL expose a duplicate indication to the Consumer before any side effects are executed.
3. WHERE idempotent consumption is enabled in Messaging_Configuration, THE Messaging_Core SHALL record each successfully processed Idempotency_Key and retain it for a configured retention window between 1 second and 2,592,000 seconds (30 days) inclusive, with a default of 86,400 seconds (24 hours).
4. WHEN a duplicate Message_Envelope is detected, THE Messaging_Core SHALL acknowledge the Message_Envelope and SHALL NOT re-execute the associated Consumer side effects.
5. IF recording a processed Idempotency_Key fails, THEN THE Messaging_Core SHALL NOT acknowledge the Message_Envelope, SHALL leave the processed Idempotency_Key record unchanged, and SHALL expose an error indication to the Consumer identifying the recording failure.
6. IF idempotent consumption is enabled in Messaging_Configuration and the Idempotency_Key cannot be derived from the Message_Envelope, THEN THE Messaging_Core SHALL reject the Message_Envelope without executing Consumer side effects and SHALL expose an error indication identifying the missing Idempotency_Key.

### Requirement 8: Dead-Letter and Poison-Message Handling

**User Story:** As a service operator, I want messages that cannot be processed to be isolated, so that they do not block the pipeline and can be inspected later.

#### Acceptance Criteria

1. WHEN a message exhausts the Retry_Policy without a successful processing result, THE Messaging_Core SHALL route the message to the Dead_Letter_Sink within 5 seconds of the final failed attempt and SHALL remove the message from the active processing pipeline.
2. WHEN a message is routed to the Dead_Letter_Sink, THE Messaging_Core SHALL preserve the complete original Message_Envelope byte-for-byte, the Correlation_Id, the failure reason, the total count of processing attempts, and the UTC timestamp of the dead-letter event.
3. IF a message payload cannot be bound to a known message contract, THEN THE Messaging_Core SHALL route the message to the Dead_Letter_Sink as a Poison_Message without applying the Retry_Policy.
4. WHEN a message is routed to the Dead_Letter_Sink, THE Messaging_Core SHALL emit one metric recording the dead-letter event and one structured log entry containing the Correlation_Id, the failure reason, and the processing attempt count.
5. THE Dead_Letter_Sink SHALL be configurable per Messaging_Mode through Messaging_Configuration.
6. IF routing a message to the Dead_Letter_Sink fails, THEN THE Messaging_Core SHALL retain the message in the active processing pipeline, SHALL emit a structured log entry indicating the dead-letter routing failure, and SHALL NOT acknowledge the message to its source.

### Requirement 9: Observability

**User Story:** As a service operator, I want uniform logging, metrics, and tracing across modes, so that I can monitor, trace, and troubleshoot any instance the same way.

#### Acceptance Criteria

1. WHEN a message publish, consume, retry, or dead-letter event occurs, THE Messaging_Core SHALL emit a structured log entry that includes the event type, the Correlation_Id, and a UTC timestamp.
2. WHEN a message is published or consumed, THE Messaging_Core SHALL include the Correlation_Id in the associated log entries and trace spans.
3. WHEN a Producer publishes a message without a Correlation_Id, THE Messaging_Core SHALL generate a Correlation_Id and attach it to the message before publishing.
4. THE Messaging_Core SHALL propagate the same Correlation_Id unchanged from the Producer through publishing and delivery to the Consumer.
5. THE Messaging_Core SHALL expose metrics for published message count, consumed message count, retry count, and dead-letter count, each exposed as a monotonically increasing counter.
6. WHEN a message is published or consumed, THE Messaging_Core SHALL record a distributed trace span that links the publish operation to the consume operation through the Correlation_Id.
7. IF a log entry would contain a secret or a raw payload bearing sensitive data, THEN THE Messaging_Core SHALL replace the sensitive value with a fixed redaction marker before writing the log entry, such that the original sensitive value does not appear in the written log entry.
8. IF emitting a log entry, recording a metric, or recording a trace span fails, THEN THE Messaging_Core SHALL continue processing the associated message without failing the publish or consume operation.

### Requirement 10: Service Lifecycle and Health

**User Story:** As a service operator, I want the service to report health and shut down cleanly, so that orchestration and deployment behave predictably.

#### Acceptance Criteria

1. THE Messaging_Core SHALL expose a liveness endpoint that returns a healthy status when the service process is running and a readiness endpoint that returns a ready or not-ready status.
2. WHEN the liveness endpoint is queried, THE Messaging_Core SHALL return a response within 1 second.
3. WHEN the readiness endpoint is queried and the active Messaging_Provider is able to reach its underlying transport, THE Messaging_Core SHALL return a ready status within 1 second.
4. WHILE the active Messaging_Provider is unable to reach its underlying transport, THE Messaging_Core SHALL return a not-ready status through the readiness endpoint, including an indication of which dependency is unavailable.
5. WHEN the Messaging_Core receives a shutdown signal, THE Message_Consumer_Host SHALL stop accepting new messages.
6. WHILE a shutdown is in progress, THE Message_Consumer_Host SHALL allow in-flight messages to complete within the configured shutdown timeout, where the configured shutdown timeout defaults to 30 seconds and is bounded between 1 second and 300 seconds.
7. IF in-flight messages do not complete within the configured shutdown timeout, THEN THE Messaging_Core SHALL terminate the consumer host and record the count of incomplete in-flight messages as a non-negative integer.
