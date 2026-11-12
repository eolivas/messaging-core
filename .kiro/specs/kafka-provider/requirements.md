# Requirements Document

## Introduction

The Kafka Provider implements Apache Kafka as the concrete Streaming transport for the Messaging Core library by filling in the existing `Messaging.Kafka` adapter shell. Unlike the RabbitMQ provider — which reaches its broker **through MassTransit** (the `MassTransit.RabbitMQ` transport) — Kafka is realized through the **native Confluent.Kafka** producer and consumer, not the MassTransit Kafka Rider. The deciding factor is Streaming's defining concern: explicit consumer-group and partition assignment plus manual offset-commit control. The MassTransit Kafka Rider abstracts those exact mechanics away, so the native client is used to keep commit timing and group membership under the provider's own control. This is a deliberate asymmetry with the RabbitMQ provider and is stated here so later work does not try to re-route Streaming through MassTransit.

The work gives the shell a live broker by providing production implementations of the four seam ports the shell was deliberately left open against, each a thin wrapper over the native Confluent.Kafka client: the producer (`IKafkaProducer` — produce an envelope to a topic with durability and idempotence), the consumer (`IKafkaConsumer` — consumer-group membership, partition assignment, and manual offset commit with auto-commit disabled), the stream message processor (`IStreamMessageProcessor` — the single point where the core's correlation, idempotency, contract binding, retry/classification, and dead-letter steps run ahead of an application handler), and the transport probe (`IKafkaTransportProbe` — cluster-metadata reachability via the admin client). The work also adds the hosted-service consume loop that drives the shell's deterministic `ConsumeOnceAsync` iteration while the host is running.

The Messaging Core remains transport-neutral (ports and adapters): the active provider is selected at startup by the configured `MessagingMode` through the provider registry, and the Kafka provider backs exactly one mode — Streaming. This feature makes Kafka the concrete transport under that mode, validates Kafka-specific settings fail-fast at startup before any record is processed, participates in the core's existing observability (structured logs, metrics, tracing, readiness) without logging payloads or secrets, and is verified against a real Kafka broker in containers. Security material (SASL credentials, TLS) is injected from the environment or a secret store at runtime and is never committed or logged; the provider defaults to PLAINTEXT so local docker-compose and Testcontainers run unchanged.

Scope boundary (what this feature covers):
- Production implementation of `IKafkaProducer` over a native Confluent.Kafka producer configured for durable, idempotent writes (acks=all, idempotence enabled).
- Production implementation of `IKafkaConsumer` over a native Confluent.Kafka consumer with auto-commit disabled, earliest offset reset, and exact-offset manual commit.
- Production implementation of `IStreamMessageProcessor` that drives the Application-layer consume pipeline (correlation propagation, idempotency gate, contract binding, handler invocation, failure classification, retry, dead-letter routing) ahead of the application handler, identically to every other mode.
- Production implementation of `IKafkaTransportProbe` that checks cluster reachability through the admin client's cluster metadata.
- A hosted-service consume loop that pumps the shell's `ConsumeOnceAsync` while the host is Running, with graceful drain on shutdown and a continue-on-failure policy for uncommitted records.
- Kafka-specific required-settings validation integrated into the existing startup validate-then-resolve gate, with injected-only SASL/TLS security defaulting to PLAINTEXT.
- Participation in the existing observability stack (logs, metrics, tracing, readiness/transport health) with payload and secret redaction.
- Containerized integration verification against a real Kafka broker (`confluentinc/cp-kafka:7.6.1` via Testcontainers) and a docker-compose Kafka service for local development.

Scope boundary (what this feature does NOT cover):
- Realizing Streaming through the MassTransit Kafka Rider (explicitly rejected in favor of the native client).
- Kafka Streams / ksqlDB stream-processing topologies, exactly-once transactional producers, or stateful stream joins.
- Deep partition-assignment-strategy tuning, custom rebalance-callback orchestration, offset-retention/replay policies, or topic auto-creation/administration beyond the reachability probe.
- Schema-registry integration or Avro/Protobuf serialization (the envelope carries its own payload bytes and headers).
- Changes to the transport-neutral core contracts, the Publish-Subscribe, Message Queue, or Batch Processing modes, or the RabbitMQ / SQS / Azure Service Bus / in-memory transports.
- The internal mechanics of the Application-layer consume pipeline, retry executor, dead-letter router, or observability facade, which are defined by the Messaging Core spec and are consumed here unchanged.
- Changing the pinned Confluent.Kafka client version (2.15.1) or the pinned broker image version (`confluentinc/cp-kafka:7.6.1`).

## Glossary

- **Kafka_Provider**: The feature delivered here — the `Messaging.Kafka` adapter shell completed with live native-Confluent.Kafka seam implementations and a hosted consume loop so the Streaming mode runs over a real Kafka broker.
- **Kafka_Messaging_Provider**: The existing `KafkaMessagingProvider` (an `IMessagingProvider`) that backs the Streaming mode; the Kafka_Provider is its native-Confluent.Kafka realization.
- **Native_Kafka_Client**: The Confluent.Kafka producer and consumer (package `Confluent.Kafka`, pinned at version 2.15.1) used directly, rather than the MassTransit Kafka Rider.
- **Kafka_Producer**: The production implementation of `IKafkaProducer` over a native Confluent.Kafka producer — the seam the publisher produces an envelope to a topic through.
- **Kafka_Consumer**: The production implementation of `IKafkaConsumer` over a native Confluent.Kafka consumer — the seam owning consumer-group membership, partition assignment, and manual offset commit.
- **Stream_Message_Processor**: The production implementation of `IStreamMessageProcessor` that runs the core consume steps for each consumed Message_Envelope ahead of the application handler.
- **Transport_Probe**: The production implementation of `IKafkaTransportProbe` that reports whether the Kafka cluster is reachable, backing the readiness probe.
- **Kafka_Consumer_Host**: The existing `KafkaConsumerHost` shell owning the deterministic consume → process → commit iteration (`ConsumeOnceAsync`) where the offset is committed manually only after the core pipeline completes.
- **Kafka_Consumer_Hosted_Service**: The `IHostedService` that drives `ConsumeOnceAsync` in a loop while the Kafka_Consumer_Host is Running, with graceful drain on shutdown.
- **Consume_Iteration**: One deterministic `ConsumeOnceAsync` cycle: poll one record, run it through the Stream_Message_Processor, then commit its offset — strictly in that order.
- **Offset_Commit**: The manual commit that advances the consumer group's committed offset past a record; performed only after the core pipeline completes successfully (auto-commit is disabled).
- **Message_Envelope**: The transport-neutral representation of a message carrying payload plus metadata (message id, correlation id, message type, UTC creation timestamp, contract version, headers).
- **Messaging_Mode**: A category of messaging behavior; for this feature the only relevant value is Streaming.
- **Streaming_Mode**: The Messaging_Mode in which records are consumed from a partitioned, offset-tracked log by a consumer group, realized over Kafka via the Native_Kafka_Client.
- **Provider_Registry**: The component that maps a configured Messaging_Mode to its provider and resolves the active provider at startup.
- **Kafka_Provider_Settings**: The provider-specific settings (`KafkaProviderSettings`) binding the bootstrap servers, the consumer group id, the topic, and the injected-only security settings.
- **Bootstrap_Servers**: The non-secret `host:port[,host:port...]` list locating the Kafka cluster; used as the broker's display name in diagnostics.
- **Consumer_Group_Id**: The consumer group the Kafka_Consumer joins; group membership is what gives Streaming its partition-assignment and offset-tracking semantics.
- **Topic**: The Kafka topic the Kafka_Producer produces to and the Kafka_Consumer subscribes to.
- **Security_Protocol**: The optional transport security selector (Plaintext, Ssl, SaslPlaintext, or SaslSsl); Plaintext is the default.
- **Sasl_Mechanism**: The optional SASL mechanism (None, Plain, ScramSha256, or ScramSha512) used when a SASL Security_Protocol is selected.
- **Sasl_Credentials**: The secret-bearing SASL username and password, injected from the environment or secret store; referenced by setting name and never logged.
- **Tls_Ca_Location**: The optional path to the TLS certificate-authority material used when a TLS Security_Protocol is selected.
- **Startup_Validator**: The existing `MessagingStartupValidator` that validates configuration and resolves the active provider fail-fast before any record is processed.
- **Observability_Context**: The existing core facade applying uniform structured logging, metrics, and tracing around every message operation, with best-effort emission and payload redaction.
- **Transport_Health**: The reachability result the provider reports to back the readiness probe, naming the unavailable dependency when the cluster cannot be reached.
- **Dead_Letter_Sink**: The destination for a Poison_Message that cannot be processed after the Retry_Policy is exhausted.
- **Dead_Letter_Router**: The core component that routes a message to the Dead_Letter_Sink and returns a routing result of RoutedAndAcknowledged or RetainedUnacknowledged.
- **Retry_Policy**: The configured strategy (attempt count, backoff, per-attempt delay) governing reprocessing of transiently failed messages, owned by the core and consumed here.
- **Poison_Message**: A message that repeatedly fails processing and is routed to the Dead_Letter_Sink.
- **Failure_Classifier**: The core component that classifies a processing failure as transient or non-transient.
- **Correlation_Id**: An identifier propagated across the producer, the Messaging Core, and the consumer to trace one logical operation.
- **Idempotency_Key**: A business or message key used to detect and discard duplicate deliveries under at-least-once delivery.
- **Kafka_Broker_Container**: The ephemeral `confluentinc/cp-kafka:7.6.1` container provisioned via Testcontainers for integration verification.

## Requirements

### Requirement 1: Kafka Transport Realized Through the Native Confluent.Kafka Client

**User Story:** As a developer, I want Kafka provided through the native Confluent.Kafka client rather than MassTransit, so that explicit consumer-group/partition assignment and manual offset-commit control remain under the provider's own control.

#### Acceptance Criteria

1. THE Kafka_Provider SHALL realize the Kafka transport through the Native_Kafka_Client and SHALL NOT route the Streaming_Mode through the MassTransit Kafka Rider.
2. THE Kafka_Provider SHALL provide a production implementation of the Kafka_Producer seam that produces a Message_Envelope to the configured Topic with broker acknowledgement from all in-sync replicas and producer idempotence enabled.
3. THE Kafka_Provider SHALL provide a production implementation of the Kafka_Consumer seam that joins the Consumer_Group_Id, takes a partition assignment, disables auto-commit, and commits the exact offset of a processed record through a manual Offset_Commit.
4. WHEN the Kafka_Consumer takes a partition assignment for which no committed offset exists, THE Kafka_Consumer SHALL reset to the earliest offset of that partition.
5. IF a produce of a Message_Envelope does not receive broker acknowledgement from all in-sync replicas, THEN THE Kafka_Producer SHALL treat the produce as failed, SHALL NOT report it as successfully produced, and SHALL surface an error indication identifying the Topic while excluding the payload bytes.
6. THE Kafka_Provider SHALL provide a production implementation of the Stream_Message_Processor seam that, for each consumed Message_Envelope and ahead of the application handler, drives the core consume steps: Correlation_Id propagation, the idempotency gate, contract binding, handler invocation, and failure classification with dead-letter routing.
7. THE Kafka_Provider SHALL provide a production implementation of the Transport_Probe seam that checks Kafka cluster reachability by requesting cluster metadata through the admin client.
8. THE Kafka_Producer, Kafka_Consumer, Stream_Message_Processor, and Transport_Probe SHALL reside in the `Messaging.Kafka` Infrastructure package and SHALL depend on the core contracts only through the ports declared in the inward Domain and Application layers.
9. THE Kafka_Provider SHALL reference the Confluent.Kafka client at the pinned version 2.15.1 and SHALL NOT change that pinned version.

### Requirement 2: Provider Selection and Resolution for Kafka

**User Story:** As a service operator, I want Kafka selected through configuration for the Streaming mode, so that I can deploy the same artifact against Kafka without code changes.

#### Acceptance Criteria

1. WHERE Kafka_Provider_Settings are registered for the Streaming_Mode, THE Kafka_Provider SHALL register exactly one Kafka_Messaging_Provider for the Streaming_Mode so the Provider_Registry resolves it at startup.
2. WHEN the Messaging Core starts with Kafka selected for the Streaming_Mode, THE Provider_Registry SHALL resolve exactly one Kafka_Messaging_Provider for the Streaming_Mode.
3. IF zero providers or more than one provider is registered for the Streaming_Mode, THEN THE Provider_Registry SHALL fail startup before the Messaging Core enters a processing-ready state and SHALL report the unresolved or ambiguous mapping naming the Streaming_Mode.
4. IF the configured Messaging_Mode resolved to the Kafka_Provider is any mode other than the Streaming_Mode, THEN THE Kafka_Provider SHALL fail startup before the Messaging Core enters a processing-ready state, SHALL NOT register a partial provider, and SHALL report an error indicating that Kafka backs only the Streaming mode.
5. WHILE the Messaging Core instance is running, THE Provider_Registry SHALL return the same reference-identical Kafka_Messaging_Provider instance on every resolution of the Streaming_Mode, and the mode-to-provider mapping SHALL NOT change for the lifetime of the instance.

### Requirement 3: Kafka Startup Configuration Validation

**User Story:** As a service operator, I want Kafka settings validated at startup, so that a missing or invalid cluster configuration fails fast instead of surfacing during record processing.

#### Acceptance Criteria

1. WHEN the Messaging Core starts with the Kafka_Messaging_Provider active, THE Startup_Validator SHALL validate all Kafka-specific required settings — the Bootstrap_Servers, the Consumer_Group_Id, and the Topic — in a single pass and SHALL complete that validation before the Messaging Core produces or consumes any record.
2. IF a required Kafka setting is absent from the Kafka_Provider_Settings, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the missing setting by its configuration setting name.
3. IF the Bootstrap_Servers, the Consumer_Group_Id, or the Topic holds a value that is empty or whitespace-only, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name.
4. IF an enumerated selector setting (the Security_Protocol or the Sasl_Mechanism) holds a value outside its permitted set, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name together with its permitted set.
5. WHEN two or more Kafka settings are missing or invalid in the same startup, THE Startup_Validator SHALL report a separate configuration error for each missing or invalid setting rather than stopping at the first failure.
6. WHEN the Startup_Validator reports any Kafka configuration failure, THE Startup_Validator SHALL identify each affected secret-bearing setting (including the Sasl_Credentials) by its configuration setting name and SHALL exclude that setting's configured value, in whole and in every fragment, from every reported configuration error.
7. WHEN Kafka startup validation completes with zero missing required settings and zero invalid setting values, THE Startup_Validator SHALL permit the Messaging Core to enter a processing-ready state.

### Requirement 4: At-Least-Once Delivery via Manual Offset Commit

**User Story:** As a developer, I want the offset committed only after the pipeline succeeds, so that a crash before commit reprocesses the record rather than skipping it.

#### Acceptance Criteria

1. THE Kafka_Consumer SHALL disable auto-commit so that no Offset_Commit occurs unless the Kafka_Consumer_Host explicitly performs it.
2. WHEN a Consume_Iteration polls a record and the Stream_Message_Processor returns for that record without throwing, THE Kafka_Consumer_Host SHALL perform the Offset_Commit for that record, and SHALL perform that Offset_Commit only after the Stream_Message_Processor has returned.
3. IF the Stream_Message_Processor throws while processing a polled record, THEN THE Kafka_Consumer_Host SHALL NOT perform the Offset_Commit for that record.
4. WHEN a Consume_Iteration completes the configured poll timeout (inclusive bounds 1 to 300 seconds, default 5 seconds) without obtaining a record, THE Kafka_Consumer_Host SHALL treat the iteration as a no-op and SHALL perform no Offset_Commit.
5. IF the process is interrupted after the Stream_Message_Processor returns but before the Offset_Commit is performed, THEN on the next consumer-group assignment of that partition THE Kafka_Consumer SHALL deliver the uncommitted record again rather than resuming past it.
6. WHEN the Offset_Commit is performed for a record, THE Kafka_Consumer SHALL advance the committed offset of the record's partition to one position beyond that record's offset.
7. IF the Offset_Commit for a record fails to be acknowledged by the broker, THEN THE Kafka_Consumer_Host SHALL leave the record's committed offset unchanged and SHALL surface an error indication identifying the uncommitted record by its partition and offset, so that record is delivered again on the next poll or consumer-group assignment of its partition rather than being treated as committed.

### Requirement 5: Core-Consistent Consumption Through the Shared Pipeline

**User Story:** As a developer, I want Kafka consumption to run the same core pipeline as every other mode, so that correlation, idempotency, contract binding, retry, and dead-lettering behave identically regardless of transport.

#### Acceptance Criteria

1. WHEN a consumed Message_Envelope is handed to the Stream_Message_Processor, THE Stream_Message_Processor SHALL drive the Application-layer consume pipeline, the Retry_Policy executor, and the Dead_Letter_Router rather than reimplementing correlation, idempotency, binding, retry, classification, or dead-letter routing.
2. WHEN a Message_Envelope is processed over Kafka, THE Stream_Message_Processor SHALL propagate the Correlation_Id carried on the Message_Envelope into the core consume steps as an unchanged, character-for-character identical value.
3. WHERE idempotency is enabled in configuration, WHEN a Message_Envelope is processed over Kafka, THE Stream_Message_Processor SHALL evaluate the Idempotency_Key through the core idempotency gate before invoking the application handler.
4. WHERE idempotency is enabled in configuration AND a consumed Message_Envelope carries an Idempotency_Key already recorded as processed, THE Stream_Message_Processor SHALL NOT invoke the application handler for that Message_Envelope and SHALL complete the record with a successful outcome so its offset is committed.
5. WHERE idempotency is enabled in configuration AND the Idempotency_Key cannot be derived from a consumed Message_Envelope, THEN THE Stream_Message_Processor SHALL reject the Message_Envelope with a missing-idempotency-key error before invoking the application handler and SHALL NOT invoke the application handler.
6. WHEN a Message_Envelope is processed over Kafka, THE Stream_Message_Processor SHALL bind the payload to a known message contract before invoking the application handler.
7. IF a consumed Message_Envelope carries a message type identifier that matches no registered message contract, or a payload that cannot be bound to the contract registered for its message type identifier, THEN THE Stream_Message_Processor SHALL reject the Message_Envelope through the dead-letter path, SHALL NOT invoke the application handler with the payload, and SHALL surface an error indication identifying the unknown or unbindable contract.
8. WHEN a processing failure occurs over Kafka, THE Stream_Message_Processor SHALL classify the failure through the core Failure_Classifier and SHALL route a transient failure to retry and a non-transient failure to the Dead_Letter_Sink per the classifier result.

### Requirement 6: Commit-Gated Dead-Letter Retain Semantics

**User Story:** As a developer, I want the offset committed only once a record is safely terminal, so that a record whose dead-letter sink failed is reprocessed rather than silently dropped.

#### Acceptance Criteria

1. WHEN the core pipeline for a consumed record completes with a successful outcome, THE Stream_Message_Processor SHALL return without throwing so the Kafka_Consumer_Host performs the Offset_Commit and advances past the record.
2. WHEN the core pipeline routes a record to the Dead_Letter_Sink and the Dead_Letter_Router returns RoutedAndAcknowledged, THE Stream_Message_Processor SHALL return without throwing so the Kafka_Consumer_Host performs the Offset_Commit and advances past the record.
3. IF the core pipeline routes a record to the Dead_Letter_Sink and the Dead_Letter_Router returns RetainedUnacknowledged because the Dead_Letter_Sink itself failed to accept the record, THEN THE Stream_Message_Processor SHALL throw so the Kafka_Consumer_Host does NOT perform the Offset_Commit for that record and the committed offset of the record's partition remains unchanged.
4. WHEN the Stream_Message_Processor throws for a RetainedUnacknowledged record, THE Kafka_Consumer SHALL deliver that same uncommitted record, at the same partition offset, again on the next poll or on the next consumer-group assignment of its partition.
5. WHEN a consumed record reaches a terminal outcome of either successful processing or a Dead_Letter_Router result of RoutedAndAcknowledged, THE Kafka_Consumer_Host SHALL advance the committed offset of that record's partition to one position beyond the record's offset, and in every other outcome SHALL leave the committed offset of that partition unchanged.

### Requirement 7: Retry, Backoff, and Dead-Letter for Poison Messages

**User Story:** As a service operator, I want transient failures retried with backoff and persistent failures dead-lettered through the single core policy, so that native-client redelivery does not multiply the attempt count.

#### Acceptance Criteria

1. IF processing a consumed Message_Envelope fails with an error the Failure_Classifier classifies as transient, THEN THE Stream_Message_Processor SHALL re-invoke processing of the same Message_Envelope under the configured Retry_Policy and SHALL stop re-invoking at the first attempt that completes without throwing or when the configured maximum attempt count (Retry_Policy.MaxAttempts, inclusive bounds 1 to 100, default 5) has been performed, whichever occurs first.
2. WHILE retrying a transiently failed Message_Envelope, THE Stream_Message_Processor SHALL perform the first attempt with no preceding delay and SHALL precede each attempt after the first by a delay computed from the configured base per-attempt delay (Retry_Policy.PerAttemptDelaySeconds, inclusive bounds 1 to 300 seconds, default 5 seconds) under the configured backoff strategy.
3. IF processing a consumed Message_Envelope fails with an error the Failure_Classifier classifies as non-transient, THEN THE Stream_Message_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink without any further retry attempt.
4. WHEN the configured Retry_Policy maximum attempt count is reached for a transiently failed Message_Envelope, THE Stream_Message_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink within 5 seconds of the final failed attempt.
5. WHEN a Message_Envelope is routed to the Dead_Letter_Sink, THE Stream_Message_Processor SHALL preserve on the dead-lettered message the Correlation_Id unchanged character-for-character, the message id, the payload equal byte-for-byte to the consumed payload, and the total attempt count performed, where the total attempt count equals 1 for a Message_Envelope routed without any retry.
6. THE Stream_Message_Processor SHALL apply the core Retry_Policy as the single source of retry and backoff behavior such that the total number of processing attempts performed for any one consumed Message_Envelope is at least 1 and at most the configured maximum attempt count, and SHALL NOT configure any additional Native_Kafka_Client redelivery policy.

### Requirement 8: Hosted-Service Consume Loop with Graceful Drain

**User Story:** As a service operator, I want a hosted consume loop that pumps the consume iteration while the host runs and drains cleanly on shutdown, so that records are processed continuously without tearing the host down on a single uncommitted record.

#### Acceptance Criteria

1. WHILE the Kafka_Consumer_Host is in the Running state, THE Kafka_Consumer_Hosted_Service SHALL invoke a new Consume_Iteration within 1 second of the previous Consume_Iteration returning, so polled records are processed in an uninterrupted loop.
2. WHILE the Kafka_Consumer_Host is not in the Running state, THE Kafka_Consumer_Hosted_Service SHALL NOT invoke the Consume_Iteration.
3. WHEN the hosting application signals shutdown, THE Kafka_Consumer_Hosted_Service SHALL stop invoking new Consume_Iterations, SHALL allow an in-flight Consume_Iteration to complete its process-then-commit, and SHALL stop the Kafka_Consumer_Host within the configured graceful-shutdown timeout (inclusive bounds 1 to 300 seconds, default 30 seconds).
4. IF an in-flight Consume_Iteration has not completed its process-then-commit when the configured graceful-shutdown timeout elapses, THEN THE Kafka_Consumer_Hosted_Service SHALL stop the Kafka_Consumer_Host without performing the Offset_Commit for the uncompleted record and SHALL emit a structured log entry carrying the Correlation_Id, excluding the payload bytes, and indicating that the drain exceeded the graceful-shutdown timeout.
5. IF a Consume_Iteration throws because a record was retained unacknowledged or the Dead_Letter_Sink failed, THEN THE Kafka_Consumer_Hosted_Service SHALL emit a structured log entry carrying the Correlation_Id and excluding the payload bytes, SHALL delay the next Consume_Iteration by a backoff interval (inclusive bounds 1 to 60 seconds, default 5 seconds), and SHALL continue the consume loop rather than terminating the Kafka_Consumer_Host.
6. WHEN a Consume_Iteration throws for an uncommitted record and the loop continues, THE Kafka_Consumer_Hosted_Service SHALL invoke a subsequent Consume_Iteration that re-polls that same uncommitted record, so that the record is reprocessed rather than skipped because no Offset_Commit advanced past it.
7. IF the Native_Kafka_Client reports a consumer-group rebalance during the consume loop, THEN THE Kafka_Consumer_Hosted_Service SHALL continue invoking the Consume_Iteration without terminating the Kafka_Consumer_Host.

### Requirement 9: Correlation and Envelope Fidelity Over Kafka

**User Story:** As a developer, I want every envelope field preserved across Kafka, so that a consumer reconstructs the same message and a single logical operation stays traceable end to end.

#### Acceptance Criteria

1. WHEN a Message_Envelope is produced over Kafka and subsequently consumed, THE Kafka_Provider SHALL carry the payload as the Kafka record value such that the consumed payload byte sequence is equal, in length and in every byte position, to the produced payload byte sequence, including the boundary case in which the produced payload is a zero-length byte sequence.
2. WHEN a Message_Envelope is produced over Kafka and subsequently consumed, THE Kafka_Provider SHALL carry the message id, Correlation_Id, message type identifier, UTC creation timestamp, and contract version as Kafka record headers whose values are each equal to the corresponding values on the produced Message_Envelope, with the UTC creation timestamp equal at the full precision carried on the produced Message_Envelope without truncation or rounding.
3. WHEN a Message_Envelope is produced over Kafka and subsequently consumed, THE Kafka_Provider SHALL carry the user header collection as Kafka record headers such that the consumed header collection contains exactly the same number of user-header entries as the produced collection, with each header key present and mapped to a value equal to its produced value, including the boundary case in which the produced user header collection contains zero entries.
4. WHEN a Message_Envelope is produced over Kafka and subsequently consumed, THE Kafka_Provider SHALL make the consumed Message_Envelope equal field-for-field and byte-for-byte to the produced Message_Envelope.
5. WHEN a Message_Envelope is consumed over Kafka, THE Stream_Message_Processor SHALL propagate the Correlation_Id carried on the consumed Message_Envelope into the core consume steps as an unchanged, character-for-character identical value such that the consume trace and the originating produce trace share one equal Correlation_Id value.
6. IF a consumed Kafka record is missing a required metadata header (the message id, Correlation_Id, message type identifier, UTC creation timestamp, or contract version) or carries such a header whose value cannot be read back into its Message_Envelope field, THEN THE Kafka_Provider SHALL reject the record through the dead-letter path, SHALL NOT hand a partially reconstructed Message_Envelope to the application handler, and SHALL surface an error indication identifying the missing or unreadable required header.

### Requirement 10: Observability Participation Without Secret Exposure

**User Story:** As a service operator, I want to log and monitor the Kafka integration, so that I can observe cluster state and record activity without exposing payloads or secrets.

#### Acceptance Criteria

1. WHEN a Message_Envelope is produced over Kafka, THE Kafka_Provider SHALL record a publish event through the Observability_Context carrying an event type classification from the set {Publish, Consume, Retry, DeadLetter}, the Correlation_Id, and a UTC timestamp.
2. WHEN a Message_Envelope is consumed over Kafka, THE Kafka_Provider SHALL record a consume event through the Observability_Context carrying an event type classification from the set {Publish, Consume, Retry, DeadLetter}, the Correlation_Id, and a UTC timestamp.
3. WHEN the Kafka_Provider emits a log entry or trace that references the cluster, THE Kafka_Provider SHALL reference the cluster only by its non-secret Bootstrap_Servers display value, SHALL exclude the Sasl_Credentials values, and SHALL exclude the message payload bytes.
4. WHEN the Kafka_Provider includes a sensitive field value in an observability record, THE Kafka_Provider SHALL replace that value with a fixed redaction marker that shares no character-sequence fragment with the original value and does not reveal the original value's length.
5. WHEN a Message_Envelope is produced over Kafka, THE Kafka_Provider SHALL carry the Correlation_Id in the record headers so a consume trace links to the originating produce trace by an equal Correlation_Id value.
6. IF emitting an observability record fails, THEN THE Kafka_Provider SHALL suppress the failure and SHALL complete the produce or consume operation with its normal outcome without propagating the failure.

### Requirement 11: Transport Reachability and Readiness

**User Story:** As a service operator, I want readiness to reflect Kafka reachability, so that traffic is not routed to an instance that cannot reach the cluster.

#### Acceptance Criteria

1. WHEN the readiness probe queries the active provider, THE Transport_Probe SHALL request Kafka cluster metadata through the admin client and SHALL return a Transport_Health result reporting whether the Kafka cluster is reachable within a fixed reachability-check timeout of 1 second measured from the query.
2. WHEN the admin client returns cluster metadata listing at least one reachable broker within the 1-second reachability-check timeout, THE Transport_Probe SHALL return a Transport_Health result whose reachable indicator is true and whose unavailable-dependency field is unset.
3. IF the admin client returns cluster metadata listing no reachable broker within the 1-second reachability-check timeout, THEN THE Transport_Probe SHALL return a Transport_Health result whose reachable indicator is false and that names the unavailable dependency as "Kafka (bootstrap servers)" using the non-secret Bootstrap_Servers display value and excluding any Sasl_Credentials value.
4. IF requesting cluster metadata through the admin client raises an error, THEN THE Transport_Probe SHALL return a Transport_Health result whose reachable indicator is false that names the unavailable dependency as "Kafka (bootstrap servers)" using the non-secret Bootstrap_Servers display value and excluding any Sasl_Credentials value, and SHALL NOT propagate the error to the readiness probe.
5. IF the reachability check does not complete within the 1-second reachability-check timeout, THEN THE Transport_Probe SHALL return a Transport_Health result whose reachable indicator is false that names the unavailable dependency as "Kafka (bootstrap servers)" using the non-secret Bootstrap_Servers display value and excluding any Sasl_Credentials value, and SHALL NOT propagate a timeout error to the readiness probe.

### Requirement 12: Injected-Only Security Defaulting to PLAINTEXT

**User Story:** As a service operator, I want Kafka security configured through injected credentials with a PLAINTEXT default, so that secured clusters are supported in production while local development and tests run unchanged.

#### Acceptance Criteria

1. WHERE no Security_Protocol is configured, THE Kafka_Provider SHALL default the Security_Protocol to Plaintext so that local docker-compose and containerized integration verification connect without SASL or TLS material.
2. WHERE a Security_Protocol is configured, THE Kafka_Provider SHALL accept the configured Security_Protocol only when its value is exactly one of the permitted values Plaintext, Ssl, SaslPlaintext, or SaslSsl.
3. WHERE a Sasl_Mechanism is configured, THE Kafka_Provider SHALL accept the configured Sasl_Mechanism only when its value is exactly one of the permitted values None, Plain, ScramSha256, or ScramSha512.
4. IF a configured Security_Protocol or Sasl_Mechanism holds a value that is empty, whitespace-only, or outside its permitted set, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name together with its permitted set.
5. WHERE a SASL Security_Protocol (SaslPlaintext or SaslSsl) is configured, THE Kafka_Provider SHALL require a Sasl_Mechanism other than None together with a Sasl_Credentials username and a Sasl_Credentials password.
6. IF a SASL Security_Protocol (SaslPlaintext or SaslSsl) is configured and the Sasl_Mechanism is absent or None, or either the Sasl_Credentials username or the Sasl_Credentials password is absent, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a separate configuration error for each absent or invalid setting, identifying each by its configuration setting name and excluding every Sasl_Credentials value from the reported errors.
7. WHERE a TLS Security_Protocol (Ssl or SaslSsl) is configured and a Tls_Ca_Location is present, THE Kafka_Provider SHALL apply the configured Tls_Ca_Location so the client verifies the broker certificate against that certificate-authority material.
8. WHERE a TLS Security_Protocol (Ssl or SaslSsl) is configured and no Tls_Ca_Location is present, THE Kafka_Provider SHALL verify the broker certificate against the host's default trusted certificate-authority material.
9. THE Kafka_Provider SHALL obtain the Sasl_Credentials and Tls_Ca_Location from the runtime environment or secret store and SHALL NOT require those values to be embedded in source or in a committed configuration file.
10. WHEN the Kafka_Provider emits any log entry or trace, THE Kafka_Provider SHALL exclude the Sasl_Credentials values and SHALL reference a secret-bearing security setting only by its configuration setting name.

### Requirement 13: Containerized Verification Against a Real Broker

**User Story:** As a developer, I want Kafka behavior verified against a real broker in containers, so that transport-level behavior such as manual-offset-commit redelivery surfaces before production.

#### Acceptance Criteria

1. THE Kafka_Provider SHALL be verified by integration tests that run against a real Kafka broker provided by a Kafka_Broker_Container rather than against an in-memory transport substitute.
2. WHERE the Kafka broker container image is specified for verification, THE verification SHALL reference the image as `confluentinc/cp-kafka:7.6.1` by that explicit fixed-release version tag and SHALL NOT reference a floating latest tag.
3. WHEN the integration test suite runs, THE Kafka_Provider verification SHALL provision the Kafka_Broker_Container through Testcontainers and SHALL wait for the broker to become ready to accept connections for up to a readiness timeout within the inclusive bounds 1 to 300 seconds (default 60 seconds) before the assertions run.
4. IF the Kafka_Broker_Container does not become ready to accept connections within the readiness timeout, THEN THE Kafka_Provider verification SHALL fail with a timeout error identifying the Kafka_Broker_Container as the unready dependency and SHALL NOT run the assertions.
5. WHEN an integration test run completes, whether it succeeds or fails, THE Kafka_Provider verification SHALL tear down the ephemeral Kafka_Broker_Container.
6. THE Kafka_Provider integration verification SHALL cover at-least-once redelivery that asserts a record whose processing fails before its offset is committed is redelivered, within 60 seconds of the failed processing, to a consumer in the same Consumer_Group_Id.
7. THE Kafka_Provider integration verification SHALL cover Correlation_Id propagation that asserts the Correlation_Id of a consumed Message_Envelope is equal, character-for-character, to the Correlation_Id of the produced Message_Envelope end to end.
8. THE Kafka_Provider integration verification SHALL cover the success path that asserts a successfully processed record has its offset committed and is NOT redelivered to a newly started consumer in the same Consumer_Group_Id observed over a window of at least 30 seconds after that consumer reaches its partition assignment.

### Requirement 14: Local Infrastructure for Streaming Development

**User Story:** As a developer, I want a docker-compose Kafka service, so that `docker compose up` gives me a working local Streaming environment in one command.

#### Acceptance Criteria

1. WHEN a developer starts the docker-compose Kafka service with a single `docker compose up` command, THE Kafka_Provider SHALL supply a docker-compose Kafka service using the same `confluentinc/cp-kafka` image family as the integration verification, and THE docker-compose Kafka service SHALL become ready to accept client connections within 60 seconds of start.
2. WHERE the docker-compose Kafka service specifies the broker image, THE service SHALL reference the image as `confluentinc/cp-kafka:7.6.1` by that explicit fixed-release version tag, identical to the tag used by the integration verification, and SHALL NOT reference a floating latest tag.
3. WHERE the docker-compose Kafka service is used for local development, THE service SHALL expose the broker over the Plaintext Security_Protocol so a local consumer and publisher connect without SASL or TLS material.
4. THE docker-compose Kafka service SHALL NOT embed any Sasl_Credentials, TLS private key, or other secret value in a committed compose or configuration file.
5. WHEN a developer runs the Messaging Core locally in the Streaming_Mode against the docker-compose Kafka service using the Bootstrap_Servers, Consumer_Group_Id, and Topic supplied by the local configuration, THE Kafka_Provider SHALL connect to the broker at those Bootstrap_Servers without any additional code change.
6. WHILE connected to the docker-compose Kafka service in the Streaming_Mode, THE Kafka_Provider SHALL act as a publisher, a consumer, or both as determined by the local configuration, without any additional code change.
7. IF the Kafka_Provider cannot reach the docker-compose Kafka service at the configured Bootstrap_Servers at startup, THEN THE Kafka_Provider SHALL fail startup before entering a processing-ready state and SHALL report an error naming the unavailable dependency by its non-secret Bootstrap_Servers display value.
