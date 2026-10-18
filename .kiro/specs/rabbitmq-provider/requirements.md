# Requirements Document

## Introduction

The RabbitMQ Provider implements RabbitMQ as a concrete messaging transport for the Messaging Core library by filling in the existing `Messaging.MassTransit` adapter shell. RabbitMQ is reached **through MassTransit** (the `MassTransit.RabbitMQ` transport, which depends transitively on the official `RabbitMQ.Client`); this feature does not build a separate adapter directly on `RabbitMQ.Client`. The work gives the shell a live broker by providing production implementations of the two seam ports the shell was deliberately left open against: the bus adapter (`IMassTransitBusAdapter` — bus start/stop, publish/send, transport reachability) and the consume processor (`IMassTransitConsumeProcessor` — the single point where the core's correlation, idempotency, retry/classification, and dead-letter steps run ahead of an application handler).

The Messaging Core remains transport-neutral (ports and adapters): the active provider is selected at startup by the configured `Messaging_Mode` through the provider registry, and the MassTransit provider backs the broker-shaped modes — Publish-Subscribe (RabbitMQ fan-out) and Message Queue (point-to-point). This feature makes RabbitMQ the concrete transport under those modes and wires it so a service embedding the library can act as a publisher, a consumer, or both, without starting machinery it does not use.

The feature also ensures RabbitMQ-specific settings are validated fail-fast at startup before any message is processed, that the integration participates in the core's existing observability (structured logs, metrics, tracing, readiness) without logging secrets, and that it is verified against a real RabbitMQ broker in containers locally and in CI. In production the provider defaults to being ready to use a hosted "Azure RabbitMQ" broker, with credentials injected from the environment or a secret store at runtime.

Scope boundary (what this feature covers):
- Production implementation of `IMassTransitBusAdapter` over a configured MassTransit RabbitMQ bus (start/stop, publish for Publish-Subscribe, send for Message Queue, reachability probe).
- Production implementation of `IMassTransitConsumeProcessor` that drives the Application-layer consume pipeline (correlation propagation, idempotency gate, contract binding, handler invocation, failure classification, retry, dead-letter routing) ahead of the application handler.
- DI registration that stands up only the publish side, only the consume side, or both, matching the embedding application's role.
- RabbitMQ-specific required-settings validation integrated into the existing startup validate-then-resolve gate.
- Participation in the existing observability stack (logs, metrics, tracing, readiness/transport health) with payload/secret redaction.
- Containerized integration verification against a real RabbitMQ broker (Testcontainers locally, docker-compose acceptable in CI) and default readiness for a hosted Azure RabbitMQ broker in production.

Scope boundary (what this feature does NOT cover):
- RabbitMQ RPC (request/response messaging).
- IoT messaging scenarios.
- RabbitMQ Streams / streaming consumption.
- Changes to the transport-neutral core contracts, the Streaming or Batch Processing modes, or the SQS / Azure Service Bus / in-memory transports.
- The internal mechanics of the Application-layer consume pipeline, outbox, retry executor, or observability facade, which are defined by the Messaging Core spec and are consumed here unchanged.

## Glossary

- **RabbitMQ_Provider**: The feature delivered here — the MassTransit adapter shell completed with live RabbitMQ seam implementations so the broker-shaped modes run over a real RabbitMQ broker.
- **MassTransit_Provider**: The existing `MassTransitMessagingProvider` (an `IMessagingProvider`) that backs the Publish-Subscribe and Message Queue modes; the RabbitMQ_Provider is the RabbitMQ realization of its transport seams.
- **MassTransit_RabbitMQ_Transport**: The MassTransit transport (package `MassTransit.RabbitMQ`) that connects MassTransit to a RabbitMQ broker and that depends transitively on the official RabbitMQ client library.
- **Bus_Adapter**: The production implementation of `IMassTransitBusAdapter` over a configured MassTransit RabbitMQ bus — the seam exposing bus start/stop, publish/send, and transport reachability.
- **Consume_Processor**: The production implementation of `IMassTransitConsumeProcessor` that runs the core consume steps for each delivered Message_Envelope ahead of the application handler.
- **Messaging_Mode**: A category of messaging behavior; for this feature the relevant values are Publish-Subscribe and Message Queue.
- **Publish_Subscribe_Mode**: The Messaging_Mode in which one message is fanned out to many subscribers, realized over RabbitMQ via MassTransit's publish endpoint.
- **Message_Queue_Mode**: The Messaging_Mode in which a message is delivered point-to-point to one of competing consumers, realized over RabbitMQ via MassTransit's send endpoint.
- **Message_Envelope**: The transport-neutral representation of a message carrying payload plus metadata (message id, correlation id, message type, UTC creation timestamp, contract version, headers).
- **Provider_Registry**: The component that maps a configured Messaging_Mode to its provider and resolves the active provider at startup.
- **MassTransit_Provider_Settings**: The provider-specific settings (`MassTransitProviderSettings`) binding the transport selector, the broker connection string (secret-bearing), and a non-secret host display name.
- **Connection_String**: The secret-bearing value locating and authenticating to the RabbitMQ broker; referenced by its configuration setting name in diagnostics and never logged.
- **Azure_RabbitMQ**: A hosted RabbitMQ broker running on Azure that the RabbitMQ_Provider is configured to use in production, with credentials injected at runtime.
- **Startup_Validator**: The existing `MessagingStartupValidator` that validates configuration and resolves the active provider fail-fast before any message is processed.
- **Observability_Context**: The existing core facade applying uniform structured logging, metrics, and tracing around every message operation, with best-effort emission and payload redaction.
- **Transport_Health**: The reachability result the provider reports to back the readiness probe, naming the unavailable dependency when the broker cannot be reached.
- **Dead_Letter_Sink**: The destination for a Poison_Message that cannot be processed after the Retry_Policy is exhausted.
- **Retry_Policy**: The configured strategy (attempt count, backoff, delays) governing reprocessing of transiently failed messages, owned by the core and consumed here.
- **Poison_Message**: A message that repeatedly fails processing and is routed to the Dead_Letter_Sink.
- **Correlation_Id**: An identifier propagated across the producer, the Messaging Core, and the consumer to trace one logical operation.
- **Idempotency_Key**: A business or message key used to detect and discard duplicate deliveries under at-least-once delivery.
- **Publisher_Only_Role**: An embedding application that publishes messages and does not consume them.
- **Consumer_Only_Role**: An embedding application that consumes messages and does not publish them.
- **Publisher_And_Consumer_Role**: An embedding application that both consumes and publishes.

## Requirements

### Requirement 1: RabbitMQ Transport Realized Through MassTransit

**User Story:** As a developer, I want RabbitMQ provided through the existing MassTransit adapter, so that the official RabbitMQ client is used while the established MassTransit pattern in the codebase is preserved.

#### Acceptance Criteria

1. THE RabbitMQ_Provider SHALL realize the RabbitMQ transport through the MassTransit_RabbitMQ_Transport and SHALL NOT introduce a separate adapter built directly on the RabbitMQ client library.
2. THE RabbitMQ_Provider SHALL provide a production implementation of the Bus_Adapter seam exposing bus start, bus stop, message publish/send, and transport reachability over a configured MassTransit RabbitMQ bus.
3. THE RabbitMQ_Provider SHALL provide a production implementation of the Consume_Processor seam that, for each delivered Message_Envelope and ahead of the application handler, runs the core consume steps: Correlation_Id propagation, the idempotency gate, contract binding, handler invocation, and failure classification with dead-letter routing.
4. THE Bus_Adapter and the Consume_Processor SHALL reside in the `Messaging.MassTransit` Infrastructure package and SHALL depend on the core contracts only through the ports declared in the inward Domain and Application layers.
5. WHERE the active Messaging_Mode is Publish_Subscribe_Mode, THE Bus_Adapter SHALL publish a Message_Envelope through the MassTransit publish endpoint.
6. WHERE the active Messaging_Mode is Message_Queue_Mode, THE Bus_Adapter SHALL send a Message_Envelope through the MassTransit send endpoint.
7. WHEN the RabbitMQ broker connection is established and the bus is started, THE Bus_Adapter SHALL report a reachable Transport_Health result with no named unavailable dependency.
8. IF the RabbitMQ broker is unreachable or the reachability probe throws, THEN THE Bus_Adapter SHALL report a not-reachable Transport_Health result naming the broker as the unavailable dependency and SHALL NOT propagate the error to the caller.
9. WHEN a delivered Message_Envelope is processed and the application handler completes without throwing, THE Consume_Processor SHALL acknowledge the message to the broker; IF processing throws, THEN THE Consume_Processor SHALL NOT acknowledge the message as successfully processed.

### Requirement 2: Provider Selection and Resolution for RabbitMQ

**User Story:** As a service operator, I want RabbitMQ selected through configuration, so that I can deploy the same artifact against RabbitMQ without code changes.

#### Acceptance Criteria

1. WHERE MassTransit_Provider_Settings specify the RabbitMQ transport for Publish_Subscribe_Mode or Message_Queue_Mode, THE RabbitMQ_Provider SHALL register exactly one MassTransit_Provider for the configured Messaging_Mode so the Provider_Registry resolves it at startup.
2. WHEN the Messaging Core starts with RabbitMQ selected for Publish_Subscribe_Mode or Message_Queue_Mode, THE Provider_Registry SHALL resolve exactly one MassTransit_Provider for that Messaging_Mode; IF zero or more than one provider is registered for that Messaging_Mode, THEN THE Provider_Registry SHALL fail startup and report the unresolved or ambiguous mapping.
3. IF MassTransit_Provider_Settings specify the RabbitMQ transport for a Messaging_Mode other than Publish_Subscribe_Mode or Message_Queue_Mode, THEN THE RabbitMQ_Provider SHALL fail startup before the Messaging Core enters a processing-ready state, SHALL NOT register a partial provider, and SHALL report an error indicating that RabbitMQ backs only the Publish-Subscribe and Message Queue modes.
4. WHILE the Messaging Core instance is running, THE Provider_Registry SHALL return the same resolved MassTransit_Provider instance on every resolution of the active Messaging_Mode, and the mode-to-provider mapping SHALL NOT change.

### Requirement 3: RabbitMQ Startup Configuration Validation

**User Story:** As a service operator, I want RabbitMQ settings validated at startup, so that a missing or invalid broker configuration fails fast instead of surfacing during message processing.

#### Acceptance Criteria

1. WHEN the Messaging Core starts with the MassTransit_Provider active for the RabbitMQ transport, THE Startup_Validator SHALL validate all RabbitMQ-specific required settings in a single pass and SHALL complete that validation before the Messaging Core publishes or consumes any message.
2. IF the Connection_String required setting is absent from the provider settings, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the missing setting by its configuration setting name.
3. IF a required RabbitMQ setting holds a value outside its permitted set or its inclusive permitted range, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name together with its permitted set or permitted range.
4. WHEN two or more RabbitMQ required settings are missing or invalid in the same startup, THE Startup_Validator SHALL report a separate configuration error for each missing or invalid setting rather than stopping at the first failure.
5. WHEN the Startup_Validator reports any RabbitMQ configuration failure, THE Startup_Validator SHALL identify each affected secret-bearing setting (including Connection_String) by its configuration setting name and SHALL exclude that setting's configured value from every reported configuration error.
6. WHEN RabbitMQ startup validation completes with zero missing required settings and zero invalid setting values, THE Startup_Validator SHALL permit the Messaging Core to enter a processing-ready state.

### Requirement 4: Role-Scoped Composition for Publisher, Consumer, or Both

**User Story:** As a developer, I want the RabbitMQ provider to start only the machinery my service's role needs, so that a publisher-only service runs no consume loop and a consumer-only service stands up no unused publish machinery.

#### Acceptance Criteria

1. WHERE an embedding application is composed in the Publisher_Only_Role, THE RabbitMQ_Provider SHALL enable the Bus_Adapter publish/send path over RabbitMQ.
2. WHERE an embedding application is composed in the Publisher_Only_Role, THE RabbitMQ_Provider SHALL NOT invoke the core consume steps for any delivered message and SHALL NOT register a receive endpoint on the RabbitMQ broker.
3. WHERE an embedding application is composed in the Consumer_Only_Role, THE RabbitMQ_Provider SHALL register exactly one receive endpoint on the RabbitMQ broker and SHALL route each delivered Message_Envelope through the core consume steps.
4. WHERE an embedding application is composed in the Consumer_Only_Role, THE RabbitMQ_Provider SHALL NOT enable an application-initiated publish/send path, while still permitting the core dead-letter send path required to route a Poison_Message.
5. WHERE an embedding application is composed in the Publisher_And_Consumer_Role, THE RabbitMQ_Provider SHALL enable the Bus_Adapter publish/send path and SHALL register exactly one receive endpoint that routes each delivered Message_Envelope through the core consume steps.

### Requirement 5: Bus Lifecycle and Graceful Drain

**User Story:** As a service operator, I want the RabbitMQ bus to start and stop cleanly, so that deployments and restarts do not drop in-flight messages.

#### Acceptance Criteria

1. WHEN the consumer host is signaled to start, THE Bus_Adapter SHALL start the MassTransit RabbitMQ bus and, upon a successful start, SHALL transition the consumer host to the Running state, after which it accepts messages delivered by the broker into the core consume steps.
2. WHEN the consumer host is signaled to stop, THE Bus_Adapter SHALL transition the consumer host out of the Running state and SHALL stop accepting new messages into the core consume steps before draining.
3. WHEN the consumer host has stopped accepting new messages and all in-flight consumers complete within the configured graceful-shutdown timeout (1 to 300 seconds, default 30 seconds), THE Bus_Adapter SHALL stop the MassTransit RabbitMQ bus and SHALL leave the consumer host in the Stopped state.
4. IF the configured graceful-shutdown timeout elapses before all in-flight consumers complete, THEN THE Bus_Adapter SHALL stop the MassTransit RabbitMQ bus, SHALL terminate the consumer host, and SHALL report the exact non-negative count of in-flight messages that had not completed.
5. IF the MassTransit RabbitMQ bus fails to start, THEN THE Bus_Adapter SHALL surface an error indicating the startup failure to the caller and SHALL leave the consumer host in the Stopped (non-running) state.
6. WHILE the consumer host is not in the Running state, THE RabbitMQ_Provider SHALL reject delivery of a new message to the core consume steps without starting any new in-flight work for that message.

### Requirement 6: Correlation and Envelope Fidelity Over RabbitMQ

**User Story:** As a developer, I want every envelope field preserved across RabbitMQ, so that a consumer reconstructs the same message and a single logical operation stays traceable end to end.

#### Acceptance Criteria

1. WHEN a Message_Envelope is published over RabbitMQ and subsequently delivered, THE RabbitMQ_Provider SHALL make the delivered Message_Envelope carry a message id, Correlation_Id, message type identifier, UTC creation timestamp, and contract version whose values are each equal to the corresponding values on the published Message_Envelope.
2. WHEN a Message_Envelope is published over RabbitMQ and subsequently delivered, THE RabbitMQ_Provider SHALL make the delivered header collection contain exactly the same number of entries as the published header collection, with each header key present and mapped to a value equal to its published value.
3. WHEN a Message_Envelope is published over RabbitMQ and subsequently delivered, THE RabbitMQ_Provider SHALL make the delivered payload byte sequence equal, in length and in every byte position, to the published payload byte sequence.
4. WHEN a Message_Envelope is delivered over RabbitMQ, THE Consume_Processor SHALL propagate the Correlation_Id carried on the Message_Envelope into the core consume steps as an unchanged, character-for-character identical value.
5. WHEN a Message_Envelope is delivered over RabbitMQ, THE Consume_Processor SHALL bind the payload to a known message contract before invoking the application handler.
6. IF a delivered Message_Envelope carries a message type identifier that matches no registered message contract, or a payload that cannot be bound to the contract registered for its message type identifier, THEN THE Consume_Processor SHALL reject the Message_Envelope, SHALL route it through the dead-letter path, SHALL NOT invoke the application handler with the payload, and SHALL surface an error indication identifying the unknown or unbindable contract.
7. IF the dead-letter path fails to accept a rejected Message_Envelope, THEN THE Consume_Processor SHALL retain the Message_Envelope in active processing, SHALL NOT acknowledge it to RabbitMQ, and SHALL surface an error indication that dead-lettering failed.

### Requirement 7: At-Least-Once Delivery with Idempotent Consumption

**User Story:** As a developer, I want RabbitMQ consumption to be idempotent under at-least-once delivery, so that a redelivered message is not processed twice.

#### Acceptance Criteria

1. THE RabbitMQ_Provider SHALL deliver each published Message_Envelope to the consume steps at least once, such that each Message_Envelope is delivered one or more times.
2. WHEN a Message_Envelope is delivered and the application handler completes without throwing, THE Consume_Processor SHALL acknowledge the message to the RabbitMQ broker and SHALL NOT redeliver that Message_Envelope.
3. WHERE idempotency is enabled in configuration, WHEN a Message_Envelope is delivered, THE Consume_Processor SHALL evaluate the Idempotency_Key through the core idempotency gate before invoking the application handler.
4. WHERE idempotency is enabled in configuration AND a delivered Message_Envelope carries an Idempotency_Key recorded as processed within the configured retention window (1 to 2,592,000 seconds, default 86,400 seconds), THE Consume_Processor SHALL NOT invoke the application handler for that Message_Envelope and SHALL acknowledge the message to the RabbitMQ broker.
5. WHERE idempotency is enabled in configuration AND the Idempotency_Key cannot be derived from a delivered Message_Envelope, THEN THE Consume_Processor SHALL reject the Message_Envelope with a missing-idempotency-key error before consulting the processed-key record and before invoking the application handler, SHALL NOT invoke the application handler, and SHALL NOT record the Idempotency_Key as processed.
6. WHERE idempotency is disabled in configuration, WHEN a Message_Envelope is delivered, THE Consume_Processor SHALL invoke the application handler without evaluating an Idempotency_Key and SHALL expose a null Idempotency_Key to the application handler.

### Requirement 8: Retry, Backoff, and Dead-Letter for Poison Messages

**User Story:** As a service operator, I want transient failures retried with backoff and persistent failures dead-lettered, so that RabbitMQ consumption recovers automatically and a poison message does not block the queue.

#### Acceptance Criteria

1. IF processing a delivered Message_Envelope fails with an error the Failure_Classifier classifies as transient, THEN THE Consume_Processor SHALL retry processing the Message_Envelope according to the configured Retry_Policy until processing succeeds or the configured maximum attempt count (Retry_Policy.MaxAttempts, inclusive bounds 1 to 100, default 5) is reached.
2. WHILE retrying a transiently failed Message_Envelope, THE Consume_Processor SHALL delay each attempt after the first by the interval produced by the configured backoff strategy from the configured base per-attempt delay (Retry_Policy.PerAttemptDelaySeconds, inclusive bounds 1 to 300 seconds, default 5 seconds).
3. IF processing a delivered Message_Envelope fails with an error the Failure_Classifier classifies as non-transient, THEN THE Consume_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink without any further retry attempt.
4. WHEN the configured Retry_Policy maximum attempt count is reached for a transiently failed Message_Envelope, THE Consume_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink within 5 seconds of the final failed attempt.
5. WHEN a Message_Envelope is routed to the Dead_Letter_Sink, THE Consume_Processor SHALL preserve on the dead-lettered message the Correlation_Id, the message id, the payload, and the total attempt count performed.
6. WHEN a Message_Envelope is routed to the Dead_Letter_Sink and acknowledged, THE Consume_Processor SHALL remove the Message_Envelope from the active processing pipeline so the application handler is not invoked again for that delivery.
7. IF routing a Message_Envelope to the Dead_Letter_Sink fails, THEN THE Consume_Processor SHALL NOT acknowledge the original delivery, SHALL retain the Message_Envelope for redelivery, and SHALL emit an error indication that dead-letter routing failed.
8. THE Consume_Processor SHALL apply the core Retry_Policy as the single source of retry and backoff behavior such that the total number of processing attempts for any delivered Message_Envelope does not exceed the configured maximum attempt count, rather than configuring an additional MassTransit-level redelivery policy that would multiply the attempt count.

### Requirement 9: Observability Participation Without Secret Exposure

**User Story:** As a service operator, I want to log and monitor the RabbitMQ integration, so that I can observe connection state and message activity without exposing secrets.

#### Acceptance Criteria

1. WHEN a Message_Envelope is published over RabbitMQ, THE RabbitMQ_Provider SHALL record a publish event through the Observability_Context carrying the event type classification, the Correlation_Id, and a UTC timestamp.
2. WHEN a Message_Envelope is consumed over RabbitMQ, THE RabbitMQ_Provider SHALL record a consume event through the Observability_Context carrying the event type classification, the Correlation_Id, and a UTC timestamp.
3. WHEN the Bus_Adapter transitions from stopped to started, THE RabbitMQ_Provider SHALL emit a structured log entry whose bus-state field records the started state.
4. WHEN the Bus_Adapter transitions from started to stopped, THE RabbitMQ_Provider SHALL emit a structured log entry whose bus-state field records the stopped state.
5. WHEN the RabbitMQ_Provider emits a log entry or trace that references the broker, THE RabbitMQ_Provider SHALL reference the broker only by its non-secret host display name, SHALL exclude the Connection_String value, and SHALL exclude the message payload bytes.
6. WHEN the RabbitMQ_Provider includes a sensitive field value in an observability record, THE RabbitMQ_Provider SHALL replace that value with a fixed redaction marker that contains no fragment of the original value.
7. WHEN a Message_Envelope is published over RabbitMQ, THE RabbitMQ_Provider SHALL propagate the Correlation_Id in the message headers so a consume trace links to the originating publish trace by an equal Correlation_Id value.
8. IF emitting an observability record fails, THEN THE RabbitMQ_Provider SHALL suppress the failure and SHALL NOT fail the publish or consume operation.

### Requirement 10: Transport Reachability and Readiness

**User Story:** As a service operator, I want readiness to reflect RabbitMQ reachability, so that traffic is not routed to an instance that cannot reach the broker.

#### Acceptance Criteria

1. WHEN the readiness probe queries the active provider, THE RabbitMQ_Provider SHALL return a Transport_Health result that reports whether the RabbitMQ broker is reachable within 1 second of the query.
2. IF the RabbitMQ broker is determined to be not reachable, THEN THE RabbitMQ_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the RabbitMQ broker as the unavailable dependency using its non-secret host display name, excluding any credentials, password, or connection secret.
3. IF the reachability check fails with an error, THEN THE RabbitMQ_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the RabbitMQ broker as the unavailable dependency, without propagating the error to the readiness probe.
4. IF the reachability check does not complete within 1 second, THEN THE RabbitMQ_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the RabbitMQ broker as the unavailable dependency.
5. WHEN the RabbitMQ broker is confirmed reachable, THE RabbitMQ_Provider SHALL return a Transport_Health result whose reachable indicator is true and whose unavailable dependency is unset.

### Requirement 11: Containerized Verification Against a Real Broker

**User Story:** As a developer, I want RabbitMQ behavior verified against a real broker in containers, so that transport-level bugs surface before production.

#### Acceptance Criteria

1. THE RabbitMQ_Provider SHALL be verified by integration tests that run against a real RabbitMQ broker provided by a container rather than against an in-memory transport substitute.
2. WHEN the integration test suite runs locally, THE RabbitMQ_Provider verification SHALL provision the RabbitMQ broker through an ephemeral container that becomes ready to accept connections within 60 seconds, and IF the broker is not ready within 60 seconds, THEN the verification SHALL fail with a timeout error.
3. WHEN an integration test run completes, whether it succeeds or fails, THE RabbitMQ_Provider verification SHALL tear down the ephemeral RabbitMQ container.
4. THE RabbitMQ_Provider integration verification SHALL cover a publish-then-consume round trip that, within 30 seconds, asserts the delivered Message_Envelope's message id, Correlation_Id, message type identifier, UTC creation timestamp, contract version, headers, and payload bytes are each equal to the published values.
5. THE RabbitMQ_Provider integration verification SHALL cover retry exhaustion that asserts a Poison_Message is routed to the Dead_Letter_Sink, is not redelivered to the application handler, and is not reported as in-flight.
6. WHERE the RabbitMQ broker container image is specified for verification, THE verification SHALL reference the image by an explicit fixed-release version tag and SHALL NOT reference a floating latest tag.

### Requirement 12: Production Readiness for Hosted Azure RabbitMQ

**User Story:** As a service operator, I want the provider ready by default for a hosted Azure RabbitMQ broker, so that production uses managed infrastructure with injected credentials.

#### Acceptance Criteria

1. WHERE the production configuration selects the Azure_RabbitMQ broker, THE RabbitMQ_Provider SHALL connect to the broker using the Connection_String supplied by the runtime environment or secret store.
2. THE RabbitMQ_Provider SHALL obtain the Connection_String from the runtime environment or secret store and SHALL NOT require the Connection_String to be embedded in source or in a committed configuration file.
3. IF the Connection_String for the Azure_RabbitMQ broker is absent when startup validation runs, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report the absent Connection_String by its configuration setting name.
4. IF the Connection_String for the Azure_RabbitMQ broker is present but empty, contains only whitespace, or cannot be parsed as a broker connection string when startup validation runs, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report the invalid Connection_String by its configuration setting name without emitting the Connection_String value.
5. WHEN the RabbitMQ_Provider connects to the Azure_RabbitMQ broker, THE RabbitMQ_Provider SHALL exclude the Connection_String value from every log entry and trace it emits.
6. IF a connection attempt to the Azure_RabbitMQ broker fails, THEN THE RabbitMQ_Provider SHALL report the failure by the Connection_String configuration setting name and SHALL exclude the Connection_String value from every log entry and trace it emits.
