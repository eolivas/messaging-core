# Requirements Document

## Introduction

The SNS/SQS Provider implements Amazon SNS and Amazon SQS as a concrete messaging transport for the Messaging Core library by completing the existing `Messaging.MassTransit` adapter shell for the `AmazonSqs` transport. Amazon SNS/SQS is reached **through MassTransit** (the `MassTransit.AmazonSQS` transport, which depends transitively on the AWS SDK for .NET clients for SNS and SQS), following the same approach as the RabbitMQ Provider. This feature does not build a separate adapter directly on the AWS SDK.

The defining use case is **fan-out from one producer to many consumers**. A publisher publishes a Message_Envelope once to an SNS topic named after the message type. Each consuming service owns its own SQS queue that is subscribed to that topic, so every consuming service receives its own copy of the message and processes it independently, with its own retry, dead-letter, and idempotency state. Competing consumers inside one service share that service's queue.

The provider supplies production implementations of the two seam ports the adapter shell was left open against: the bus adapter (`IMassTransitBusAdapter`: bus start/stop, publish/send, transport reachability) and the consume processor (`IMassTransitConsumeProcessor`: the single point where the core's correlation, idempotency, retry/classification, and dead-letter steps run ahead of the application handler). The core remains transport-neutral; the active provider is selected at startup by the configured `Messaging_Mode` through the provider registry. The SNS/SQS provider backs Publish-Subscribe (SNS fan-out to SQS queues) and Message Queue (point-to-point over SQS).

The feature also covers MassTransit-style conventions (message-type to topic naming, endpoint to queue naming, raw message delivery versus envelope delivery), topology management (topics, queues, subscriptions, queue access policies, dead-letter queue and redrive policy), FIFO versus standard topic and queue selection, fail-fast configuration validation, credential handling that never exposes secrets, observability, and verification with both unit tests and integration tests against a real AWS-compatible endpoint (LocalStack) running in containers. LocalStack is pinned to the 4.x release `4.14.0` (the Pinned_LocalStack_Image), which runs without a LocalStack auth token. Newer LocalStack images (unified image, since 2026-03-23) require a `LOCALSTACK_AUTH_TOKEN`, and the 4.x line receives no further updates, so the pin is deliberate. The tag `4.14.0` was checked against Docker Hub (published 2026-02-26) and a local container run confirmed that it starts without an auth token and supports a FIFO SNS topic subscribed to a FIFO SQS queue with raw message delivery.

Scope boundary (what this feature covers):
- Production `IMassTransitBusAdapter` over a configured MassTransit Amazon SQS/SNS bus (start/stop, publish to SNS for Publish-Subscribe, send to SQS for Message Queue, reachability probe).
- Production `IMassTransitConsumeProcessor` that drives the Application-layer consume pipeline for every message delivered from an SQS queue.
- Topology: SNS topics, SQS queues, topic-to-queue subscriptions, queue policies permitting SNS delivery, dead-letter queues, and redrive policies.
- Naming conventions, raw message delivery, envelope and header mapping, Correlation_Id propagation, FIFO and standard modes.
- Role-scoped DI registration (publisher only, consumer only, both).
- Settings validation, credentials and security, observability, readiness, and graceful shutdown.
- Unit tests and integration tests (LocalStack in containers), plus a docker-compose service for local development.

Scope boundary (what this feature does NOT cover):
- Changes to the transport-neutral core contracts or to the Streaming and Batch Processing modes.
- The RabbitMQ, Azure Service Bus, Kafka, and in-memory transports.
- SNS delivery protocols other than SQS (HTTP/S, email, SMS, Lambda, mobile push).
- SQS request/response (RPC) messaging, IoT scenarios, and message payloads larger than the SQS/SNS size limit (no S3 extended-payload offloading).
- The internal mechanics of the consume pipeline, outbox, retry executor, and observability facade, which are defined by the Messaging Core spec and consumed here unchanged.
- Provisioning of AWS accounts, IAM roles, KMS keys, and production infrastructure-as-code.
- Moving integration tests or local development to a newer LocalStack release that requires a `LOCALSTACK_AUTH_TOKEN`, or to another AWS emulator.

Open items for the design phase:
- Confirm SQS redrive behavior on `4.14.0` through the integration tests in Requirement 18 (not yet exercised by the manual check) and record any emulation gap as a known limitation.
- Choose the name of the configuration value that overrides the image tag (default proposed: environment variable `LOCALSTACK_IMAGE_TAG`).

## Glossary

- **SnsSqs_Provider**: The feature delivered here: the MassTransit adapter shell completed with live Amazon SNS/SQS seam implementations.
- **MassTransit_Provider**: The existing `MassTransitMessagingProvider` (an `IMessagingProvider`) that backs the Publish-Subscribe and Message Queue modes. The SnsSqs_Provider is the Amazon realization of its transport seams.
- **MassTransit_AmazonSqs_Transport**: The MassTransit transport (package `MassTransit.AmazonSQS`) that connects MassTransit to Amazon SNS and Amazon SQS.
- **Bus_Adapter**: The production implementation of `IMassTransitBusAdapter` over a configured MassTransit Amazon SQS/SNS bus.
- **Consume_Processor**: The production implementation of `IMassTransitConsumeProcessor` that runs the core consume steps for each delivered Message_Envelope ahead of the application handler.
- **Messaging_Mode**: A category of messaging behavior. For this feature the relevant values are Publish-Subscribe and Message Queue.
- **Publish_Subscribe_Mode**: The Messaging_Mode in which one message is fanned out to many subscribing consumers, realized as publish to an SNS_Topic with one Subscription per consumer Consumer_Queue.
- **Message_Queue_Mode**: The Messaging_Mode in which a message is delivered point-to-point to one of competing consumers, realized as send to an SQS queue.
- **Message_Envelope**: The transport-neutral representation of a message carrying payload plus metadata (message id, correlation id, message type, UTC creation timestamp, contract version, headers).
- **SNS_Topic**: An Amazon SNS topic that receives published messages and delivers a copy to every Subscription.
- **Consumer_Queue**: An Amazon SQS queue owned by exactly one consuming service endpoint. Competing consumer instances of that service read from the same Consumer_Queue.
- **Subscription**: The binding of one SNS_Topic to one Consumer_Queue so SNS delivers each published message to that queue.
- **Queue_Policy**: The SQS access policy on a Consumer_Queue that permits the subscribed SNS_Topic to send messages to it.
- **Dead_Letter_Queue**: An SQS queue that receives messages the Consumer_Queue could not deliver successfully, associated with the Consumer_Queue through a Redrive_Policy.
- **Redrive_Policy**: The SQS setting that moves a message to the Dead_Letter_Queue after it has been received a configured number of times without deletion.
- **Topology_Manager**: The component of the SnsSqs_Provider that creates or verifies SNS_Topics, Consumer_Queues, Subscriptions, Queue_Policies, Dead_Letter_Queues, and Redrive_Policies.
- **Naming_Convention**: The rule set that derives the SNS_Topic name from a message type and the Consumer_Queue name from an endpoint name, following MassTransit conventions.
- **Raw_Message_Delivery**: The SNS Subscription attribute that delivers the published message body to the Consumer_Queue without the SNS notification wrapper.
- **FIFO_Mode**: The use of FIFO SNS_Topics and FIFO queues (names ending in `.fifo`) that preserve ordering per message group and deduplicate within the deduplication window. The alternative is Standard_Mode.
- **Standard_Mode**: The use of standard SNS_Topics and standard queues, providing at-least-once delivery with best-effort ordering.
- **Message_Group_Id**: The identifier that scopes ordering in FIFO_Mode.
- **SnsSqs_Provider_Settings**: The provider-specific settings (extension of `MassTransitProviderSettings` for the `AmazonSqs` transport) binding region, optional service endpoint override, naming options, topology options, and FIFO option.
- **Credential_Source**: The origin of AWS credentials: the AWS default credential chain (environment, shared profile, container or instance role) or explicitly supplied access key and secret key values.
- **Service_Endpoint_Override**: An optional non-default URL for the SNS and SQS service endpoints, used for LocalStack.
- **Startup_Validator**: The existing `MessagingStartupValidator` that validates configuration and resolves the active provider fail-fast before any message is processed.
- **Observability_Context**: The existing core facade applying uniform structured logging, metrics, and tracing around every message operation, with best-effort emission and payload redaction.
- **Transport_Health**: The reachability result the provider reports to back the readiness probe, naming the unavailable dependency when the transport cannot be reached.
- **Dead_Letter_Sink**: The core destination for a Poison_Message that cannot be processed after the Retry_Policy is exhausted.
- **Retry_Policy**: The configured strategy (attempt count, backoff, delays) governing reprocessing of transiently failed messages, owned by the core and consumed here.
- **Poison_Message**: A message that repeatedly fails processing and is routed to the Dead_Letter_Sink.
- **Correlation_Id**: An identifier propagated across the producer, the Messaging Core, and the consumer to trace one logical operation.
- **Idempotency_Key**: A business or message key used to detect and discard duplicate deliveries under at-least-once delivery.
- **Visibility_Timeout**: The SQS period during which a received message is hidden from other consumers while it is being processed.
- **LocalStack**: A container-hosted emulator of AWS services (including SNS and SQS) used for local development and integration tests.
- **Pinned_LocalStack_Image**: The image `localstack/localstack:4.14.0`, a fixed 4.x release that runs without a LocalStack auth token. Its tag is the Pinned_LocalStack_Tag.
- **Pinned_LocalStack_Tag**: The exact tag `4.14.0`, used as the default for both `docker-compose.yml` and the integration test fixture.
- **LocalStack_Image_Tag_Setting**: The configuration value (an environment variable) that overrides the Pinned_LocalStack_Tag; its default is the Pinned_LocalStack_Tag.
- **Publisher_Only_Role**: An embedding application that publishes messages and does not consume them.
- **Consumer_Only_Role**: An embedding application that consumes messages and does not publish them.
- **Publisher_And_Consumer_Role**: An embedding application that both consumes and publishes.

## Requirements

### Requirement 1: Amazon SNS/SQS Realized Through MassTransit

**User Story:** As a developer, I want SNS/SQS provided through the existing MassTransit adapter, so that the MassTransit conventions and the established pattern in the codebase are preserved.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL realize the Amazon transport through the MassTransit_AmazonSqs_Transport and SHALL NOT introduce a separate adapter built directly on the AWS SDK clients for SNS and SQS.
2. THE SnsSqs_Provider SHALL provide a production implementation of the Bus_Adapter seam exposing bus start, bus stop, message publish/send, and transport reachability over a configured MassTransit Amazon SQS/SNS bus.
3. THE SnsSqs_Provider SHALL provide a production implementation of the Consume_Processor seam that, for each delivered Message_Envelope and ahead of the application handler, runs the core consume steps: Correlation_Id propagation, the idempotency gate, contract binding, handler invocation, and failure classification with dead-letter routing.
4. THE Bus_Adapter and the Consume_Processor SHALL reside in the `Messaging.MassTransit` Infrastructure package and SHALL depend on the core contracts only through the ports declared in the inward Domain and Application layers.
5. WHERE the active Messaging_Mode is Publish_Subscribe_Mode, THE Bus_Adapter SHALL publish a Message_Envelope to the SNS_Topic derived from its message type through the MassTransit publish endpoint.
6. WHERE the active Messaging_Mode is Message_Queue_Mode, THE Bus_Adapter SHALL send a Message_Envelope to the configured SQS queue through the MassTransit send endpoint.
7. WHEN the MassTransit Amazon SQS/SNS bus is started and the SNS and SQS service endpoints are reachable, THE Bus_Adapter SHALL report a reachable Transport_Health result with no named unavailable dependency.
8. IF the SNS or SQS service endpoint is unreachable or the reachability probe throws, THEN THE Bus_Adapter SHALL report a not-reachable Transport_Health result naming the Amazon SNS/SQS service as the unavailable dependency and SHALL NOT propagate the error to the caller.
9. WHEN a delivered Message_Envelope is processed and the application handler completes without throwing, THE Consume_Processor SHALL delete the message from the Consumer_Queue; IF processing throws, THEN THE Consume_Processor SHALL NOT delete the message as successfully processed.

### Requirement 2: Fan-Out From One Producer to Many Consumers

**User Story:** As a developer, I want one published message delivered to every consuming service, so that a producer does not need to know who consumes its messages.

#### Acceptance Criteria

1. WHEN a publisher publishes one Message_Envelope in Publish_Subscribe_Mode, THE SnsSqs_Provider SHALL publish it to exactly one SNS_Topic exactly once per publish call.
2. WHEN a published Message_Envelope reaches an SNS_Topic that has N Subscriptions, THE SnsSqs_Provider SHALL deliver one copy of the Message_Envelope to each of the N Consumer_Queues, where N is greater than or equal to 1.
3. THE SnsSqs_Provider SHALL assign each consuming service endpoint its own Consumer_Queue so that a message consumed from one Consumer_Queue does not remove or hide the message from any other Consumer_Queue.
4. WHERE multiple instances of one consuming service endpoint run, THE SnsSqs_Provider SHALL have those instances read from the same Consumer_Queue so that each copy delivered to that endpoint is processed by exactly one instance at a time.
5. WHEN a new consuming service endpoint starts and subscribes its Consumer_Queue to an existing SNS_Topic, THE SnsSqs_Provider SHALL deliver messages published after the Subscription is active to the new Consumer_Queue without requiring any change to the publisher.
6. WHEN one Consumer_Queue fails to process a message and routes it to its Dead_Letter_Queue, THE SnsSqs_Provider SHALL leave the copies delivered to all other Consumer_Queues unaffected.
7. WHERE the Messaging_Mode is Publish_Subscribe_Mode and a publisher publishes a Message_Envelope to an SNS_Topic with zero Subscriptions, THE Bus_Adapter SHALL complete the publish without error.

### Requirement 3: Provider Selection and Resolution

**User Story:** As a service operator, I want SNS/SQS selected through configuration, so that I can deploy the same artifact against Amazon SNS/SQS without code changes.

#### Acceptance Criteria

1. WHERE the MassTransit provider settings specify the `AmazonSqs` transport for Publish_Subscribe_Mode or Message_Queue_Mode, THE SnsSqs_Provider SHALL register exactly one MassTransit_Provider for the configured Messaging_Mode so the Provider_Registry resolves it at startup.
2. WHEN the Messaging Core starts with the `AmazonSqs` transport selected for Publish_Subscribe_Mode or Message_Queue_Mode, THE Provider_Registry SHALL resolve exactly one MassTransit_Provider for that Messaging_Mode; IF zero or more than one provider is registered for that Messaging_Mode, THEN THE Provider_Registry SHALL fail startup and report the unresolved or ambiguous mapping.
3. IF the MassTransit provider settings specify the `AmazonSqs` transport for a Messaging_Mode other than Publish_Subscribe_Mode or Message_Queue_Mode, THEN THE SnsSqs_Provider SHALL fail startup before the Messaging Core enters a processing-ready state, SHALL NOT register a partial provider, and SHALL report an error indicating that SNS/SQS backs only the Publish-Subscribe and Message Queue modes.
4. WHILE the Messaging Core instance is running, THE Provider_Registry SHALL return the same resolved MassTransit_Provider instance on every resolution of the active Messaging_Mode, and the mode-to-provider mapping SHALL NOT change.

### Requirement 4: Naming Conventions

**User Story:** As a developer, I want topic and queue names derived by MassTransit-style conventions, so that publishers and consumers agree on names without sharing configuration.

#### Acceptance Criteria

1. WHEN the SnsSqs_Provider derives an SNS_Topic name from a message type, THE Naming_Convention SHALL produce a name from the message type's namespace-qualified name using the MassTransit entity-name formatter, so that the same message type yields the same SNS_Topic name in every publisher and consumer process.
2. WHEN the SnsSqs_Provider derives a Consumer_Queue name from an endpoint name, THE Naming_Convention SHALL produce a name from the endpoint name using the MassTransit endpoint-name formatter (kebab-case by default).
3. WHERE a configured topic name prefix or queue name prefix is present, THE Naming_Convention SHALL prepend that prefix to every derived SNS_Topic name or Consumer_Queue name respectively.
4. THE Naming_Convention SHALL replace each character that SNS or SQS does not permit in a name (permitted: alphanumeric characters, hyphens, and underscores, plus the `.fifo` suffix in FIFO_Mode) with an underscore and SHALL keep each derived name within 256 characters for SNS_Topics and 80 characters for SQS queues.
5. IF a derived name exceeds the applicable length limit after formatting, THEN THE Naming_Convention SHALL fail with an error identifying the message type or endpoint name and the limit, rather than truncating the name.
6. WHERE FIFO_Mode is enabled, THE Naming_Convention SHALL append the `.fifo` suffix to every derived SNS_Topic name, Consumer_Queue name, and Dead_Letter_Queue name that does not already end in `.fifo`.
7. FOR ALL message types and endpoint names, deriving a name twice from the same input and the same options SHALL produce identical names (idempotence).
8. THE Naming_Convention SHALL derive the Dead_Letter_Queue name from the Consumer_Queue name by appending the configured dead-letter suffix (default `_error`), keeping the `.fifo` suffix last in FIFO_Mode.

### Requirement 5: Topology Management

**User Story:** As a service operator, I want topics, queues, subscriptions, and policies created consistently, so that fan-out works without manual AWS console steps in development and with controlled behavior in production.

#### Acceptance Criteria

1. WHERE topology auto-creation is enabled, WHEN the consumer host starts, THE Topology_Manager SHALL create each missing SNS_Topic, Consumer_Queue, Dead_Letter_Queue, and Subscription required by the registered consumers, and SHALL leave existing resources unchanged.
2. WHEN the Topology_Manager creates or verifies a Subscription, THE Topology_Manager SHALL bind exactly one SNS_Topic to exactly one Consumer_Queue and SHALL NOT create a second Subscription for the same topic and queue pair.
3. WHEN the Topology_Manager creates a Subscription, THE Topology_Manager SHALL set a Queue_Policy on the Consumer_Queue that permits `sqs:SendMessage` only from the subscribed SNS_Topic Amazon Resource Name.
4. WHEN the Topology_Manager creates a Consumer_Queue, THE Topology_Manager SHALL create its Dead_Letter_Queue and SHALL attach a Redrive_Policy whose maximum receive count is the configured value (inclusive bounds 1 to 1,000, default 5).
5. WHEN the Topology_Manager creates a Consumer_Queue, THE Topology_Manager SHALL set the Visibility_Timeout to the configured value (inclusive bounds 1 to 43,200 seconds, default 30 seconds) and the message retention period to the configured value (inclusive bounds 60 to 1,209,600 seconds, default 345,600 seconds).
6. WHERE topology auto-creation is disabled, THE Topology_Manager SHALL verify that each required SNS_Topic, Consumer_Queue, and Subscription exists and SHALL NOT create or modify any resource.
7. IF topology auto-creation is disabled and a required resource does not exist, THEN THE Topology_Manager SHALL fail consumer host startup with an error naming each missing resource, and the consumer host SHALL remain in the Stopped state.
8. IF a topology operation fails because the caller lacks permission, THEN THE Topology_Manager SHALL fail startup with an error naming the failed operation and the resource name, excluding credential values.
9. WHEN the Topology_Manager runs twice against the same configuration, THE Topology_Manager SHALL leave the resulting topology identical to the topology after the first run (idempotence).
10. THE Topology_Manager SHALL apply the SnsSqs_Provider_Settings Redrive_Policy and Visibility_Timeout values without relying on the Retry_Policy attempt count (see Requirement 10).

### Requirement 6: Message Format, Raw Message Delivery, and Header Mapping

**User Story:** As a developer, I want envelope fields preserved across SNS and SQS, so that a consumer reconstructs the same message regardless of the delivery format.

#### Acceptance Criteria

1. WHERE Raw_Message_Delivery is enabled (default), THE Topology_Manager SHALL set the Raw_Message_Delivery attribute on each Subscription, and THE Consume_Processor SHALL read the Message_Envelope directly from the SQS message body and message attributes.
2. WHERE Raw_Message_Delivery is disabled, THE Consume_Processor SHALL unwrap the SNS notification wrapper and SHALL read the Message_Envelope from the wrapped message body and wrapped message attributes.
3. WHEN a Message_Envelope is published and subsequently delivered, THE SnsSqs_Provider SHALL make the delivered Message_Envelope carry a message id, Correlation_Id, message type identifier, UTC creation timestamp, and contract version whose values are each equal to the corresponding values on the published Message_Envelope.
4. WHEN a Message_Envelope is published and subsequently delivered, THE SnsSqs_Provider SHALL make the delivered header collection contain exactly the same entries as the published header collection, with each header key present and mapped to a value equal to its published value.
5. WHEN a Message_Envelope is published and subsequently delivered, THE SnsSqs_Provider SHALL make the delivered payload byte sequence equal, in length and in every byte position, to the published payload byte sequence, including payloads that contain non-ASCII characters.
6. THE SnsSqs_Provider SHALL map each Message_Envelope header to an SNS message attribute and SHALL keep the number of message attributes per message at or below 10, the SNS and SQS limit.
7. IF a Message_Envelope would require more than 10 message attributes, THEN THE SnsSqs_Provider SHALL carry the additional headers inside the serialized message body and SHALL restore them to the header collection on delivery.
8. IF the serialized Message_Envelope exceeds the configured maximum message size (inclusive bounds 1 byte to 262,144 bytes, default 262,144 bytes), THEN THE Bus_Adapter SHALL reject the publish with an error stating the actual size and the limit, and SHALL NOT send any part of the message.
9. FOR ALL valid Message_Envelope values, serializing then deserializing through the SnsSqs_Provider message format SHALL produce an equivalent Message_Envelope (round-trip property).
10. WHEN a Message_Envelope is delivered, THE Consume_Processor SHALL bind the payload to a known message contract before invoking the application handler.
11. IF a delivered Message_Envelope carries a message type identifier that matches no registered message contract, or a payload that cannot be bound to the registered contract, THEN THE Consume_Processor SHALL reject the Message_Envelope, SHALL route it through the dead-letter path, SHALL NOT invoke the application handler with the payload, and SHALL surface an error indication identifying the unknown or unbindable contract.
12. IF the dead-letter path fails to accept a rejected Message_Envelope, THEN THE Consume_Processor SHALL NOT delete the message from the Consumer_Queue and SHALL surface an error indication that dead-lettering failed.

### Requirement 7: Correlation Propagation

**User Story:** As a service operator, I want one Correlation_Id to follow a message across SNS, SQS, and every consumer, so that a logical operation is traceable end to end.

#### Acceptance Criteria

1. WHEN a Message_Envelope is published, THE Bus_Adapter SHALL carry the Correlation_Id in the SNS message attributes and SHALL map it to the MassTransit correlation identifier of the outgoing message.
2. WHEN a Message_Envelope is delivered from a Consumer_Queue, THE Consume_Processor SHALL propagate the Correlation_Id into the core consume steps as an unchanged, character-for-character identical value.
3. WHEN one published Message_Envelope is delivered to N Consumer_Queues, THE SnsSqs_Provider SHALL deliver the same Correlation_Id to each of the N consumers.
4. IF a delivered Message_Envelope carries no Correlation_Id, THEN THE Consume_Processor SHALL reject it through the core Correlation_Id validation and SHALL route it through the dead-letter path without invoking the application handler.

### Requirement 8: Standard and FIFO Delivery Modes

**User Story:** As a developer, I want to choose standard or FIFO delivery per deployment, so that I can trade throughput for ordering and deduplication where the business requires it.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL operate in Standard_Mode unless FIFO_Mode is enabled in the SnsSqs_Provider_Settings.
2. WHERE FIFO_Mode is enabled, THE Bus_Adapter SHALL publish each Message_Envelope with a Message_Group_Id and a deduplication identifier.
3. WHERE FIFO_Mode is enabled, THE Bus_Adapter SHALL derive the Message_Group_Id from the configured group-selection rule (default: the Message_Envelope Correlation_Id) and SHALL derive the deduplication identifier from the Message_Envelope message id.
4. WHERE FIFO_Mode is enabled, WHEN two Message_Envelopes with the same Message_Group_Id are published in sequence, THE SnsSqs_Provider SHALL deliver them to each Consumer_Queue in publish order.
5. WHERE FIFO_Mode is enabled, WHEN two publishes carry the same deduplication identifier within the 5-minute SNS/SQS deduplication window, THE SnsSqs_Provider SHALL deliver only one copy to each Consumer_Queue.
6. WHERE FIFO_Mode is enabled, THE Consume_Processor SHALL process messages of one Message_Group_Id one at a time in the order received from the Consumer_Queue.
7. IF FIFO_Mode is enabled and a Consumer_Queue or SNS_Topic with a conflicting type (standard versus FIFO) already exists under the derived name, THEN THE Topology_Manager SHALL fail startup with an error naming the resource and the conflicting type.
8. WHERE Standard_Mode is active, THE Consume_Processor SHALL treat delivery as at-least-once with possible reordering and SHALL NOT assume ordering between messages.

### Requirement 9: At-Least-Once Delivery with Idempotent Consumption

**User Story:** As a developer, I want SQS consumption to be idempotent under at-least-once delivery, so that a redelivered or duplicated message is not processed twice.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL deliver each published Message_Envelope to each Consumer_Queue's consume steps at least once.
2. WHEN a Message_Envelope is delivered and the application handler completes without throwing, THE Consume_Processor SHALL delete the message from the Consumer_Queue so that SQS does not redeliver it.
3. WHERE idempotency is enabled in configuration, WHEN a Message_Envelope is delivered, THE Consume_Processor SHALL evaluate the Idempotency_Key through the core idempotency gate before invoking the application handler.
4. WHERE idempotency is enabled in configuration AND a delivered Message_Envelope carries an Idempotency_Key recorded as processed within the configured retention window (1 to 2,592,000 seconds, default 86,400 seconds), THE Consume_Processor SHALL NOT invoke the application handler and SHALL delete the message from the Consumer_Queue.
5. WHERE idempotency is enabled in configuration AND the Idempotency_Key cannot be derived from a delivered Message_Envelope, THEN THE Consume_Processor SHALL reject the Message_Envelope with a missing-idempotency-key error before consulting the processed-key record and before invoking the application handler, and SHALL NOT record the Idempotency_Key as processed.
6. WHERE idempotency is disabled in configuration, WHEN a Message_Envelope is delivered, THE Consume_Processor SHALL invoke the application handler without evaluating an Idempotency_Key and SHALL expose a null Idempotency_Key to the application handler.
7. THE SnsSqs_Provider SHALL keep the idempotency state of each Consumer_Queue independent so that processing a message in one consuming service does not mark it as processed in another consuming service.
8. WHEN the same message id is delivered to the same Consumer_Queue twice (for example because of a Visibility_Timeout expiry), THE Consume_Processor SHALL invoke the application handler no more than once for that Idempotency_Key while idempotency is enabled and the key is within the retention window.

### Requirement 10: Retry, Backoff, Dead-Letter, and Redrive

**User Story:** As a service operator, I want transient failures retried with backoff and persistent failures dead-lettered, so that a poison message does not block a Consumer_Queue.

#### Acceptance Criteria

1. IF processing a delivered Message_Envelope fails with an error the Failure_Classifier classifies as transient, THEN THE Consume_Processor SHALL retry processing according to the configured Retry_Policy until processing succeeds or the maximum attempt count (inclusive bounds 1 to 100, default 5) is reached.
2. WHILE retrying a transiently failed Message_Envelope, THE Consume_Processor SHALL delay each attempt after the first by the interval produced by the configured backoff strategy from the base per-attempt delay (inclusive bounds 1 to 300 seconds, default 5 seconds).
3. WHILE the Consume_Processor retries a Message_Envelope, THE SnsSqs_Provider SHALL keep the message invisible to other consumers by extending the Visibility_Timeout so that the message is not redelivered before the final attempt completes.
4. IF processing a delivered Message_Envelope fails with an error the Failure_Classifier classifies as non-transient, THEN THE Consume_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink without any further retry attempt.
5. WHEN the maximum attempt count is reached for a transiently failed Message_Envelope, THE Consume_Processor SHALL route the Message_Envelope to the Dead_Letter_Sink within 5 seconds of the final failed attempt.
6. WHEN a Message_Envelope is routed to the Dead_Letter_Sink, THE Consume_Processor SHALL preserve on the dead-lettered message the Correlation_Id, the message id, the payload, and the total attempt count performed.
7. WHEN a Message_Envelope is routed to the Dead_Letter_Sink and the dead-letter send is confirmed, THE Consume_Processor SHALL delete the original message from the Consumer_Queue so the application handler is not invoked again for that delivery.
8. IF routing a Message_Envelope to the Dead_Letter_Sink fails, THEN THE Consume_Processor SHALL NOT delete the original message, SHALL leave it for redelivery after the Visibility_Timeout, and SHALL emit an error indication that dead-letter routing failed.
9. THE Consume_Processor SHALL apply the core Retry_Policy as the single source of retry behavior so that the total number of processing attempts for a delivered Message_Envelope does not exceed the configured maximum attempt count, and THE SnsSqs_Provider SHALL NOT configure an additional MassTransit-level redelivery or retry policy that would multiply the attempt count.
10. WHEN the SQS Redrive_Policy moves a message to the Dead_Letter_Queue because the maximum receive count was exceeded, THE Observability_Context SHALL record a dead-letter event carrying the Correlation_Id when that message is later observed, and THE SnsSqs_Provider SHALL leave the message in the Dead_Letter_Queue available for inspection and manual redrive.
11. WHEN the configured Redrive_Policy maximum receive count is less than the Retry_Policy maximum attempt count, THE Startup_Validator SHALL report a configuration error identifying both settings by name, because SQS would dead-letter the message before the core Retry_Policy completes.

### Requirement 11: Role-Scoped Composition for Publisher, Consumer, or Both

**User Story:** As a developer, I want the provider to create only the topology and machinery my service's role needs, so that a publisher-only service creates no queues and a consumer-only service starts no unused publish path.

#### Acceptance Criteria

1. WHERE an embedding application is composed in the Publisher_Only_Role, THE SnsSqs_Provider SHALL enable the Bus_Adapter publish/send path and SHALL NOT register a receive endpoint or create a Consumer_Queue.
2. WHERE an embedding application is composed in the Publisher_Only_Role, THE SnsSqs_Provider SHALL NOT invoke the core consume steps for any message.
3. WHERE an embedding application is composed in the Consumer_Only_Role, THE SnsSqs_Provider SHALL register exactly one receive endpoint per configured endpoint name and SHALL route each delivered Message_Envelope through the core consume steps.
4. WHERE an embedding application is composed in the Consumer_Only_Role, THE SnsSqs_Provider SHALL NOT enable an application-initiated publish/send path, while still permitting the core dead-letter send path required to route a Poison_Message.
5. WHERE an embedding application is composed in the Publisher_And_Consumer_Role, THE SnsSqs_Provider SHALL enable the publish/send path and SHALL register the receive endpoint.
6. WHERE a service consumes N message types, THE Topology_Manager SHALL create one Consumer_Queue for the endpoint and SHALL subscribe that single Consumer_Queue to each of the N SNS_Topics.

### Requirement 12: Bus Lifecycle and Graceful Drain

**User Story:** As a service operator, I want the bus to start and stop cleanly, so that deployments and restarts do not drop in-flight messages.

#### Acceptance Criteria

1. WHEN the consumer host is signaled to start, THE Bus_Adapter SHALL run topology creation or verification, then start the MassTransit bus, and upon a successful start SHALL transition the consumer host to the Running state.
2. WHEN the consumer host is signaled to stop, THE Bus_Adapter SHALL transition the consumer host out of the Running state and SHALL stop polling the Consumer_Queue for new messages before draining.
3. WHEN the consumer host has stopped polling and all in-flight consumers complete within the configured graceful-shutdown timeout (1 to 300 seconds, default 30 seconds), THE Bus_Adapter SHALL stop the bus and SHALL leave the consumer host in the Stopped state.
4. IF the graceful-shutdown timeout elapses before all in-flight consumers complete, THEN THE Bus_Adapter SHALL stop the bus, SHALL terminate the consumer host, SHALL report the exact non-negative count of in-flight messages that had not completed, and SHALL leave those messages in the Consumer_Queue so SQS redelivers them after the Visibility_Timeout.
5. IF the MassTransit bus fails to start, THEN THE Bus_Adapter SHALL surface an error indicating the startup failure to the caller and SHALL leave the consumer host in the Stopped state.
6. WHILE the consumer host is not in the Running state, THE SnsSqs_Provider SHALL reject delivery of a new message to the core consume steps without starting any new in-flight work for that message.
7. THE SnsSqs_Provider SHALL configure the SQS long-polling wait time (inclusive bounds 0 to 20 seconds, default 20 seconds), the maximum messages per receive (inclusive bounds 1 to 10, default 10), and the consumer concurrency limit (inclusive bounds 1 to 1,000, default 10) from the SnsSqs_Provider_Settings.

### Requirement 13: SNS/SQS Startup Configuration Validation

**User Story:** As a service operator, I want Amazon settings validated at startup, so that a missing or invalid configuration fails fast instead of surfacing during message processing.

#### Acceptance Criteria

1. WHEN the Messaging Core starts with the `AmazonSqs` transport active, THE Startup_Validator SHALL validate all SNS/SQS-specific required settings in a single pass and SHALL complete that validation before the Messaging Core publishes or consumes any message.
2. IF the AWS region setting is absent or is not a valid AWS region identifier, THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name.
3. IF a setting holds a value outside its permitted set or inclusive permitted range (including the bounds stated in Requirements 5, 6, 10, and 12), THEN THE Startup_Validator SHALL prevent the Messaging Core from entering a processing-ready state and SHALL report a configuration error identifying the setting by its configuration setting name together with its permitted set or range.
4. IF the Service_Endpoint_Override is present and is not an absolute `http` or `https` URI, THEN THE Startup_Validator SHALL report a configuration error identifying the setting by its configuration setting name.
5. IF explicit access key credentials are configured and exactly one of the access key identifier or the secret access key is present, THEN THE Startup_Validator SHALL report a configuration error identifying both settings by name.
6. IF FIFO_Mode is enabled and a configured topic, queue, or Dead_Letter_Queue name prefix or suffix would produce a derived name that cannot end in `.fifo` within the length limit, THEN THE Startup_Validator SHALL report a configuration error identifying the affected setting.
7. WHEN two or more SNS/SQS settings are missing or invalid in the same startup, THE Startup_Validator SHALL report a separate configuration error for each missing or invalid setting rather than stopping at the first failure.
8. WHEN the Startup_Validator reports any SNS/SQS configuration failure, THE Startup_Validator SHALL identify each affected secret-bearing setting by its configuration setting name and SHALL exclude that setting's configured value from every reported configuration error.
9. WHEN SNS/SQS startup validation completes with zero missing required settings and zero invalid setting values, THE Startup_Validator SHALL permit the Messaging Core to enter a processing-ready state.
10. THE SnsSqs_Provider SHALL declare its provider-specific settings as `ProviderSettingDescriptor` entries so the existing validator checks their presence and permitted values.

### Requirement 14: Credentials and Security

**User Story:** As a service operator, I want AWS credentials and message data protected, so that no secret reaches logs, source control, or an over-permissioned role.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL obtain credentials from the AWS default credential chain (environment variables, shared profile, container role, or instance role) unless explicit access key credentials are configured.
2. WHERE explicit access key credentials are configured, THE SnsSqs_Provider SHALL obtain the access key identifier and secret access key from the runtime environment or a secret store and SHALL NOT require them to be embedded in source or in a committed configuration file.
3. THE SnsSqs_Provider SHALL exclude the access key identifier, secret access key, session token, and message payload bytes from every log entry, trace, metric label, and error message it emits.
4. WHEN the SnsSqs_Provider references the Amazon service in a log entry, trace, or health result, THE SnsSqs_Provider SHALL reference it only by a non-secret display name (the region identifier or the Service_Endpoint_Override host).
5. WHEN the SnsSqs_Provider includes a sensitive field value in an observability record, THE SnsSqs_Provider SHALL replace that value with a fixed redaction marker that contains no fragment of the original value.
6. THE SnsSqs_Provider SHALL communicate with Amazon SNS and Amazon SQS over HTTPS unless a Service_Endpoint_Override with the `http` scheme is configured.
7. WHERE a Service_Endpoint_Override with the `http` scheme is configured, THE Startup_Validator SHALL accept it only when the host is a loopback address or a container-network host name used for local development and test, and SHALL report a configuration error otherwise.
8. WHERE server-side encryption with a customer-managed key is configured, THE Topology_Manager SHALL set the key identifier on each SNS_Topic and Consumer_Queue it creates.
9. THE Queue_Policy created by the Topology_Manager SHALL grant access to the subscribed SNS_Topic Amazon Resource Name only and SHALL NOT grant access to any wildcard principal.
10. IF a connection or authorization attempt to Amazon SNS or Amazon SQS fails, THEN THE SnsSqs_Provider SHALL report the failure by the configuration setting name of the credential source and SHALL exclude the credential values.

### Requirement 15: Observability Participation Without Secret Exposure

**User Story:** As a service operator, I want to log and monitor the SNS/SQS integration, so that I can observe publish, consume, and fan-out activity without exposing secrets.

#### Acceptance Criteria

1. WHEN a Message_Envelope is published, THE SnsSqs_Provider SHALL record a publish event through the Observability_Context carrying the event type classification, the Correlation_Id, the SNS_Topic name, and a UTC timestamp.
2. WHEN a Message_Envelope is consumed, THE SnsSqs_Provider SHALL record a consume event through the Observability_Context carrying the event type classification, the Correlation_Id, the Consumer_Queue name, and a UTC timestamp.
3. WHEN the Bus_Adapter transitions from stopped to started or from started to stopped, THE SnsSqs_Provider SHALL emit a structured log entry whose bus-state field records the new state.
4. WHEN the Topology_Manager creates, verifies, or fails to create a resource, THE SnsSqs_Provider SHALL emit a structured log entry carrying the resource kind, the resource name, and the outcome.
5. WHEN a Message_Envelope is published, THE SnsSqs_Provider SHALL propagate the trace context in the message attributes so a consume trace links to the originating publish trace, and SHALL link each of the N consume traces of one fan-out publish to that publish trace.
6. THE SnsSqs_Provider SHALL emit metrics for messages published, messages consumed, messages dead-lettered, and processing failures, labeled by SNS_Topic name or Consumer_Queue name and excluding the Correlation_Id and message payload.
7. IF emitting an observability record fails, THEN THE SnsSqs_Provider SHALL suppress the failure and SHALL NOT fail the publish or consume operation.

### Requirement 16: Transport Reachability and Readiness

**User Story:** As a service operator, I want readiness to reflect Amazon SNS/SQS reachability, so that traffic is not routed to an instance that cannot reach its transport.

#### Acceptance Criteria

1. WHEN the readiness probe queries the active provider, THE SnsSqs_Provider SHALL return a Transport_Health result that reports whether the SNS and SQS service endpoints are reachable within 1 second of the query.
2. IF the SNS or SQS service endpoint is determined to be not reachable, THEN THE SnsSqs_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the Amazon SNS/SQS service as the unavailable dependency using its non-secret display name.
3. IF the reachability check fails with an error (including an authorization error), THEN THE SnsSqs_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the Amazon SNS/SQS service as the unavailable dependency, without propagating the error to the readiness probe.
4. IF the reachability check does not complete within 1 second, THEN THE SnsSqs_Provider SHALL return a Transport_Health result whose reachable indicator is false and that names the Amazon SNS/SQS service as the unavailable dependency.
5. WHEN the SNS and SQS service endpoints are confirmed reachable, THE SnsSqs_Provider SHALL return a Transport_Health result whose reachable indicator is true and whose unavailable dependency is unset.
6. WHERE the active role includes consuming, WHEN the Consumer_Queue approximate message count and approximate age of oldest message can be read, THE SnsSqs_Provider SHALL expose them as metrics without affecting the readiness result.

### Requirement 17: Unit Test Coverage

**User Story:** As a developer, I want the provider's logic covered by fast unit tests, so that naming, mapping, settings, and failure handling are verified without any AWS endpoint.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL have unit tests that run without network access, a container, or AWS credentials, using test doubles for the MassTransit bus and the AWS clients.
2. THE unit tests SHALL cover the Naming_Convention, including prefix handling, invalid character replacement, length-limit failure, `.fifo` suffix handling, and the idempotence property in Requirement 4.7.
3. THE unit tests SHALL cover the message format round-trip property in Requirement 6.9 using generated Message_Envelope values that include empty and non-ASCII payloads, empty and maximum-size header collections, and the more-than-10-attribute overflow case.
4. THE unit tests SHALL cover SnsSqs_Provider_Settings validation: each required setting missing, each setting at and outside both of its inclusive bounds, the Redrive_Policy versus Retry_Policy conflict, endpoint override rules, partial credential configuration, and secret exclusion from every error message.
5. THE unit tests SHALL cover the Consume_Processor: success then delete, duplicate Idempotency_Key then delete without handler invocation, missing Idempotency_Key, unknown contract, transient failure retried up to the maximum attempt count, non-transient failure dead-lettered without retry, dead-letter failure retaining the message, and shutdown rejection.
6. THE unit tests SHALL cover the Topology_Manager decisions against a fake AWS client: create-when-missing, no-op-when-present, verify-only failure naming missing resources, Queue_Policy principal scoping, and the idempotence property in Requirement 5.9.
7. THE unit tests SHALL cover Bus_Adapter role gating (Publisher_Only_Role, Consumer_Only_Role, Publisher_And_Consumer_Role), graceful shutdown with and without timeout, bus start failure, and readiness results for reachable, unreachable, error, and timeout cases.
8. THE unit tests SHALL cover the FIFO_Mode rules for Message_Group_Id and deduplication identifier derivation.
9. THE unit tests SHALL follow the xUnit and property-based testing conventions of the existing `Messaging.MassTransit.Tests` project, and each property-based test SHALL run a minimum of 100 generated cases.

### Requirement 18: Integration Test Coverage Against LocalStack

**User Story:** As a developer, I want SNS/SQS behavior verified against a real AWS-compatible endpoint in containers, so that transport-level bugs surface before production.

#### Acceptance Criteria

1. THE SnsSqs_Provider SHALL be verified by integration tests that run against LocalStack provided by a container, and SHALL NOT use an in-memory transport substitute for those tests.
2. WHEN the integration test suite runs locally, THE verification SHALL provision LocalStack through an ephemeral Testcontainers container that becomes ready to accept SNS and SQS requests within 60 seconds, and IF LocalStack is not ready within 60 seconds, THEN the verification SHALL fail with a timeout error.
3. WHEN an integration test run completes, whether it succeeds or fails, THE verification SHALL tear down the ephemeral LocalStack container it started and SHALL NOT remove any container that existed before the run.
4. THE verification SHALL reference the LocalStack container image by the Pinned_LocalStack_Tag as its default, SHALL read the tag from the LocalStack_Image_Tag_Setting when that setting is present, and SHALL NOT reference a floating tag, including `latest` and a bare major tag such as `4`.
5. THE integration tests SHALL cover a fan-out round trip in which one publish reaches three Consumer_Queues belonging to three distinct endpoints and, within 30 seconds, asserts each consumer receives one copy whose message id, Correlation_Id, message type identifier, UTC creation timestamp, contract version, headers, and payload bytes equal the published values.
6. THE integration tests SHALL cover competing consumers by running two consumer instances on one Consumer_Queue and asserting that each of 20 published messages is handled by exactly one instance.
7. THE integration tests SHALL cover late subscription by starting a new endpoint after a first publish and asserting that it receives only messages published after its Subscription became active.
8. THE integration tests SHALL cover retry exhaustion and asserting that a Poison_Message is routed to the Dead_Letter_Sink, is not redelivered to the application handler, and is not reported as in-flight, and that the other Consumer_Queues still received and processed their copies.
9. THE integration tests SHALL cover the SQS Redrive_Policy by asserting that a message received more than the maximum receive count without deletion appears in the Dead_Letter_Queue.
10. THE integration tests SHALL cover topology creation (topics, queues, Subscriptions, Queue_Policy, Dead_Letter_Queue, Redrive_Policy), a second startup that leaves the topology unchanged, and verify-only mode failing on a missing resource.
11. THE integration tests SHALL cover both Raw_Message_Delivery enabled and disabled.
12. THE integration tests SHALL cover FIFO_Mode ordering within one Message_Group_Id and deduplication of a repeated deduplication identifier, WHERE the pinned LocalStack version supports FIFO SNS_Topics.
13. THE integration tests SHALL cover idempotent consumption by delivering the same message id twice to one Consumer_Queue and asserting that the application handler runs once.
14. THE integration tests SHALL cover graceful shutdown with in-flight messages, asserting that completed messages are deleted and unfinished messages are redelivered after restart.
15. THE integration tests SHALL cover readiness: reachable while LocalStack runs and not reachable, naming the Amazon SNS/SQS service, after LocalStack stops.
16. THE integration tests SHALL give each test class an isolated set of topic and queue names, and SHALL leave no dependency on resources created by another test.
17. THE integration tests SHALL use dummy AWS credentials supplied at runtime, SHALL require no LocalStack auth token, and SHALL NOT contain real AWS credentials.

### Requirement 19: Local Development Environment

**User Story:** As a developer, I want one command to start a local SNS/SQS environment, so that I can run and debug the service on the host against realistic infrastructure.

#### Acceptance Criteria

1. THE repository `docker-compose.yml` SHALL define a LocalStack service that exposes the SNS and SQS APIs on the host and references its image by the Pinned_LocalStack_Tag as the default, overridable through the LocalStack_Image_Tag_Setting.
2. THE LocalStack compose service SHALL define a health check so that `docker compose up -d --wait` returns after SNS and SQS are available.
3. THE LocalStack compose service SHALL require no LocalStack auth token and no real credentials, and the documented local configuration SHALL use dummy credentials and a Service_Endpoint_Override pointing to the host-exposed endpoint.
4. THE LocalStack compose service and the integration test fixture SHALL use the same default Pinned_LocalStack_Tag and the same LocalStack_Image_Tag_Setting.
5. THE repository README SHALL document how to start the local environment, which settings select the `AmazonSqs` transport, how to configure the Service_Endpoint_Override, the Pinned_LocalStack_Tag, the LocalStack_Image_Tag_Setting, that newer LocalStack images require a `LOCALSTACK_AUTH_TOKEN`, and that the tag is pinned because 4.x runs without a token.
6. THE existing Kafka compose service SHALL continue to start unchanged.
