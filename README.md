# Messaging Core

A transport-neutral messaging library for .NET services. It defines a uniform publish/consume
surface — reliable publishing, idempotent consumption, retries with backoff, dead-lettering,
graceful shutdown, and observability — and keeps the concrete transport (RabbitMQ, Kafka, Amazon SNS/SQS, batch)
behind swappable provider adapters.

> **Status: core complete; RabbitMQ, Kafka, and Amazon SNS/SQS transports live, Batch adapter pending.**
> The messaging engine, resilience, and persistence logic are
> implemented and tested. The RabbitMQ provider (via MassTransit),
> the Amazon SNS/SQS provider (via MassTransit, verified against
> LocalStack), and the Kafka provider (native Confluent.Kafka client)
> are implemented and verified against real brokers or LocalStack in
> containers, so the library can publish to and consume from any of
> them end-to-end.
> The Batch adapter and some host wiring remain intentionally
> deferred to follow-up specs
> (see [Current status](#current-status)).

## Highlights

- **Transport-neutral by design.** The core depends only on ports (`IMessagePublisher`,
  `IMessageConsumerHost`, `IMessagingProvider`). A `MessagingMode` selects the active provider at
  startup, so switching transports is a configuration and registration change, not a code change.
- **Clean Architecture layering.** Dependencies point inward: Domain (`Messaging.Abstractions`,
  `Messaging.Contracts`) has no external package dependencies; the Application layer
  (`Messaging`) composes ports via the Microsoft *abstractions* packages only; concrete adapters
  live in their own Infrastructure packages.
- **Reliable publishing (transactional outbox).** Outgoing messages are recorded in the same
  database transaction as the originating state change, then dispatched out-of-band with
  at-least-once delivery and a bounded dispatch-attempt cap.
- **Idempotent consumption.** A deterministic idempotency key derived from stable message identity
  lets consumers safely absorb the duplicates that at-least-once delivery can produce.
- **Uniform resilience.** A single `RetryPolicyConfig` (max attempts, backoff shape, per-attempt
  delay, clamped 1–300s) maps onto the active transport. Failures are classified transient vs.
  non-transient; exhausted or poison messages are routed to a dead-letter sink.
- **Observability without vendor lock-in.** Metrics and tracing are emitted through the standard
  .NET primitives (`System.Diagnostics.Metrics.Meter`, `ActivitySource`) under the name
  `Messaging.Core`, so any OpenTelemetry or Prometheus exporter can observe them. Logging goes
  through an `IMessagingLog` port with payload redaction, so no concrete logger or secret leaks
  into the core.
- **Graceful lifecycle.** A consumer-host state machine and a shutdown coordinator drain in-flight
  work within a configurable timeout.
- **Fail-fast configuration.** Configuration is validated at startup — bounds, required
  provider-specific settings, and provider resolution — before any message is processed.

## Projects

| Project | Layer | Responsibility |
|---|---|---|
| `Messaging.Contracts` | Domain | Transport-neutral `MessageEnvelope` and shared contracts; no external dependencies. |
| `Messaging.Abstractions` | Domain | Ports (interfaces), modes, and value types; references only `Messaging.Contracts`. |
| `Messaging` | Application | Publish/consume pipelines, resilience, observability, configuration, lifecycle, and DI registration. |
| `Messaging.Persistence` | Infrastructure | EF Core outbox recorder/dispatcher and idempotency store (PostgreSQL). |
| `Messaging.MassTransit` | Infrastructure | MassTransit provider adapter for broker-shaped modes. **RabbitMQ and Amazon SNS/SQS (`AmazonSqs`) transports implemented** (bus adapter + consume processor). RabbitMQ is verified against a real broker, SNS/SQS against LocalStack. |
| `Messaging.Kafka` | Infrastructure | Kafka streaming provider adapter over the native `Confluent.Kafka` client. **Implemented**, verified against a real broker. |
| `Messaging.Batch` | Infrastructure | Scheduled/triggered batch provider adapter. *Shell.* |

## Requirements

- .NET 8 SDK
- PostgreSQL (only when using the `Messaging.Persistence` outbox/idempotency stores)
- Docker (integration tests start RabbitMQ, Kafka, PostgreSQL, and LocalStack (SNS/SQS) containers via Testcontainers;
  `docker compose up -d --wait` starts a local Kafka broker on `localhost:9092`, and LocalStack
  for SNS/SQS on `localhost:4566`)

Nullable reference types and implicit usings are enabled across the solution, and package versions
are pinned to exact patches.

## Build and test

```bash
dotnet build MessagingCore.sln
dotnet test MessagingCore.sln
```

## Current status

Implemented and ready:

- Core publish and consume pipelines (correlation-id handling, envelope validation, contract
  binding, idempotency gate, handler dispatch).
- Resilience: failure classification, bounded retry with backoff, dead-letter routing.
- Transactional outbox recorder and dispatcher *logic*, and the idempotency store and sweep.
- Observability emission (metrics, tracing, structured-logging port, payload redaction).
- Startup configuration validation and provider resolution.
- **RabbitMQ transport (via MassTransit).** The `Messaging.MassTransit` shell is now a live
  RabbitMQ provider — see highlights below.
- **Kafka transport (native Confluent.Kafka).** The
  `Messaging.Kafka` shell is now a live Streaming provider —
  see [Kafka provider highlights](#kafka-provider-highlights).
- **Amazon SNS/SQS transport (via MassTransit).** The `AmazonSqs` transport in
  `Messaging.MassTransit` is live and verified against LocalStack — see
  [Amazon SNS/SQS provider](#amazon-snssqs-provider-local-development).
- **Idempotency recording fix.** The consume pipeline now records the idempotency key after the
  handler succeeds — see
  [Shared core change: idempotency recording](#shared-core-change-idempotency-recording).
- **Missing idempotency key fix.** A message whose idempotency
  key cannot be derived is now dead-lettered on the first
  attempt instead of being retried — see
  [Bugfix: missing idempotency key classification](#bugfix-missing-idempotency-key-classification).

### RabbitMQ provider highlights

Delivered by the `rabbitmq-provider` spec, RabbitMQ is reached **through MassTransit** (the
`MassTransit.RabbitMQ` transport over the official RabbitMQ client); the core stays
transport-neutral and selects the provider at startup.

- **Live seams filled in.** Production implementations of the bus adapter (`IMassTransitBusAdapter`
  — start/stop, publish/send, transport reachability) and the consume processor
  (`IMassTransitConsumeProcessor`), which drives the core consume steps ahead of the application
  handler.
- **Both broker-shaped modes.** Publish-Subscribe maps to the MassTransit publish endpoint (fan-out)
  and Message Queue maps to the send endpoint (point-to-point).
- **Role-scoped composition.** A service registers as publisher-only, consumer-only, or both;
  unused machinery (consume loop or publish path) is never started.
- **Core resilience applied once.** Correlation propagation, idempotency gate, contract binding,
  failure classification, bounded retry with backoff, and dead-letter routing run through the core
  pipeline — MassTransit-level redelivery is deliberately not layered on top, so attempt counts
  stay bounded.
- **Envelope fidelity + ack/retain semantics.** Message id, correlation id, type, timestamp,
  contract version, headers, and payload bytes are preserved across the broker; messages are
  acknowledged only on success and retained for redelivery when dead-lettering fails.
- **Fail-fast config, no secret exposure.** RabbitMQ settings (including the secret-bearing
  connection string) are validated at startup and referenced by setting name — never by value — in
  logs, traces, and health output. Production defaults to a hosted Azure RabbitMQ broker with
  credentials injected at runtime.
- **Verified against a real broker.** Integration tests run against a containerized RabbitMQ
  (pinned image tag) covering a publish-then-consume round trip and retry-exhaustion
  dead-lettering, alongside property tests for the invariants above.

### Kafka provider highlights

Delivered by the `kafka-provider` spec. Unlike RabbitMQ, Kafka is
reached through the **native `Confluent.Kafka` client**, not the
MassTransit Kafka Rider. Streaming's defining concern is explicit
consumer-group membership and manual offset-commit control, which
the Rider abstracts away. The core stays transport-neutral.

- **Live seams filled in.** Production implementations of the
  producer (`IKafkaProducer`, `Acks.All` with idempotence enabled),
  the consumer (`IKafkaConsumer`, auto-commit disabled, commits the
  exact offset processed), the stream message processor
  (`IStreamMessageProcessor`), and the reachability probe
  (`IKafkaTransportProbe`, a bounded admin-client metadata call).
- **Hosted consume loop.** `KafkaConsumerHostedService` pumps the
  consume, process, commit iteration while the host is running,
  drains gracefully on shutdown, and keeps going after a per-record
  failure.
- **Commit after process.** An offset is committed only after the
  core pipeline returns. Success or a routed dead-letter commits. A
  dead-letter that could not be routed throws
  `RetainOffsetException`, so the offset is not committed and the
  record is redelivered (at-least-once).
- **Core resilience applied once.** As with RabbitMQ, the core
  pipeline owns idempotency, contract binding, classification,
  bounded retry with backoff, and dead-lettering. No client-side
  redelivery policy is layered on top.
- **Envelope fidelity.** Message id, correlation id, type,
  timestamp, contract version, user headers, and payload bytes
  round-trip through Kafka record headers and value
  (`KafkaWireFormat`).
- **Config and security.** `KafkaClientConfigFactory` builds
  producer, consumer, and admin configs from
  `Kafka:BootstrapServers`, `Kafka:ConsumerGroupId`, and
  `Kafka:Topic`, plus optional security protocol, SASL mechanism,
  injected credentials, and TLS CA location. Secrets are injected
  and never logged.
- **No topic auto-administration.** Topic creation stays out of the
  production path, in line with the no-auto-migration rule. The
  admin client is used only for the read-only reachability probe.
- **Verified against a real broker.** Integration tests run against
  a containerized Kafka (`confluentinc/cp-kafka:7.6.1`, pinned)
  covering at-least-once redelivery with manual offset commit.
  `docker-compose.yml` provides the same image for local
  development.

### Bugfix: missing idempotency key classification

Delivered by the `missing-idempotency-key-classification-fix` spec.

- **Defect.** With idempotent consumption enabled, an envelope whose
  key cannot be derived (for example `MessageId == Guid.Empty`) made
  `IdempotencyGate` throw `MissingIdempotencyKeyException`. That type
  derived from plain `Exception`, so `FailureClassifier` fell through
  to its `Transient` default and retried the message up to
  `MaxAttempts` before dead-lettering it (`AttemptCount == 3` with
  `MaxAttempts = 3`). The handler never ran, but attempts and backoff
  were wasted and the commit or acknowledgement was delayed. The
  defect sat in the shared pipeline, so it affected both the Kafka
  and MassTransit consume paths.
- **Fix.** `MissingIdempotencyKeyException` now derives from
  `MessagingFailureException` and reports `FailureKind.NonTransient`.
  `FailureClassifier` logic is unchanged; its existing
  `MessagingFailureException` rule picks up the declared kind. The
  type, `MessageType` diagnostic, and payload-free message text are
  preserved, so existing callers are unaffected.
- **Result.** An underivable key is dead-lettered on the first
  attempt (`AttemptCount == 1`) for any `MaxAttempts`, with no
  handler invocation and no idempotency store lookups or writes.
  Transient failures still retry up to `MaxAttempts`, other failure
  kinds keep their classification, and unrecognized exceptions
  still default to `Transient`.
- **Tests.** A property test across arbitrary `MaxAttempts`,
  classifier unit tests, a MassTransit processor test, the
  previously skipped Kafka test
  `ProcessAsync_MissingIdempotencyKey_IsNotRetried`, and
  preservation property tests for the unchanged behavior.

Intentionally deferred to follow-up specs (a consuming service must supply these before going
live end-to-end):

- **Batch transport adapter.** The Batch provider is still a shell;
  its bus/consume seams are not yet implemented, so that transport
  does not publish or consume yet.
- **Outbox dispatch scheduler.** The dispatcher runs one pass per call; no background service
  drives it on an interval yet, so recorded outbox entries are not published automatically.
- **Observability backend wiring.** The host must register an exporter (e.g. OpenTelemetry) for
  the `Messaging.Core` meter/activity source and a concrete `IMessagingLog` adapter.

## Amazon SNS/SQS provider (local development)

The `Messaging.MassTransit` project also hosts the Amazon SNS/SQS transport (`AmazonSqs`), which
backs Publish-Subscribe (SNS fan-out to SQS queues) and Message Queue (point-to-point over SQS).
For local development and integration tests it runs against LocalStack, not real AWS.

### Start the environment

```bash
docker compose up -d --wait
```

The LocalStack service exposes SNS and SQS on `http://localhost:4566`. Its health check queries both
APIs, so the command returns once both are available. It needs no auth token and no real credentials.

### Select the transport

| Setting | Local value | Notes |
|---|---|---|
| `MassTransit:Transport` | `AmazonSqs` | Selects the SNS/SQS transport. |
| `AmazonSqs:Region` | `us-east-1` | Required. Any valid AWS region identifier. |
| `AmazonSqs:EndpointName` | e.g. `orders-service` | Required. Derives the consumer queue name. |
| `AmazonSqs:ServiceUrl` | `http://localhost:4566` | Optional endpoint override. Plain `http` is accepted only for local hosts. |
| `AmazonSqs:AccessKeyId` | `test` | Dummy value for LocalStack only. Set together with the secret key. |
| `AmazonSqs:SecretAccessKey` | `test` | Dummy value for LocalStack only. |

`ServiceUrl`, the keys, and the session token are treated as secret-bearing and are never logged or
echoed in validation errors. In a real AWS deployment leave `ServiceUrl` and the keys unset so the
default AWS credential chain applies. Other settings (`AutoCreateTopology`, `RawMessageDelivery`,
`Fifo`, `MaxReceiveCount`, `VisibilityTimeoutSeconds`, name prefixes, and so on) are declared in
`AmazonSqsProviderSettings` with defaults and are validated at startup.

### Modes and FIFO

- **Publish-Subscribe.** The publisher sends to an SNS topic and each subscribing consumer gets its
  own SQS queue subscribed to that topic (fan-out). `AmazonSqs:RawMessageDelivery` (default `true`)
  chooses raw or SNS-wrapped delivery to the queue.
- **Message Queue.** Point-to-point over SQS; competing consumers share one queue.
- **FIFO.** Set `AmazonSqs:Fifo` to `true` for FIFO topics and queues. `AmazonSqs:FifoGroupSelection`
  picks the message group id: `CorrelationId` (default) or `MessageType`.

### Settings reference

All names are prefixed `AmazonSqs:` and bound from the provider-settings bag. Values out of range are
rejected at startup. Only `Region` and `EndpointName` are required.

| Setting | Default | Notes |
|---|---|---|
| `Region` | none | Required. AWS region identifier. |
| `EndpointName` | none | Required. |
| `ServiceUrl` | unset | Endpoint override (LocalStack). |
| `AccessKeyId`, `SecretAccessKey`, `SessionToken` | unset | Explicit credentials. When unset, the default AWS credential chain applies. |
| `TopicNamePrefix`, `QueueNamePrefix` | empty | Prepended to derived topic and queue names. |
| `DeadLetterQueueSuffix` | `_error` | Derives the dead-letter queue name. |
| `AutoCreateTopology` | `true` | `true` creates missing topics, queues, and subscriptions. `false` only verifies they exist. |
| `RawMessageDelivery` | `true` | Raw delivery on SNS-to-SQS subscriptions. |
| `Fifo` | `false` | FIFO topics and queues. |
| `FifoGroupSelection` | `CorrelationId` | `CorrelationId` or `MessageType`. |
| `KmsMasterKeyId` | unset | Optional server-side encryption key. |
| `MaxReceiveCount` | `5` | Redrive threshold to the dead-letter queue (1 to 1000). |
| `VisibilityTimeoutSeconds` | `30` | 1 to 43200. |
| `MessageRetentionSeconds` | `345600` (4 days) | 60 to 1209600. |
| `MaxMessageSizeBytes` | `262144` (256 KiB) | 1 to 262144, the SQS limit. |
| `WaitTimeSeconds` | `20` | Long-poll wait, 0 to 20. |
| `MaxMessagesPerReceive` | `10` | 1 to 10. |
| `ConcurrencyLimit` | `10` | 1 to 1000. |

**Credentials.** Never put real keys in committed configuration. In AWS, rely on the default
credential chain (instance or task role). If explicit keys are needed, supply them from environment
variables or a secret store. The `test`/`test` values above are for LocalStack only.

### Run the tests

```bash
# Unit tests (no Docker)
dotnet test tests/Messaging.MassTransit.Tests --filter "Category!=Integration"

# Integration tests (require Docker; LocalStack is started via Testcontainers)
dotnet test tests/Messaging.MassTransit.Tests --filter "Category=Integration" --blame-hang-timeout 2m
```

### LocalStack image tag

Compose and the integration-test fixture both default to `localstack/localstack:4.14.0`. To try
another patch version, set `LOCALSTACK_IMAGE_TAG`:

```bash
LOCALSTACK_IMAGE_TAG=4.14.0 docker compose up -d --wait
```

Only exact `major.minor.patch` tags are accepted by the test fixture; `latest` and bare major tags
are rejected.

**Why the tag is pinned.** LocalStack images published after the 4.x line (the unified image) require
a `LOCALSTACK_AUTH_TOKEN`. The 4.x line runs without one but receives no further updates, so `4.14.0`
is pinned deliberately to keep local development and CI free of a token.

### IAM actions by role

Grant only what the role needs. The second column applies when `AmazonSqs:AutoCreateTopology` is
`true`; with `false` the provider only verifies that topology exists.

| Role | Always | Added when `AutoCreateTopology = true` |
|---|---|---|
| Publisher | `sns:Publish`, `sns:GetTopicAttributes`, `sns:ListTopics` (reachability probe), `sqs:ListQueues` (reachability probe), `sqs:SendMessage` (Message Queue mode) | `sns:CreateTopic` |
| Consumer | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, `sqs:SendMessage` (dead-letter queue), `sns:ListSubscriptionsByTopic`, `sns:GetTopicAttributes` | `sqs:CreateQueue`, `sqs:SetQueueAttributes`, `sns:CreateTopic`, `sns:Subscribe` |

**Verify-only deployments.** The provider configures MassTransit not to manage topology, so at bus
start MassTransit only looks up the queue the provider already verified or created (spike R1). A
verify-only deployment (`AutoCreateTopology = false`, resources provisioned by infrastructure code)
therefore needs no `sqs:CreateQueue`, `sqs:SetQueueAttributes`, or `sns:*` create permissions for
the topology itself.

**Open item: temporary bus queue.** MassTransit also creates an auto-delete temporary bus queue
(`{machine}_..._bus_*`). Whether verify-only roles need `sqs:CreateQueue` (and queue deletion) for it
has not yet been verified against a real IAM policy (design risk R2). Until it is, validate your
policy in a non-production account before removing create permissions from consumer roles.

### Shared core change: idempotency recording

Delivered with the SNS/SQS provider but applies to every provider (RabbitMQ, Kafka, SNS/SQS). The
consume pipeline now records the idempotency key **after** the handler succeeds. A handler failure
records nothing, and a failure while recording propagates, so the message is not acknowledged and is
redelivered under the provider's at-least-once semantics.

## Architecture notes

- The core provides a uniform retry/backoff configuration surface rather than a bespoke engine, and
  it is the single source of retry behavior: for the RabbitMQ, Kafka, and Amazon SNS/SQS providers the core pipeline owns
  retry, backoff, and dead-lettering, and a transport-level redelivery policy is deliberately not
  layered on top so attempt counts stay bounded. The consumer pulls in-process at its own pace, so
  backpressure is natural and no circuit breaker is needed on the consume path.
- Delivery semantics are **at-least-once**; idempotent consumption absorbs duplicates.
- Message payloads are treated as untrusted on consume and bound only to known, pre-registered
  contracts.
