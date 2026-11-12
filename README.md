# Messaging Core

A transport-neutral messaging library for .NET services. It defines a uniform publish/consume
surface — reliable publishing, idempotent consumption, retries with backoff, dead-lettering,
graceful shutdown, and observability — and keeps the concrete transport (RabbitMQ, Kafka, batch)
behind swappable provider adapters.

> **Status: core complete; RabbitMQ and Kafka transports live, Batch adapter pending.**
> The messaging engine, resilience, and persistence logic are
> implemented and tested. The RabbitMQ provider (via MassTransit)
> and the Kafka provider (native Confluent.Kafka client) are
> implemented and verified against real brokers in containers, so
> the library can publish to and consume from either end-to-end.
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
| `Messaging.MassTransit` | Infrastructure | MassTransit provider adapter for broker-shaped modes. **RabbitMQ transport implemented** (bus adapter + consume processor), verified against a real broker. |
| `Messaging.Kafka` | Infrastructure | Kafka streaming provider adapter over the native `Confluent.Kafka` client. **Implemented**, verified against a real broker. |
| `Messaging.Batch` | Infrastructure | Scheduled/triggered batch provider adapter. *Shell.* |

## Requirements

- .NET 8 SDK
- PostgreSQL (only when using the `Messaging.Persistence` outbox/idempotency stores)
- Docker (integration tests start RabbitMQ, Kafka, and PostgreSQL containers via Testcontainers;
  `docker compose up -d --wait` starts a local Kafka broker on `localhost:9092`)

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

## Architecture notes

- The core provides a uniform retry/backoff configuration surface rather than a bespoke engine, and
  it is the single source of retry behavior: for the RabbitMQ and Kafka providers the core pipeline owns
  retry, backoff, and dead-lettering, and a transport-level redelivery policy is deliberately not
  layered on top so attempt counts stay bounded. The consumer pulls in-process at its own pace, so
  backpressure is natural and no circuit breaker is needed on the consume path.
- Delivery semantics are **at-least-once**; idempotent consumption absorbs duplicates.
- Message payloads are treated as untrusted on consume and bound only to known, pre-registered
  contracts.
