# Messaging Core

A transport-neutral messaging library for .NET services. It defines a uniform publish/consume
surface — reliable publishing, idempotent consumption, retries with backoff, dead-lettering,
graceful shutdown, and observability — and keeps the concrete transport (RabbitMQ, Kafka, batch)
behind swappable provider adapters.

> **Status: core complete; RabbitMQ transport live, other adapters pending.** The messaging
> engine, resilience, and persistence logic are implemented and tested. The RabbitMQ provider (via
> MassTransit) is now implemented and verified against a real broker in containers, so the library
> can publish to and consume from RabbitMQ end-to-end. The Kafka and Batch adapters and some host
> wiring remain intentionally deferred to follow-up specs (see [Current status](#current-status)).

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
| `Messaging.Kafka` | Infrastructure | Kafka streaming provider adapter. *Shell.* |
| `Messaging.Batch` | Infrastructure | Scheduled/triggered batch provider adapter. *Shell.* |

## Requirements

- .NET 8 SDK
- PostgreSQL (only when using the `Messaging.Persistence` outbox/idempotency stores)

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

Intentionally deferred to follow-up specs (a consuming service must supply these before going
live end-to-end):

- **Remaining transport adapters.** The Kafka and Batch providers are still shells; their
  bus/consume seams are not yet implemented, so those transports do not publish or consume yet.
- **Outbox dispatch scheduler.** The dispatcher runs one pass per call; no background service
  drives it on an interval yet, so recorded outbox entries are not published automatically.
- **Observability backend wiring.** The host must register an exporter (e.g. OpenTelemetry) for
  the `Messaging.Core` meter/activity source and a concrete `IMessagingLog` adapter.

## Architecture notes

- The core provides a uniform retry/backoff configuration surface rather than a bespoke engine, and
  it is the single source of retry behavior: for the RabbitMQ provider the core pipeline owns
  retry, backoff, and dead-lettering, and a transport-level redelivery policy is deliberately not
  layered on top so attempt counts stay bounded. The consumer pulls in-process at its own pace, so
  backpressure is natural and no circuit breaker is needed on the consume path.
- Delivery semantics are **at-least-once**; idempotent consumption absorbs duplicates.
- Message payloads are treated as untrusted on consume and bound only to known, pre-registered
  contracts.
