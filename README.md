# Messaging Core

A transport-neutral messaging library for .NET services. It defines a uniform publish/consume
surface — reliable publishing, idempotent consumption, retries with backoff, dead-lettering,
graceful shutdown, and observability — and keeps the concrete transport (RabbitMQ, Kafka, batch)
behind swappable provider adapters.

> **Status: core complete, transport adapters pending.** The messaging engine, resilience, and
> persistence logic are implemented and tested. The live-broker adapters and some host wiring are
> intentionally deferred to follow-up specs (see [Current status](#current-status)). The library
> builds and resolves today, but does not yet publish to or consume from a real broker.

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
| `Messaging.MassTransit` | Infrastructure | MassTransit provider adapter for broker-shaped modes (RabbitMQ). *Shell — bus adapter pending.* |
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

Intentionally deferred to follow-up specs (a consuming service must supply these before going
live end-to-end):

- **Live transport adapters.** The MassTransit/Kafka/Batch providers are shells; the bus/consume
  seams (e.g. `IMassTransitBusAdapter`, `IMassTransitConsumeProcessor`) are not yet implemented,
  so nothing publishes to or consumes from a real broker yet.
- **Outbox dispatch scheduler.** The dispatcher runs one pass per call; no background service
  drives it on an interval yet, so recorded outbox entries are not published automatically.
- **Observability backend wiring.** The host must register an exporter (e.g. OpenTelemetry) for
  the `Messaging.Core` meter/activity source and a concrete `IMessagingLog` adapter.

## Architecture notes

- Resilience is delegated to each transport's native retry/redelivery mechanism; the core provides
  a uniform configuration surface rather than a bespoke engine. The consumer pulls in-process at
  its own pace, so backpressure is natural and no circuit breaker is needed on the consume path.
- Delivery semantics are **at-least-once**; idempotent consumption absorbs duplicates.
- Message payloads are treated as untrusted on consume and bound only to known, pre-registered
  contracts.
