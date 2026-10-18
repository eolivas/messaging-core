# Implementation Plan: RabbitMQ Provider

## Overview

This plan completes the existing `Messaging.MassTransit` adapter shell by supplying production
implementations for its two open seam ports (`IMassTransitBusAdapter`, `IMassTransitConsumeProcessor`)
and the role-scoped DI wiring that stands them up, reaching RabbitMQ through the
`MassTransit.RabbitMQ` transport. Every transport-neutral core contract is consumed unchanged.

The implementation language is **C#** (net8.0), inferred from the design and the existing codebase —
no language selection is needed. Tasks build incrementally: package + data models first, then the two
seam adapters, then the DI wiring and validation, then end with integration wiring against a real
broker so there is no orphaned code. All production adapters live in the `Messaging.MassTransit`
Infrastructure package and depend inward on the core ports only (Clean Architecture). Test work is
paired with each code task; property tests use `FsCheck.Xunit` and integration tests use
`Testcontainers.RabbitMq` pinned to `rabbitmq:3.13-management-alpine`.

## Tasks

- [x] 1. Add the RabbitMQ transport package and the shared Infrastructure data models
  - [x] 1.1 Pin `MassTransit.RabbitMQ` 8.5.11 in the adapter project
    - Add `<PackageReference Include="MassTransit.RabbitMQ" Version="8.5.11" />` to
      `src/Messaging.MassTransit/Messaging.MassTransit.csproj`, exactly matching the pinned core
      `MassTransit` version so the transport and core versions never drift
    - This brings in the official `RabbitMQ.Client` transitively (no separate client adapter)
    - Confirm the project restores and builds with the new reference
    - _Requirements: 1.1_

  - [x] 1.2 Promote `EnvelopeMessage` into the Infrastructure package
    - Create `src/Messaging.MassTransit/Transport/EnvelopeMessage.cs` with the wire contract carrying
      every envelope field as data (`MessageId`, `CorrelationId`, `MessageType`, `CreatedAtUtc`,
      `ContractVersion`, `Headers`, `Payload` bytes)
    - Implement `FromEnvelope` copying headers with `StringComparer.Ordinal` and `Payload.ToArray()`,
      and `ToEnvelope` wrapping the bytes back in `ReadOnlyMemory<byte>` — a lossless round trip
    - Source the implementation from the proven test-local `EnvelopeMessage` in
      `tests/.../MassTransitRabbitMqIntegrationSupport.cs`
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 1.3 Write property test for envelope fidelity round trip
    - **Property 1: Envelope fidelity round trip** — `ToEnvelope(FromEnvelope(e))` preserves message
      id, correlation id, message type, UTC timestamp, contract version, header count/keys/values, and
      every payload byte
    - Generate arbitrary envelopes (correlation 1–128 chars, 0–64 headers, arbitrary payload incl.
      empty); `[Property]`, min 100 iterations; tag comment "Feature: rabbitmq-provider, Property 1"
    - **Validates: Requirements 6.1, 6.2, 6.3**

  - [x] 1.4 Add the `MassTransitRole` enum
    - Create `src/Messaging.MassTransit/Configuration/MassTransitRole.cs` with `Publisher`,
      `Consumer`, `Both` — the selector that drives role-scoped endpoint registration
    - _Requirements: 4.1, 4.3, 4.5_

  - [x] 1.5 Add the `RetainForRedeliveryException` domain-specific exception
    - Create `src/Messaging.MassTransit/RetainForRedeliveryException.cs` (Infrastructure), a dedicated
      exception type (not bare `Exception`) thrown only when the dead-letter sink reports
      `RetainedUnacknowledged`, signaling MassTransit to redeliver
    - _Requirements: 6.7, 8.7_

- [x] 2. Implement `EnvelopeConsumer` and `CoreConsumeProcessor` (the consume seam)
  - [x] 2.1 Promote `EnvelopeConsumer` into the Infrastructure package
    - Create `src/Messaging.MassTransit/Consuming/EnvelopeConsumer.cs` implementing
      `IConsumer<EnvelopeMessage>`, rebuilding the `MessageEnvelope` via `ToEnvelope()` and forwarding
      to `IMassTransitConsumeProcessor.ProcessAsync(envelope, context.CancellationToken)`
    - Constructor injection of the processor only (DIP); null-guard the dependency
    - _Requirements: 1.3, 6.4, 6.5_

  - [x] 2.2 Implement `CoreConsumeProcessor : IMassTransitConsumeProcessor`
    - Create `src/Messaging.MassTransit/Consuming/CoreConsumeProcessor.cs` driving the core pipeline
      via constructor-injected `ObservabilityContext`, `RetryExecutor`, `ConsumePipeline` (used through
      `DeliverAsync`), `DeadLetterRouter`, and `RetryPolicyConfig`
    - Algorithm: call `ObservabilityContext.OnConsume(correlationId, headers)` (best-effort), then
      `RetryExecutor.ExecuteAsync(envelope, policy, processAsync: ct => consumePipeline.DeliverAsync(envelope, ct), idempotencyKey: null, ct)`
    - Map terminal outcome: `Succeeded` → return (ack); dead-letter outcomes → if
      `RetainedUnacknowledged` throw `RetainForRedeliveryException`, else return (routed+acked)
    - `UnknownContractException` and `MissingIdempotencyKeyException` are non-transient → dead-letter,
      handler never invoked
    - Async, ends in `Async`, takes `CancellationToken`; one responsibility (drive, don't reimplement)
    - _Requirements: 1.3, 1.9, 6.4, 6.5, 6.6, 6.7, 7.2, 7.3, 7.5, 7.6, 8.1, 8.3, 8.5, 8.6, 8.7, 8.8, 9.2_

  - [x] 2.3 Write property test for ack-versus-retain mapping
    - **Property 3: Ack-versus-retain mapping** — processor returns iff the message leaves the
      pipeline (success / duplicate / bypass / routed-and-acked) and throws iff the sink retained it
    - Generate arbitrary terminal outcomes using a fake retry-executor/pipeline seam (no broker);
      `[Property]`, min 100 iterations; tag comment "Feature: rabbitmq-provider, Property 3"
    - **Validates: Requirements 1.9, 6.7, 7.2, 8.6, 8.7**

  - [x] 2.4 Write property test for bounded attempt count by failure kind
    - **Property 4: Bounded attempt count by failure kind** — always-failing processing yields exactly
      1 attempt (non-transient) or exactly `MaxAttempts` (transient), never more
    - Generate `MaxAttempts` in [1, 100] and a failure kind; inject the delay function so there is no
      wall-clock wait, mock the sink; `[Property]`, min 100 iterations; tag comment
      "Feature: rabbitmq-provider, Property 4"
    - **Validates: Requirements 8.1, 8.3, 8.8**

  - [x] 2.5 Write property test for dead-letter context preservation
    - **Property 5: Dead-letter preserves context** — a dead-lettered message preserves message id,
      correlation id, payload bytes (length + every byte), and exact total attempt count
    - Generate arbitrary failing envelopes + attempt counts, capture the `DeadLetterMessage`;
      `[Property]`, min 100 iterations; tag comment "Feature: rabbitmq-provider, Property 5"
    - **Validates: Requirements 8.5**

  - [x] 2.6 Write unit tests for processor flows
    - Unknown/unbindable contract → dead-letter, handler not invoked, reason names the contract
      (Req 6.5, 6.6); idempotent duplicate → handler not invoked, acked (Req 7.4); idempotency
      disabled → handler invoked with null key (Req 7.6); missing idempotency key → dead-letter, key
      not recorded (Req 7.5); observability hook invoked with correlation id on consume (Req 9.2);
      throwing observability sink does not fail the operation (Req 9.8)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 6.5, 6.6, 7.4, 7.5, 7.6, 9.2, 9.8_

- [x] 3. Checkpoint - consume seam
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement `RabbitMqBusAdapter` (the bus seam)
  - [x] 4.1 Implement bus construction and role-scoped endpoint registration
    - Create `src/Messaging.MassTransit/Transport/RabbitMqBusAdapter.cs` implementing
      `IMassTransitBusAdapter` over an `IBusControl` built by a factory from
      `(MassTransitProviderSettings settings, MassTransitRole role, IMassTransitConsumeProcessor processor, ObservabilityContext observability, ILogger)`
    - Build via `Bus.Factory.CreateUsingRabbitMq(cfg => { cfg.Host(new Uri(settings.ConnectionString)); ... })`
    - Role = `Publisher`: register NO receive endpoint; Role = `Consumer`/`Both`: register EXACTLY ONE
      `ReceiveEndpoint` with `UseMessageRetry(r => r.None())` and `Consumer(() => new EnvelopeConsumer(processor))`
    - _Requirements: 1.2, 4.2, 4.3, 4.5, 8.8_

  - [x] 4.2 Implement `StartBusAsync` / `StopBusAsync` with redacted bus-state logging
    - `StartBusAsync`: start `IBusControl`, set started flag, emit structured bus-state=started log; on
      start failure surface the error and leave started=false
    - `StopBusAsync`: stop `IBusControl` (drains in-flight), clear started flag, emit bus-state=stopped
      log; reference the broker only by non-secret host display name, never the connection string
    - Async, end in `Async`, take `CancellationToken`
    - _Requirements: 5.1, 5.2, 5.3, 5.5, 9.3, 9.4, 9.5_

  - [x] 4.3 Implement `PublishAsync` with mode shaping and header merge
    - Map `MessageEnvelope` → `EnvelopeMessage`; call `ObservabilityContext.OnPublish(correlationId)`
    - Pub/Sub mode → `IPublishEndpoint.Publish`; Queue mode → resolve `ISendEndpoint` and `Send`
    - Merge correlation + trace headers so the correlation id is carried in published headers
    - _Requirements: 1.5, 1.6, 6.1, 6.2, 6.3, 9.1, 9.7_

  - [x] 4.4 Implement bounded `IsTransportReachableAsync`
    - Check the bus/broker connection wrapped in `Task.WhenAny(check, Task.Delay(TimeSpan.FromSeconds(1), ct))`
    - Return `false` on error or timeout; never throw; return `true` only when started and reachable
    - _Requirements: 1.7, 1.8, 10.1, 10.3, 10.4, 10.5_

  - [x] 4.5 Write property test for correlation id in published headers
    - **Property 2: Correlation id carried in published headers** — the headers handed to the transport
      on publish/send contain a correlation-id entry char-for-char equal to the envelope's correlation
      id
    - Drive the header-merge step with a fake endpoint capturing headers; `[Property]`, min 100
      iterations; tag comment "Feature: rabbitmq-provider, Property 2"
    - **Validates: Requirements 6.4, 9.7**

  - [x] 4.6 Write property test for no secret in emitted diagnostics
    - **Property 6: No connection string/secret in diagnostics** — no bus-state log, transport-health
      dependency name, or connection-failure report contains the connection string (or any fragment);
      the broker is referenced only by host display name; sensitive values equal the fixed redaction
      marker
    - Generate arbitrary connection-string values, capture diagnostics via a capturing logger;
      `[Property]`, min 100 iterations; tag comment "Feature: rabbitmq-provider, Property 6"
    - **Validates: Requirements 9.5, 9.6, 10.2, 12.5, 12.6**

  - [x] 4.7 Write unit tests for bus adapter behaviors
    - Mode→endpoint selection: Pub/Sub uses publish endpoint, Queue uses send endpoint (fake bus) —
      Req 1.5, 1.6; reachability mapping: started→reachable, not-started→false, throwing check→false
      (no propagation), injected slow check→false on timeout — Req 10.1–10.5; bus start failure leaves
      started=false and surfaces the error — Req 5.5
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 1.5, 1.6, 5.5, 10.1, 10.3, 10.4, 10.5_

- [x] 5. Checkpoint - bus seam
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Integrate connection-string format validation
  - [x] 6.1 Add AMQP(S) connection-string format validation to provider settings
    - In `src/Messaging.MassTransit/Configuration/MassTransitProviderSettings.cs`, add the
      connection-string format check for the RabbitMQ transport: empty, whitespace-only, or
      unparseable AMQP(S) URI → invalid
    - Surface it as a descriptor error reported by setting name only, excluding the value, flowing
      through the existing `ConfigurationValidator` single-pass aggregation
    - _Requirements: 3.5, 12.4_

  - [ ]* 6.2 Write unit tests for settings validation
    - Missing connection string reported by name (Req 3.2, 12.3); invalid empty/whitespace/unparseable
      reported by name with value excluded (Req 3.5, 12.4); two-at-once aggregation reports each
      failure separately (Req 3.4)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 3.2, 3.4, 3.5, 12.3, 12.4_

- [x] 7. Extend DI wiring to register the seams role-scoped with fail-fast
  - [x] 7.1 Register the seam adapters and role-scoped composition
    - In `src/Messaging.MassTransit/MassTransitMessagingProviderServiceCollectionExtensions.cs`, accept
      or infer a `MassTransitRole`, register `IMassTransitBusAdapter → RabbitMqBusAdapter` via the
      factory as a singleton (stable for the instance lifetime), and register
      `IMassTransitConsumeProcessor → CoreConsumeProcessor` for consumer/both only
    - Ensure publisher-only builds a bus with no receive endpoint; consumer/both build exactly one
    - _Requirements: 2.1, 2.4, 4.1, 4.2, 4.3, 4.4, 4.5_

  - [x] 7.2 Fail fast on unsupported mode before partial registration
    - When the configured transport is `RabbitMq` but the mode is neither Pub/Sub nor Queue, throw
      before any seam is registered, mirroring the provider ctor guard, and report that RabbitMQ backs
      only the Publish-Subscribe and Message Queue modes
    - _Requirements: 2.3_

  - [x] 7.3 Write unit tests for role-scoped composition and fail-fast
    - Publisher → 0 receive endpoints + publish enabled; consumer → 1 receive endpoint + app-publish
      disabled (dead-letter send allowed); both → 1 receive endpoint + publish enabled (Req 4.1–4.5);
      unsupported mode fails fast with no partial registration (Req 2.3); one RabbitMq registration
      resolves and returns the same instance on repeated resolution (Req 2.1, 2.2, 2.4)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 8. Checkpoint - wiring and validation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Re-point and complete containerized integration verification
  - [x] 9.1 Re-point the test-local adapter/consumer at the promoted production types
    - Remove the test-local `EnvelopeMessage`, `EnvelopeConsumer`, and `RabbitMqBusAdapter` from
      `tests/.../MassTransitRabbitMqIntegrationSupport.cs` and reference the production
      `Messaging.MassTransit` types instead, keeping `CapturingDeadLetterSink`, `EnvelopeFactory`, and
      the no-op observability helpers
    - Keep using `RabbitMqContainerFixture` (pinned `rabbitmq:3.13-management-alpine`, ready ≤ 60s,
      reaped after the run) and the shared `[Collection]`
    - _Requirements: 11.1, 11.2, 11.3, 11.6_

  - [x] 9.2 Verify publish-then-consume round trip over the real broker
    - In `tests/.../MassTransitRabbitMqIntegrationTests.cs`, assert within 30s that the delivered
      envelope's message id, correlation id, message type, UTC timestamp, contract version, headers,
      and payload bytes each equal the published values, across the real broker
    - _Requirements: 6.1, 6.2, 6.3, 11.3, 11.4_

  - [x] 9.3 Verify retry exhaustion → dead-letter over the real broker
    - Assert a poison message is routed to the dead-letter sink within 5s of the final failed attempt,
      is not redelivered to the application handler, and is not reported in-flight
    - _Requirements: 8.4, 11.5_

- [x] 10. Final checkpoint - full suite
  - Ensure all tests pass, ask the user if questions arise.
  - Verified: full solution build clean (0 warnings/0 errors); 263 tests passed, 0 failed,
    0 skipped across all projects. RabbitMQ integration tests ran against a real broker via
    Testcontainers (Docker available). Messaging.Abstractions.Tests contains no test methods.

## Notes

- Tasks marked with `*` are optional (test sub-tasks) and can be skipped for a faster MVP, though the
  property and integration tests carry this feature's correctness guarantees and are strongly advised.
- Each task references the specific requirement(s) and/or correctness property it implements.
- Property-based tests use `FsCheck.Xunit` (`[Property]`, min 100 iterations) and each carries a
  "Feature: rabbitmq-provider, Property {n}" tag comment; all six design properties are covered.
- Integration tests run against a real broker via Testcontainers, pinned to a fixed release tag.
- Checkpoints provide incremental validation at each seam boundary before wiring proceeds.
- The final tasks wire the promoted production types into the existing integration suite so no code is
  left orphaned.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.4", "1.5", "6.1"] },
    { "id": 1, "tasks": ["1.3", "2.1", "6.2"] },
    { "id": 2, "tasks": ["2.2"] },
    { "id": 3, "tasks": ["2.3", "2.4", "2.5", "2.6", "4.1"] },
    { "id": 4, "tasks": ["4.2", "4.3", "4.4"] },
    { "id": 5, "tasks": ["4.5", "4.6", "4.7", "7.1"] },
    { "id": 6, "tasks": ["7.2"] },
    { "id": 7, "tasks": ["7.3", "9.1"] },
    { "id": 8, "tasks": ["9.2", "9.3"] }
  ]
}
```
