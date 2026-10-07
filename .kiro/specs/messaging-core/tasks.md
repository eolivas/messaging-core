# Implementation Plan: Messaging Core

## Overview

This plan builds the foundational, mode-agnostic Messaging Core as a shared .NET library using
ports and adapters inside Clean Architecture. Work proceeds inward-out: first the `Domain` ports
and `Message_Envelope` (no external dependencies), then the shared contracts package, then the
`Application` pipelines and cross-cutting policies (validation, provider resolution, outbox,
retry/classification, idempotency, dead-letter, observability, lifecycle), then the EF
persistence stores, then a fake provider to exercise the core in isolation, and finally the
MassTransit / Kafka / Batch adapter shells plus DI wiring and health/lifecycle integration.

Each step builds on the previous one and ends by wiring the new piece into the pipeline so no
code is left orphaned. Property-based tests (FsCheck via `FsCheck.Xunit`, minimum 100 cases each,
tagged `Feature: messaging-core, Property {n}`) are placed next to the implementation they
validate; infrastructure-dependent properties (outbox, idempotency) run against a PostgreSQL
Testcontainer.

Language: **C#/.NET** (as specified throughout the design).

## Tasks

- [x] 1. Set up solution structure and core package skeletons
  - Create the solution and projects for `Messaging.Abstractions` (Domain), `Messaging`
    (Application core), `Messaging.Contracts`, `Messaging.MassTransit`, `Messaging.Kafka`,
    `Messaging.Batch`, and matching test projects
  - Enforce dependency direction: `Messaging.Abstractions` has no external package references;
    `Messaging` references only `Messaging.Abstractions`; adapter packages reference
    `Messaging.Abstractions`/`Messaging`
  - Add the xUnit test framework and the FsCheck.Xunit package to test projects
  - _Requirements: 3.8_

- [x] 2. Define the transport-neutral Message_Envelope and contract support (Messaging.Contracts / Domain)
  - [x] 2.1 Implement the `MessageEnvelope` record and envelope invariants
    - Add the immutable `MessageEnvelope` record (MessageId, CorrelationId, MessageType,
      CreatedAtUtc, Headers 0–64 entries, Payload, ContractVersion default 1)
    - Implement envelope validation: non-empty MessageId, non-empty MessageType, UTC timestamp,
      correlation id 1–128 chars, header count 0–64
    - Place the envelope and contract type definitions in the separately-versioned
      `Messaging.Contracts` package so producer and consumer share them without domain coupling
    - _Requirements: 4.1, 4.3, 4.7_

  - [x] 2.2 Write unit tests for header-count and field edge cases
    - Header counts 0, 1, 64 accepted; 65+ rejected; empty/overlong correlation id rejected
    - _Requirements: 4.1, 4.3_

- [x] 3. Define the core ports and result types (Domain)
  - Define `IMessagePublisher`, `IMessageConsumerHost` (+ `ConsumerHostState`), `IMessagingProvider`
    (+ `MessagingMode`, `ProviderSettingDescriptor`, `TransportHealth`), `IProviderRegistry`,
    `IConfigurationValidator`, `IOutboxDispatcher`, `IIdempotencyStore`, `IDeadLetterSink`,
    `IFailureClassifier` (+ `FailureKind`), `IIdempotencyKeyProvider`, and `IMessageHandler<TContract>`
  - Add result types: `PublishResult`, `ConfigurationValidationResult`, `ConfigurationError`,
    `DeadLetterMessage`
  - All I/O methods are async, return `Task`/`ValueTask`, and accept a `CancellationToken`
  - _Requirements: 3.1, 3.3, 3.4, 3.8_

- [x] 4. Define Messaging_Configuration model (Application)
  - Implement `MessagingConfiguration` with `Mode` (nullable), `RetryPolicyConfig`,
    `OutboxConfig`, `IdempotencyConfig`, `DeadLetterConfig`, `ObservabilityConfig`,
    `LifecycleConfig`, and the open `ProviderSettings` extension bag
  - Encode default values and documented bounds as metadata usable by the validator
    (max attempts 1–100 default 5, per-attempt delay 1–300s, retention 1–2,592,000s default 86,400,
    shutdown timeout 1–300s default 30)
  - Define `BackoffStrategy` and `OutboxStatus` enums
  - _Requirements: 1.1, 2.1, 6.1, 7.3, 10.6_

- [x] 5. Implement Configuration_Validator (Application)
  - [x] 5.1 Implement fail-fast aggregating validation
    - Validate presence of `Mode`; validate `Mode` is a recognized value and report the full
      recognized set when not; validate all numeric settings against permitted ranges; collect
      provider-specific required settings from `ProviderSettingDescriptor`
    - Aggregate every error in one pass (never stop at first); report each by setting name with
      its permitted set/range; return `IsValid` only when zero errors
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 2.6, 1.2, 1.3_

  - [x] 5.2 Write property test for complete missing-value reporting
    - **Property 2: Configuration validation reports every missing value**
    - **Validates: Requirements 2.2, 2.5**

  - [x] 5.3 Write property test for range/set soundness and completeness
    - **Property 3: Range and set validation is complete and sound**
    - **Validates: Requirements 2.3, 2.6**

  - [x] 5.4 Write unit tests for startup failure messages
    - Missing mode message (1.2); unrecognized mode listing the recognized set (1.3, 2.4)
    - _Requirements: 1.2, 1.3, 2.4_

- [x] 6. Implement Provider_Registry and resolution (Application)
  - [x] 6.1 Implement registry construction and `Resolve`
    - Build the registry from DI-registered `IMessagingProvider` instances keyed by `Mode`
    - Resolve: exactly one match returns the provider and is stable for instance lifetime; zero
      matches throws unresolved-mode error; more than one throws ambiguous-mapping error
    - Reject any attempt to change mode after startup
    - _Requirements: 1.4, 1.5, 1.6, 3.4, 3.5, 3.7_

  - [x] 6.2 Write property test for exact, fail-fast, stable resolution
    - **Property 1: Provider resolution is exact, fail-fast, and stable**
    - **Validates: Requirements 1.4, 1.5, 1.6, 3.4, 3.5, 3.7**

- [x] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement correlation id handling and the publish pipeline (Application)
  - [x] 8.1 Implement correlation id generation and envelope validation gate
    - Generate a globally unique correlation id (≤128 chars) when absent and attach it; reject
      empty/overlong correlation ids with an invalid-correlation-id error
    - _Requirements: 4.2, 4.3, 9.3_

  - [x] 8.2 Implement the publish pipeline that validates then delegates to the active provider
    - `PublishAsync` validates envelope invariants, returns a failed `PublishResult` and leaves
      the message undelivered on failure, otherwise hands off to the resolved provider's publisher
    - Preserve message id, correlation id, message type, timestamp, and all headers unchanged
    - _Requirements: 3.1, 3.2, 4.4_

  - [x] 8.3 Write property test for invalid-envelope rejection
    - **Property 4: Invalid envelopes are rejected and never delivered**
    - **Validates: Requirements 3.2, 4.3**

  - [x] 8.4 Write property test for correlation id generation
    - **Property 5: A correlation id is always generated when absent**
    - **Validates: Requirements 4.2, 9.3**

  - [x] 8.5 Write property test for field preservation through delivery
    - **Property 6: Publishing preserves envelope fields to delivery**
    - **Validates: Requirements 4.4**

- [x] 9. Implement the consume pipeline: contract binding and lifecycle (Application)
  - [x] 9.1 Implement contract binding and the consumer host state machine
    - Bind payload to a known contract by `MessageType`; reject unknown contracts with an
      unknown-contract error before invoking the handler
    - Implement `IMessageConsumerHost` state transitions Stopped → Starting → Running → Stopping → Stopped
    - Support additive versioning: a consumer bound to an earlier version binds and ignores
      unrecognized fields
    - _Requirements: 3.3, 4.5, 4.6, 4.7_

  - [x] 9.2 Write property test for unknown-contract rejection
    - **Property 8: Unknown contracts are rejected before the consumer**
    - **Validates: Requirements 4.6**

  - [x] 9.3 Write property test for additive-versioning backward compatibility
    - **Property 9: Additive versioning stays backward-compatible**
    - **Validates: Requirements 4.7**

  - [x] 9.4 Write property test for end-to-end correlation id propagation
    - **Property 7: Correlation id propagates unchanged end-to-end**
    - **Validates: Requirements 9.2, 9.4**

  - [x] 9.5 Write unit test for consumer host state sequence
    - Verify the Stopped → Starting → Running → Stopping → Stopped sequence
    - _Requirements: 3.3_

- [x] 10. Implement a FakeMessagingProvider for core isolation (test support)
  - Implement a `FakeMessagingProvider` that captures published/delivered envelopes and simulates
    confirm/fail outcomes, implementing `IMessagingProvider` + publisher + consumer host
  - Wire it so the publish and consume pipelines can be exercised without a real transport
  - _Requirements: 3.1, 3.3, 3.4_

- [x] 11. Implement failure classification and retry/backoff policy (Application)
  - [x] 11.1 Implement `IFailureClassifier` and backoff computation
    - Classify timeout/connection-failure/downstream-unavailable as Transient; validation
      failure/malformed payload as NonTransient
    - Compute per-attempt backoff for fixed/exponential/exponential-with-jitter clamped to 1–300s;
      bound attempts to the configured maximum; route to dead-letter on exhaustion and route
      NonTransient failures immediately with zero retries
    - Document delivery semantics as at-least-once; map the uniform policy onto the active
      provider's native retry/redelivery mechanism
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.7_

  - [x] 11.2 Write property test for bounded retry and dead-letter on exhaustion
    - **Property 12: Retry is bounded and routes to dead-letter on exhaustion**
    - **Validates: Requirements 6.2, 6.3, 6.5**

  - [x] 11.3 Write property test for backoff delay bounds
    - **Property 13: Backoff delay stays within bounds**
    - **Validates: Requirements 6.4**

- [x] 12. Implement idempotency key derivation and consumption policy (Application)
  - [x] 12.1 Implement `IIdempotencyKeyProvider` and the idempotent consume gate
    - Derive a deterministic key (same logical message → identical key); when enabled and the key
      is underivable, reject before side effects with a missing-key error
    - Before the handler runs, check the store; on a hit, expose a duplicate indication and
      acknowledge without re-executing side effects; expose the idempotency key to consumers
    - _Requirements: 6.8, 7.1, 7.2, 7.4, 7.6_

  - [x] 12.2 Write property test for deterministic idempotency keys
    - **Property 15: Idempotency keys are deterministic**
    - **Validates: Requirements 7.1**

  - [x] 12.3 Write property test for underivable-key rejection
    - **Property 18: An underivable idempotency key rejects before side effects**
    - **Validates: Requirements 7.6**

- [x] 13. Implement dead-letter routing (Application)
  - [x] 13.1 Implement dead-letter routing and the DeadLetterMessage payload
    - Route on retry exhaustion and on unbindable/poison payloads (no retry); build
      `DeadLetterMessage` preserving the original envelope byte-for-byte with correlation id,
      idempotency key, failure reason, total attempt count, and UTC dead-letter timestamp
    - Remove the message from the active pipeline on success; on sink failure, keep the message in
      the pipeline, log the routing failure, and do not acknowledge the source
    - Emit one dead-letter metric and one structured log entry (correlation id, reason, attempt count)
    - _Requirements: 6.6, 8.1, 8.2, 8.3, 8.4, 8.6, 6.6_

  - [x] 13.2 Write property test for dead-letter preservation of message and context
    - **Property 14: Dead-lettering preserves the message and its context**
    - **Validates: Requirements 6.6, 8.2**

  - [x] 13.3 Write property test for poison payloads skipping retry
    - **Property 19: Unbindable payloads are poison and skip retry**
    - **Validates: Requirements 8.3**

  - [x] 13.4 Write property test for routing-failure keeping message unacknowledged
    - **Property 20: Dead-letter routing failure keeps the message unacknowledged**
    - **Validates: Requirements 8.6**

  - [x] 13.5 Write unit tests for dead-letter emission and per-mode target
    - Exactly one metric and one structured log with correlation id, reason, attempt count (8.4);
      per-mode dead-letter target binding (8.5)
    - _Requirements: 8.4, 8.5_

- [x] 14. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 15. Implement observability (Application)
  - [x] 15.1 Implement structured logging, metrics, tracing, and redaction
    - Emit structured log entries (event type, correlation id, UTC timestamp) for publish, consume,
      retry, and dead-letter events; include correlation id on log entries and trace spans
    - Expose monotonic counters: published, consumed, retry, dead-letter counts; record linked
      publish/consume trace spans carried via headers through the correlation id
    - Replace secret/sensitive payload values with a fixed redaction marker before logging; make
      observability best-effort so emit failures never fail publish/consume
    - _Requirements: 9.1, 9.2, 9.4, 9.5, 9.6, 9.7, 9.8_

  - [x] 15.2 Write property test for monotonic counting
    - **Property 21: Metrics counters are monotonic and count events**
    - **Validates: Requirements 9.5**

  - [x] 15.3 Write property test for redaction
    - **Property 22: Redaction removes sensitive values before logging**
    - **Validates: Requirements 9.7**

  - [x] 15.4 Write property test for non-blocking observability
    - **Property 23: Observability failures never fail the message operation**
    - **Validates: Requirements 9.8**

  - [x] 15.5 Write unit test for structured log fields
    - Verify fields present for publish/consume/retry/dead-letter via a mock logger
    - _Requirements: 9.1_

- [x] 16. Implement EF persistence: outbox and idempotency stores (Infrastructure)
  - [x] 16.1 Implement the EF DbContext and the two tables
    - Map `messaging_outbox` (id, correlation_id, message_type, contract_version, headers jsonb,
      payload bytea, status, attempt_count, created_at_utc, dispatched_at_utc, last_error) with an
      index on (status, created_at_utc); map `messaging_idempotency`
      (idempotency_key PK, correlation_id, processed_at_utc, expires_at_utc)
    - Keep EF types out of `Domain`; use parameterized access; use ON CONFLICT DO NOTHING for
      idempotency inserts
    - _Requirements: 5.1, 7.1, 7.3_

  - [x] 16.2 Implement `IIdempotencyStore` over EF
    - `IsProcessedAsync` single indexed lookup; `RecordProcessedAsync` persists key with retention
      window; recording failure surfaces so the message is not acknowledged and the store is unchanged
    - Add a background sweep removing rows past `expires_at_utc`
    - _Requirements: 7.2, 7.3, 7.4, 7.5_

  - [x] 16.3 Write property test (Testcontainer) for exactly-once side effects
    - **Property 16: Idempotent consumption executes side effects exactly once**
    - **Validates: Requirements 7.2, 7.4**

  - [x] 16.4 Write property test (Testcontainer) for failed-record never acknowledging
    - **Property 17: A failed idempotency record never acknowledges the message**
    - **Validates: Requirements 7.5**

  - [x] 16.5 Write unit test for retention boundary
    - Key within window still blocks; key past expires_at_utc no longer blocks
    - _Requirements: 7.3_

- [x] 17. Implement the Outbox_Dispatcher (Application + Infrastructure)
  - [x] 17.1 Implement transactional outbox recording and the dispatcher loop
    - Record the outgoing envelope in the outbox within the same transaction as the originating
      state change (both commit or neither)
    - Dispatch pending entries through `IMessagePublisher`; mark Dispatched only after the
      publisher confirms and exclude from future attempts; on failure keep Pending, increment
      attempts, leave the state change unchanged; at 10 attempts mark Failed and exclude
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [x] 17.2 Write property test (Testcontainer) for at-least-once delivery
    - **Property 10: Outbox delivers every committed entry at least once**
    - **Validates: Requirements 5.2, 5.3, 5.6**

  - [x] 17.3 Write property test (Testcontainer) for retained and bounded failures
    - **Property 11: Outbox failures are retained and bounded**
    - **Validates: Requirements 5.4, 5.5**

- [x] 18. Implement lifecycle, health, and readiness logic (Application)
  - [x] 18.1 Implement liveness/readiness logic and graceful shutdown
    - Provide liveness (healthy while process runs) and readiness logic calling
      `CheckTransportAsync` (ready when reachable, not-ready naming the unavailable dependency,
      also reporting the active mode)
    - On shutdown signal, `StopAsync` stops accepting new messages and drains in-flight within the
      configured timeout (default 30s, 1–300s); on timeout, terminate the host and record the
      non-negative incomplete in-flight count
    - _Requirements: 1.7, 10.3, 10.4, 10.5, 10.6, 10.7_

  - [x] 18.2 Write property test for readiness reflecting transport reachability
    - **Property 24: Readiness reflects transport reachability**
    - **Validates: Requirements 10.3, 10.4**

  - [x] 18.3 Write property test for shutdown incomplete-count recording
    - **Property 25: Shutdown records the exact incomplete in-flight count**
    - **Validates: Requirements 10.7**

  - [x] 18.4 Write unit test for stop-accepting-new-messages on shutdown
    - Verify the host stops accepting new messages once StopAsync begins
    - _Requirements: 10.5_

- [x] 19. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [~] 20. Implement adapter package shells behind the ports (Infrastructure)
  - [x] 20.1 Implement the MassTransit adapter shell (Pub/Sub + Queue)
    - Implement `IMessagingProvider` wrapping a configured MassTransit bus; publisher delegates to
      publish/send endpoints; consumer host wraps bus start/stop; declare `RequiredSettings` and
      implement `CheckTransportAsync`; insert the core idempotency/classification/dead-letter/
      correlation steps into or ahead of the consumer pipeline
    - _Requirements: 3.4, 2.5, 10.3_

  - [x] 20.2 Implement the Kafka-native adapter shell (Streaming)
    - Implement `IMessagingProvider` over Confluent.Kafka producer/consumer with manual offset
      commit after the core pipeline completes (at-least-once); declare `RequiredSettings` and
      implement `CheckTransportAsync`
    - _Requirements: 3.4, 2.5, 6.7, 10.3_

  - [x] 20.3 Implement the Batch adapter shell (Batch Processing)
    - Implement `IMessagingProvider` with a scheduled/triggered pull → chunk → process → checkpoint
      consumer host and a publisher to the batch destination; declare `RequiredSettings` and
      implement `CheckTransportAsync`
    - _Requirements: 3.4, 2.5, 10.3_

- [x] 21. Implement use-what-you-need DI registration and final wiring (Infrastructure + Application)
  - [x] 21.1 Implement provider registration extension methods
    - Add `AddXyzMessagingProvider(...)` extension methods per adapter package that register their
      `IMessagingProvider` without changing Domain/Application, making each provider selectable
      through configuration
    - _Requirements: 3.6_

  - [x] 21.2 Wire startup validation, resolution, and the publish/consume pipeline together
    - On startup: validate configuration, fail-fast on errors, resolve the active provider, bind it
      into the publish/consume pipeline with outbox, retry/classification, idempotency, dead-letter,
      and observability wrapped around it; expose the active mode on readiness
    - Add publish-only, consume-only, and both registration surfaces so unused machinery is not started
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.7, 2.1, 3.6_

  - [x] 21.3 Write architecture test for inward dependency rule
    - Assert `Domain` and `Application` reference no concrete provider assembly
    - _Requirements: 3.1, 3.8_

- [x] 22. Integration tests (Testcontainers)
  - [x] 22.1 Write PostgreSQL outbox atomicity integration test
    - Commit persists both state and outbox rows; forced failure persists neither
    - _Requirements: 5.1_

  - [x] 22.2 Write RabbitMQ (MassTransit) round-trip and dead-letter integration test
    - Publish/consume round-trip; dead-letter routing within 5s of final failure; message leaves
      the active pipeline
    - _Requirements: 8.1_

  - [x] 22.3 Write Kafka at-least-once integration test
    - Produce/consume with manual offset commit demonstrating at-least-once and correlation propagation
    - _Requirements: 6.7, 9.4_

  - [x] 22.4 Write OpenTelemetry span-linking integration test
    - In-memory exporter asserts publish and consume spans linked via correlation id
    - _Requirements: 9.6_

  - [x] 22.5 Write endpoint and timing integration tests
    - Liveness healthy within 1s; readiness ready within 1s when reachable and not-ready naming the
      dependency otherwise; readiness exposes the active mode; graceful drain completes in-flight
      within the shutdown timeout
    - _Requirements: 1.7, 10.1, 10.2, 10.3, 10.4, 10.6_

- [x] 23. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All tasks are required and will be implemented, including every unit, property, and integration
  test; the property tests are the primary correctness guarantee here.
- Each task references specific requirements (granular sub-requirements) for traceability.
- Checkpoints ensure incremental validation at natural boundaries.
- Property-based tests use FsCheck.Xunit, run a minimum of 100 generated cases, and are tagged
  `Feature: messaging-core, Property {number}`; one property-based test per property.
- Properties that depend on persistence (10, 11, 16, 17) run against a PostgreSQL Testcontainer;
  the one-engine rule forbids an in-memory DB substitute.
- Provider internals (MassTransit/Kafka/Batch mechanics) are later specs; adapter tasks here build
  only the shells that plug into the ports.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 1, "tasks": ["2.2", "5.1", "6.1", "16.1"] },
    { "id": 2, "tasks": ["5.2", "5.3", "5.4", "6.2", "8.1", "16.2"] },
    { "id": 3, "tasks": ["8.2", "9.1", "10.1", "16.3", "16.4", "16.5"] },
    { "id": 4, "tasks": ["8.3", "8.4", "8.5", "9.2", "9.3", "9.4", "9.5", "11.1", "12.1", "15.1", "17.1"] },
    { "id": 5, "tasks": ["11.2", "11.3", "12.2", "12.3", "13.1", "15.2", "15.3", "15.4", "15.5", "17.2", "17.3", "18.1"] },
    { "id": 6, "tasks": ["13.2", "13.3", "13.4", "13.5", "18.2", "18.3", "18.4", "20.1", "20.2", "20.3"] },
    { "id": 7, "tasks": ["21.1", "21.2"] },
    { "id": 8, "tasks": ["21.3", "22.1", "22.2", "22.3", "22.4", "22.5"] }
  ]
}
```
