# Implementation Plan: Kafka Provider

## Overview

This plan completes the existing `Messaging.Kafka` adapter shell (`src/Messaging.Kafka/Streaming/`) by
supplying production implementations for its four open seam ports (`IKafkaProducer`, `IKafkaConsumer`,
`IStreamMessageProcessor`, `IKafkaTransportProbe`), a hosted consume loop, and the DI wiring that stands
them up. Kafka is reached through the **native Confluent.Kafka** client (pinned `2.15.1`, unchanged), not
the MassTransit Kafka Rider. Every transport-neutral core contract is consumed unchanged, and
`KafkaConsumerHost.ConsumeOnceAsync` needs no change (the processor's return-vs-throw is the commit gate).

The implementation language is **C#** (net8.0), taken from the design and the existing codebase, so no
language selection is needed. Tasks build incrementally: package, data models and settings first, then the
promoted wire format and the pure config factory, then the producer/consumer adapters, the stream message
processor, the transport probe and the hosted service, then DI wiring and local compose, and finally the
containerized integration tests re-pointed at the production adapters, so nothing is orphaned.

All production types live in `src/Messaging.Kafka/Streaming/` (Infrastructure) and depend inward on core
ports only (Clean Architecture). `KafkaTopicAdmin` stays test-only. Property tests use `FsCheck.Xunit`
(`[Property]`, min 100 iterations); integration tests use `Testcontainers.Kafka` against
`confluentinc/cp-kafka:7.6.1`. The `CoreConsumeProcessor` and `AddMassTransitMessagingProvider` code in
`src/Messaging.MassTransit/` is the reference for the processor and DI composition.

## Tasks

- [x] 1. Add the hosting package, security data models, exceptions, and settings validation
  - [x] 1.1 Pin `Microsoft.Extensions.Hosting.Abstractions` in the adapter project
    - Add `<PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" Version="8.0.x" />` (exact
      pinned 8.0.x patch, aligned with the other `Microsoft.Extensions.*` abstractions) to
      `src/Messaging.Kafka/Messaging.Kafka.csproj`; needed only for `IHostedService`
    - Leave `Confluent.Kafka` pinned at exactly `2.15.1`; confirm the project restores and builds
    - _Requirements: 1.9, 8.1_

  - [x] 1.2 Add `KafkaSecuritySettings` and its selector enums
    - Create `src/Messaging.Kafka/Streaming/KafkaSecuritySettings.cs` (record) with `SecurityProtocol`
      (default `Plaintext`), `SaslMechanism` (default `None`), `SaslUsername`, `SaslPassword`,
      `TlsCaLocation`; one public type per file, so add `KafkaSecurityProtocol.cs`
      (`Plaintext`, `Ssl`, `SaslPlaintext`, `SaslSsl`) and `KafkaSaslMechanism.cs`
      (`None`, `Plain`, `ScramSha256`, `ScramSha512`)
    - Hold injected-only secret material separately from the non-secret `KafkaProviderSettings`; the
      record must not expose credentials through `ToString()` (override to redact / omit)
    - _Requirements: 12.1, 12.2, 12.3, 12.9, 12.10_

  - [x] 1.3 Add `RetainOffsetException` and `MissingRequiredHeaderException`
    - Create `src/Messaging.Kafka/Streaming/RetainOffsetException.cs`, the Kafka analogue of
      `RetainForRedeliveryException`, thrown only when the dead-letter sink reports `RetainedUnacknowledged`
    - Create `src/Messaging.Kafka/Streaming/MissingRequiredHeaderException.cs` carrying the offending
      header name (never the payload); the core `FailureClassifier` must treat it as non-transient, so
      check how the classifier decides and align if needed without changing core contracts
    - _Requirements: 6.3, 6.4, 9.6_

  - [x] 1.4 Extend `KafkaProviderSettings.Describe()` with security descriptors and SASL cross-field validation
    - In `src/Messaging.Kafka/Streaming/KafkaProviderSettings.cs`, add setting-name constants and optional
      descriptors for `Kafka:SecurityProtocol` (permitted `Plaintext|Ssl|SaslPlaintext|SaslSsl`),
      `Kafka:SaslMechanism` (permitted `None|Plain|ScramSha256|ScramSha512`), `Kafka:SaslUsername`,
      `Kafka:SaslPassword`, `Kafka:TlsCaLocation`
    - Add the cross-field rule: a SASL protocol (`SaslPlaintext`/`SaslSsl`) requires a mechanism other than
      `None` plus a username and password; surface one error per absent setting, by setting name only, with
      every credential value excluded, so the existing `ConfigurationValidator` aggregates them in its
      single pass (read `ConfigurationValidator` first to match how descriptor errors are produced)
    - Keep the existing three required descriptors and `Validate()` behavior unchanged
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

  - [x] 1.5 Write property test for startup validation aggregation
    - **Property 7: Startup validation aggregates all failures and excludes secret values**
    - Generate arbitrary sets of missing / whitespace / out-of-set / SASL-without-credential settings; assert
      exactly one error per offending setting (by name, with permitted set for enums) and that no error text
      contains any configured SASL credential value in whole or in fragment
    - Add to `tests/Messaging.Kafka.Tests/`; `[Property]`, min 100 iterations; tag comment
      "Feature: kafka-provider, Property 7"
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 12.4, 12.5, 12.6**

  - [x] 1.6 Write unit tests for settings validation
    - Extend `KafkaProviderSettingsTests.cs`: default security is Plaintext and valid (Req 12.1); out-of-set
      protocol/mechanism reported by name with permitted set (Req 3.4, 12.4); SASL protocol with mechanism
      `None` or missing username/password reports each separately without values (Req 12.5, 12.6); all valid
      settings yield zero errors (Req 3.7)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 3.4, 3.7, 12.1, 12.4, 12.5, 12.6_

- [x] 2. Promote `KafkaWireFormat` into the Infrastructure package
  - [x] 2.1 Move and harden `KafkaWireFormat`
    - Create `src/Messaging.Kafka/Streaming/KafkaWireFormat.cs`, sourced from the proven test-local copy in
      `tests/Messaging.Kafka.Tests/KafkaIntegrationTestSupport.cs`: `ToMessage(MessageEnvelope)` (payload as
      record value; `x-message-id` "D", `x-correlation-id`, `x-message-type`, `x-created-at-utc` "O",
      `x-contract-version`, plus one header per user header) and `ToEnvelope(...)` (inverse, ordinal user
      header map, `Value ?? Array.Empty<byte>()`), plus the ordinal `ReservedHeaders` set
    - Replace the bare `InvalidOperationException` with `MissingRequiredHeaderException` naming the header
      when a required header is absent or unreadable; never return a partial envelope
    - Do not delete the test-local copy yet (task 12.1 re-points the tests); give the production type its
      own namespace so the test project still compiles
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.5_

  - [x] 2.2 Write property test for wire-format round-trip identity
    - **Property 1: Wire-format round-trip identity**
    - Generate arbitrary envelopes (random id, correlation 1–128 chars, type, UTC timestamp with sub-tick
      precision, version ≥ 1, 0–64 user headers, arbitrary payload incl. empty); assert
      `ToEnvelope(ToMessage(e))` preserves every field, header count/keys/values, full-precision timestamp
      and every payload byte
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 1"
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 5.2, 9.5, 10.5**

  - [x] 2.3 Write property test for missing-header rejection
    - **Property 2: Missing required header is rejected, never partially reconstructed**
    - Generate arbitrary envelopes, map to a record, remove exactly one required header; assert
      `ToEnvelope` throws `MissingRequiredHeaderException` naming that header and yields no envelope
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 2"
    - **Validates: Requirements 9.6**

- [x] 3. Implement `KafkaClientConfigFactory` (pure config and security)
  - [x] 3.1 Create the factory
    - Create `src/Messaging.Kafka/Streaming/KafkaClientConfigFactory.cs`, constructed from
      `KafkaProviderSettings` + `KafkaSecuritySettings`, with no I/O and no logging
    - `CreateProducerConfig()`: bootstrap servers, `Acks.All`, `EnableIdempotence = true`, security
    - `CreateConsumerConfig()`: bootstrap servers, group id, `EnableAutoCommit = false`,
      `AutoOffsetReset = Earliest`, security; set no client-side redelivery or retry policy beyond defaults
    - `CreateAdminClientConfig()`: bootstrap servers + security
    - Private `ApplySecurity`: set protocol (default `Plaintext`); for SASL set mechanism, username,
      password; for TLS set `SslCaLocation` only when provided, else leave default trust
    - _Requirements: 1.2, 1.3, 1.4, 4.1, 7.6, 12.1, 12.5, 12.7, 12.8, 12.9_

  - [x] 3.2 Write unit tests for the config factory
    - Producer has `Acks.All` + idempotence; consumer has auto-commit off + earliest reset; default security
      is Plaintext with no SASL/TLS keys set; SASL maps mechanism/username/password; TLS maps CA location
      when present and leaves it unset when absent; admin config carries bootstrap servers + security
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 1.2, 1.3, 1.4, 4.1, 12.1, 12.5, 12.7, 12.8_

- [x] 4. Promote the producer and consumer adapters
  - [x] 4.1 Move and harden `ConfluentKafkaProducer : IKafkaProducer, IDisposable`
    - Create `src/Messaging.Kafka/Streaming/ConfluentKafkaProducer.cs` over `IProducer<Null, byte[]>` built
      from `KafkaClientConfigFactory.CreateProducerConfig()`; ctor `(factory, settings, ILogger)` with null guards
    - `ProduceAsync(topic, envelope, ct)`: `KafkaWireFormat.ToMessage` → produce, await the delivery report;
      a produce without all-ISR acknowledgement throws an exception naming the topic and excluding payload
      bytes and credentials (`KafkaMessagePublisher` already maps it to a failed `PublishResult`)
    - `Dispose()`: `Flush` then dispose; log by correlation id and topic only
    - _Requirements: 1.2, 1.5, 1.8, 9.1, 10.3, 12.10_

  - [x] 4.2 Move and harden `ConfluentKafkaConsumer : IKafkaConsumer, IDisposable`
    - Create `src/Messaging.Kafka/Streaming/ConfluentKafkaConsumer.cs` over `IConsumer<Ignore, byte[]>` built
      from `KafkaClientConfigFactory.CreateConsumerConfig()`
    - `Subscribe(topic)`; `ConsumeAsync(ct)` polls with the poll timeout (1–300s, default 5s; check
      `ConfigurationBounds`), returns `null` on empty poll / partition EOF, otherwise builds
      `KafkaConsumeResult` and retains the underlying `ConsumeResult` in an in-flight
      `ConcurrentDictionary` keyed by the result
    - `CommitAsync(result, ct)`: remove the retained `ConsumeResult` and `Commit` it (offset + 1); a commit
      the broker does not acknowledge throws an error identifying partition and offset, leaving the offset
      unchanged; `Close()` leaves the group
    - Source from the test-local `ConfluentKafkaConsumer`; no payload or secret in logs or exceptions
    - _Requirements: 1.3, 1.4, 1.8, 4.1, 4.4, 4.5, 4.6, 4.7, 8.3, 8.7, 9.1, 10.3, 12.10_

- [x] 5. Checkpoint - wire format, config, and client adapters
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement `KafkaStreamMessageProcessor` (the consume seam)
  - [x] 6.1 Implement `KafkaStreamMessageProcessor : IStreamMessageProcessor`
    - Create `src/Messaging.Kafka/Streaming/KafkaStreamMessageProcessor.cs`, mirroring
      `src/Messaging.MassTransit/Consuming/CoreConsumeProcessor.cs`, with constructor-injected, null-guarded
      `ObservabilityContext`, `RetryExecutor`, `ConsumePipeline` (used via `DeliverAsync`), `DeadLetterRouter`,
      and `RetryPolicyConfig`
    - Algorithm: `ObservabilityContext.OnConsume(correlationId, headers)` (best-effort), then
      `RetryExecutor.ExecuteAsync(envelope, policy, ct => consumePipeline.DeliverAsync(envelope, ct), idempotencyKey: null, ct)`
    - Map the terminal outcome: `Succeeded` → return (host commits); dead-letter outcomes → if
      `RetainedUnacknowledged` throw `RetainOffsetException`, else return (routed and acknowledged)
    - Unknown contract, missing header, and missing idempotency key flow through the core classifier as
      non-transient → dead-letter with the handler never invoked; configure no client-side redelivery
    - _Requirements: 1.6, 1.8, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 9.5, 9.6, 10.2, 10.6_

  - [x] 6.2 Write property test for commit-versus-retain mapping
    - **Property 3: Commit iff terminal success or routed-and-acknowledged**
    - Generate arbitrary terminal `RetryResult`s (success / non-transient+routed / exhausted+routed /
      retained); assert `ProcessAsync` returns iff committable and throws `RetainOffsetException` iff
      retained, using a fake retry-executor/pipeline seam (no broker)
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 3"
    - **Validates: Requirements 4.2, 4.3, 6.1, 6.2, 6.3, 6.5**

  - [x] 6.3 Write property test for bounded attempt count by failure kind
    - **Property 4: Bounded attempt count by failure kind**
    - Generate `MaxAttempts` in [1, 100] and a failure kind; always-failing processing yields exactly 1
      attempt (non-transient) or exactly `MaxAttempts` (transient), never more; inject the delay function so
      there is no wall-clock wait and mock the sink
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 4"
    - **Validates: Requirements 7.1, 7.6**

  - [x] 6.4 Write property test for dead-letter context preservation
    - **Property 5: Dead-letter preserves context**
    - Generate arbitrary failing envelopes + attempt counts, capture the `DeadLetterMessage`; assert message
      id, correlation id (char-for-char), payload bytes (length + every byte) and exact total attempt count
      (1 when routed without retry)
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 5"
    - **Validates: Requirements 7.5**

  - [x] 6.5 Write unit tests for processor flows
    - Delegation to `RetryExecutor` over `DeliverAsync` (Req 1.6, 5.1); idempotent duplicate → handler not
      invoked, returns normally (Req 5.4); missing idempotency key → dead-letter, handler not invoked
      (Req 5.5); unknown/unbindable contract → dead-letter, handler not invoked, reason names the contract
      (Req 5.7); missing required header → dead-letter without a partial envelope (Req 9.6); correlation id
      reaches the handler unchanged (Req 5.2); throwing observability sink does not fail the operation
      (Req 10.6)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 1.6, 5.1, 5.2, 5.4, 5.5, 5.7, 9.6, 10.6_

- [x] 7. Implement `ConfluentKafkaTransportProbe` (the reachability seam)
  - [x] 7.1 Implement the bounded probe
    - Create `src/Messaging.Kafka/Streaming/ConfluentKafkaTransportProbe.cs` implementing
      `IKafkaTransportProbe` over an admin client built from `KafkaClientConfigFactory.CreateAdminClientConfig()`
    - `ProbeAsync`: `GetMetadata(≤ 1s)` guarded by `Task.WhenAny(check, Task.Delay(1s, ct))`; ≥ 1 broker →
      `TransportHealth(true, null)`; no broker, thrown error, or timeout →
      `TransportHealth(false, "Kafka (bootstrap servers)")`; never propagate; the dependency name never
      carries SASL material
    - _Requirements: 1.7, 1.8, 10.3, 11.1, 11.2, 11.3, 11.4, 11.5, 12.10_

  - [x] 7.2 Write unit tests for probe mapping
    - Reachable → `TransportHealth(true, null)`; empty metadata → false with the fixed name; throwing check →
      false, error not propagated; injected slow check → false on timeout, timeout not propagated
    - Use an injectable metadata-check seam so no broker is needed; name as `Method_Scenario_ExpectedResult`
    - _Requirements: 11.2, 11.3, 11.4, 11.5_

- [x] 8. Implement `KafkaConsumerHostedService` (the hosting gap)
  - [x] 8.1 Implement the hosted consume loop
    - Create `src/Messaging.Kafka/Streaming/KafkaConsumerHostedService.cs` implementing `IHostedService`,
      depending on `KafkaConsumerHost`, `LifecycleConfig`, and `ILogger` only
    - `StartAsync`: `host.StartAsync(ct)`, then launch the background loop; while the host is Running call
      `ConsumeOnceAsync` and begin the next iteration within 1s; when not Running, do not iterate
    - On a thrown exception (e.g. `RetainOffsetException`): log the correlation id only (never payload), wait
      a bounded backoff (1–60s, default 5s) and continue so the same uncommitted record is re-polled; a
      client-reported rebalance is non-fatal
    - `StopAsync`: stop starting new iterations, let the in-flight iteration finish process-then-commit,
      and stop the host within `LifecycleConfig.ShutdownTimeoutSeconds` (1–300s, default 30); if the drain
      exceeds it, stop without committing the uncompleted record and log the correlation id (no payload)
      noting the drain exceeded the timeout
    - Inject delay and clock seams so the backoff is testable without wall-clock waits
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

  - [x] 8.2 Write unit tests for the hosted service and host behavior
    - Running → calls `ConsumeOnceAsync`; not Running → no call (Req 8.1, 8.2); thrown
      `RetainOffsetException` is caught, logged by correlation id only, and the loop continues and re-polls
      (Req 8.5, 8.6); drain timeout stops without committing and logs the timeout (Req 8.3, 8.4)
    - Re-assert shell behavior against fake seams in `KafkaConsumerHostTests.cs`: empty poll returns `false`
      with no commit, and a processor throw skips the commit (Req 4.3, 4.4)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 4.3, 4.4, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [x] 9. Checkpoint - seams and hosted service
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Extend DI wiring to register the production seams
  - [x] 10.1 Extend `AddKafkaMessagingProvider`
    - In `src/Messaging.Kafka/Streaming/KafkaMessagingProviderServiceCollectionExtensions.cs`, accept the
      injected `KafkaSecuritySettings` (default Plaintext) and `TryAddSingleton` `KafkaClientConfigFactory`,
      `IKafkaProducer → ConfluentKafkaProducer`, `IKafkaConsumer → ConfluentKafkaConsumer`,
      `IKafkaTransportProbe → ConfluentKafkaTransportProbe`
    - `TryAddSingleton<IStreamMessageProcessor>` building `KafkaStreamMessageProcessor` from the registered
      `ObservabilityContext`, `RetryExecutor`, `DeadLetterRouter`, `RetryPolicyConfig` and a
      `ConsumePipeline` composed from parts (`ConsumerHostStateMachine`, `IContractBinder`, `IHandlerInvoker`,
      optional `IdempotencyGate` via `GetService`), exactly like MassTransit's `BuildConsumePipeline`
      (`src/Messaging.MassTransit/MassTransitMessagingProviderServiceCollectionExtensions.cs`)
    - `TryAddSingleton<KafkaConsumerHostedService>` and
      `AddSingleton<IHostedService>(sp => sp.GetRequiredService<KafkaConsumerHostedService>())`
    - Keep the `IMessagingProvider` singleton accumulation as a plain `AddSingleton`; a pre-registered seam
      (fake or caller-supplied) must win via `TryAdd`
    - _Requirements: 1.8, 2.1, 2.2, 2.5, 5.1, 8.1, 12.1_

  - [x] 10.2 Write unit tests for DI composition
    - Extend `KafkaMessagingProviderServiceCollectionExtensionsTests.cs`: the four seams are `TryAdd`ed and
      resolve to the production types; `KafkaConsumerHostedService` is registered as `IHostedService`;
      `ConsumePipeline` composes with and without an `IdempotencyGate`; a pre-registered fake seam wins
      (Req 2.1, 5.1, 8.1)
    - Registry/startup: one Streaming registration resolves and repeated resolution returns the same
      instance; zero or duplicate registration throws `ProviderResolutionException`; a non-Streaming mode
      resolving to Kafka fails fast with no partial registration (Req 2.2, 2.3, 2.4, 2.5)
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 5.1, 8.1_

  - [x] 10.3 Write property test for no secret or payload in diagnostics
    - **Property 6: Redaction totality — no secret or payload in diagnostics**
    - Generate arbitrary SASL credential values and payloads; using a capturing logger and the probe,
      processor and settings-error paths, assert neither appears as a substring in any produce-failure
      report, consume/retry/dead-letter log entry, probe dependency name, or validation error; assert a
      sensitive-keyed value equals `PayloadRedactor.RedactionMarker`
    - `[Property]`, min 100 iterations; tag comment "Feature: kafka-provider, Property 6"
    - **Validates: Requirements 1.5, 10.3, 10.4, 11.3, 11.4, 11.5, 12.6, 12.10**

- [x] 11. Add local docker-compose Kafka infrastructure
  - [x] 11.1 Create `docker-compose.yml` at the repo root
    - Add a single Kafka service on `confluentinc/cp-kafka:7.6.1` (identical to `KafkaContainerFixture`), a
      PLAINTEXT listener reachable from the host, a healthcheck so it is ready within 60s, and no SASL
      credentials, TLS keys, or other secrets; pin the tag and never use `latest`
    - Follow `infrastructure.md`: compose runs the broker, the service runs on the host and connects via
      `Kafka:BootstrapServers`
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

  - [x] 11.2 Write file-content tests for the compose file
    - Assert the image is exactly `confluentinc/cp-kafka:7.6.1` with no `latest`, a PLAINTEXT listener is
      declared, and no `SASL`, `PASSWORD`, `KEYSTORE`, or private-key entries exist
    - Name as `Method_Scenario_ExpectedResult`
    - _Requirements: 14.2, 14.3, 14.4_

- [x] 12. Re-point and complete containerized integration verification
  - [x] 12.1 Re-point the test-local adapters at the promoted production types
    - In `tests/Messaging.Kafka.Tests/KafkaIntegrationTestSupport.cs`, remove the test-local
      `KafkaWireFormat`, `ConfluentKafkaProducer`, and `ConfluentKafkaConsumer`, and reference the production
      types (built through `KafkaClientConfigFactory` with default Plaintext security); keep `KafkaTopicAdmin`
    - Keep `KafkaContainerFixture` (pinned `confluentinc/cp-kafka:7.6.1`, readiness bound, reaped after the
      run), the `[Collection("kafka-integration")]` sharing, and the `KafkaTestSupport.cs` fakes
    - Add a helper that builds the production `KafkaStreamMessageProcessor` over test collaborators
      (capturing dead-letter sink, no-op observability)
    - _Requirements: 13.1, 13.2, 13.3, 13.5_

  - [x] 12.2 Verify at-least-once redelivery over the real broker
    - Update `KafkaAtLeastOnceIntegrationTests.cs`: a record whose processing fails before its offset is
      committed is redelivered within 60s to a consumer in the same `ConsumerGroupId`, using the production
      producer, consumer, and processor
    - _Requirements: 4.5, 6.4, 13.6_

  - [x] 12.3 Verify correlation-id propagation over the real broker
    - Add `KafkaCorrelationIntegrationTests.cs`: produce an envelope and consume it, asserting the correlation
      id (and the other envelope fields) are equal char-for-char / byte-for-byte end to end
    - _Requirements: 5.2, 9.4, 9.5, 13.7_

  - [x] 12.4 Verify the success-path commit is not redelivered
    - Add `KafkaSuccessPathIntegrationTests.cs`: a successfully processed record has its offset committed
      and is not redelivered to a newly started consumer in the same group, observed over ≥ 30s after that
      consumer reaches its partition assignment
    - _Requirements: 4.2, 4.6, 6.1, 13.8_

- [x] 13. Final checkpoint - full suite
  - Run the build and all non-integration tests (exclude the `kafka-integration` collection, for example with
    `dotnet test --filter "Collection!=kafka-integration"`) and ensure they pass; ask the user if questions arise.
  - Integration tests (`kafka-integration` collection, tasks 12.x) need a running Docker daemon for
    Testcontainers. Docker may be unavailable in some environments, and running that collection without
    Docker hangs, so run it only where Docker is available and state clearly if it was skipped.

## Notes

- Tasks marked with `*` are optional (test sub-tasks) and can be skipped for a faster MVP, though the
  property and integration tests carry this feature's correctness guarantees and are strongly advised.
- Each task references the specific requirements it implements; property tests also name their property.
- Property-based tests use `FsCheck.Xunit` (`[Property]`, min 100 iterations) and each carries a
  "Feature: kafka-provider, Property {n}" tag comment; all seven design properties are covered.
- `Confluent.Kafka` stays pinned at `2.15.1` and the broker image at `confluentinc/cp-kafka:7.6.1`; neither
  version changes in this feature.
- `KafkaConsumerHost.ConsumeOnceAsync` is not modified; the processor's return-vs-throw drives commit-vs-retain.
- `KafkaTopicAdmin` stays test-only; production never creates topics.
- Checkpoints provide incremental validation at each seam boundary before wiring proceeds.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["1.4", "2.1", "3.1", "8.1"] },
    { "id": 2, "tasks": ["1.5", "1.6", "2.2", "2.3", "3.2", "4.1", "4.2", "6.1", "7.1", "11.1"] },
    { "id": 3, "tasks": ["6.2", "6.3", "6.4", "6.5", "7.2", "8.2", "10.1", "11.2", "12.1"] },
    { "id": 4, "tasks": ["10.2", "10.3", "12.2", "12.3", "12.4"] }
  ]
}
```
