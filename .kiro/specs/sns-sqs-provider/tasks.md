# Implementation Plan: SNS/SQS Provider

## Overview

This plan completes the `Messaging.MassTransit` adapter shell for the `AmazonSqs` transport. It supplies
production implementations of the two open seam ports (`IMassTransitBusAdapter`,
`IMassTransitConsumeProcessor`), a topology manager, and the DI wiring that stands them up. SNS and SQS are
reached through `MassTransit.AmazonSQS`; no transport-neutral core contract changes.

The implementation language is **C#** (net8.0), taken from the design and the existing codebase, so no
language selection is needed. Work happens on branch `feat/sns-sqs-provider`.

The plan is ordered to retire risk first. Task 1 runs the design's risk spike (R1 to R5) against LocalStack
`4.14.0` before any production code is written, and task 2 is a gate: if a spike contradicts the design, the
design is revised before implementation continues. After that the plan moves from pure logic (settings,
naming, planner, wire format), to topology, to the consume path, to the bus adapter, to DI, local
environment, and the LocalStack integration scenarios, so nothing is left unwired.

All new production code lives in `src/Messaging.MassTransit/AmazonSqs/` unless a task says otherwise. All
tests live in `tests/Messaging.MassTransit.Tests/` (xUnit, `FsCheck.Xunit`, existing
`MassTransitTestSupport`). Property tests use `[Property(MaxTest = 100, QuietOnSuccess = true)]` and carry
a comment `// Feature: sns-sqs-provider, Property {n}: {text}`. Integration tests use
`Testcontainers.LocalStack` with the pinned image `localstack/localstack:4.14.0`, no auth token, dummy
credentials, and per-class random name prefixes. Every
container-backed test class (LocalStack or RabbitMQ collection)
also carries `[Trait("Category", "Integration")]`. Checkpoints
run only `dotnet test tests/Messaging.MassTransit.Tests
--filter "Category!=Integration"`, which starts no containers.

## Tasks

- [x] 1. Risk spike (R1 to R5) against LocalStack 4.14.0
  - [x] 1.1 Add the AWS transport packages
    - Add `MassTransit.AmazonSQS` 8.5.11, `AWSSDK.SQS` 4.0.100.14, and
      `AWSSDK.SimpleNotificationService` 4.0.100.15 to `src/Messaging.MassTransit/Messaging.MassTransit.csproj`
    - Add `Testcontainers.LocalStack` 3.10.0, `AWSSDK.SQS` 4.0.100.14, and
      `AWSSDK.SimpleNotificationService` 4.0.100.15 to the `Messaging.MassTransit.Tests` project
    - Confirm the solution restores and builds
    - _Requirements: 1.1, 18.1_

  - [x] 1.2 Add `LocalStackImageTag` and `LocalStackContainerFixture`
    - Create the pure `LocalStackImageTag.Resolve(string? override)` in the test support code: default
      `4.14.0`, accept `major.minor.patch`, reject `latest`, bare major tags, and partial versions
    - Create `LocalStackContainerFixture` mirroring `RabbitMqContainerFixture`: `LocalStackBuilder` with image
      `localstack/localstack:{tag}` from env `LOCALSTACK_IMAGE_TAG`, `SERVICES=sns,sqs`, `WithCleanUp(true)`,
      startup wrapped in a 60-second token that throws a `TimeoutException` naming the container
    - Never set `LOCALSTACK_AUTH_TOKEN`; expose the endpoint URL and dummy credentials (`test` / `test`)
    - Add the `[Collection("localstack-integration")]` collection definition
    - If the module misbehaves with the pinned image, fall back to the generic `ContainerBuilder` with a
      `/_localstack/health` wait (design, Integration tests)
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.17_

  - [x] 1.3 Spike R5: AWS SDK v4 and a MassTransit round trip on LocalStack 4.14.0
    - Add a throwaway-then-kept characterization test `AmazonSqsSpikeR5Tests`: with raw `IAmazonSQS` and
      `IAmazonSimpleNotificationService` clients (dummy credentials, `ServiceURL`), create a topic, a queue,
      a subscription, and a policy, publish, receive, and delete
    - Then start a minimal MassTransit Amazon bus against the same endpoint and send then consume one message
    - Record whether SDK v4 works against LocalStack `4.14.0`; if not, record the failing call and the
      fallback decision (design R5)
    - _Requirements: 1.1, 18.1, 18.17_

  - [x] 1.4 Spike R1 and R2: MassTransit against pre-provisioned resources
    - Add `AmazonSqsSpikeR1R2Tests`: pre-provision a queue with the planned attributes (redrive, visibility,
      retention) and a topic and subscription through raw SDK clients, start the bus with
      `ConfigureConsumeTopology = false`, and assert start succeeds and attributes are unchanged
    - Repeat against a pre-existing queue with different attributes and record whether MassTransit's
      idempotent create is rejected
    - List all queues after bus start and record whether `{queue}_error` or `{queue}_skipped` are created
      eagerly, including with a non-default dead-letter suffix
    - Record the IAM actions MassTransit needs at start in verify-only deployments
    - _Requirements: 1.1, 5.1, 5.4, 5.5, 5.6_

  - [x] 1.5 Spike R3: rethrow error pipe leaves the message undeleted
    - Add `AmazonSqsSpikeR3Tests`: configure the receive endpoint with `UseMessageRetry(r => r.None())` and
      `ConfigureError(x => x.UseFilter(new RethrowErrorTransportFilter()))`, with a consumer that throws
    - Assert the message is not deleted, is redelivered after the visibility timeout, and lands in the
      dead-letter queue after `maxReceiveCount` receives; assert a consumer that returns normally deletes it
    - Record the exact endpoint API shape that works on MassTransit 8.5.11
    - _Requirements: 1.9, 10.8, 10.9, 10.10_

  - [x] 1.6 Spike R4: raw and SNS-wrapped bodies reach the consumer with attributes
    - Add `AmazonSqsSpikeR4Tests`: with `RawMessageDelivery` true and false, publish an `EnvelopeMessage`
      with message attributes through an SNS topic subscribed to a queue and consume it
    - Assert the body deserializes in both modes and that `x-correlation-id`, `traceparent`, and `h.*`
      attributes are visible as transport headers in both modes
    - Record the header provider behavior for wrapped delivery
    - _Requirements: 6.1, 6.2, 7.1, 15.5_

  - [x] 1.7 Record spike findings in the design
    - Update the Risks table in `.kiro/specs/sns-sqs-provider/design.md` with the outcome of R1 to R5 and any
      design change they force (queue attribute alignment, extra `_error` / `_skipped` queues in the plan,
      endpoint API shape, SDK fallback, IAM notes)
    - If a spike invalidates a design decision, stop and ask the user before continuing
    - _Requirements: 1.1, 5.1, 10.8, 10.9_

- [x] 2. Checkpoint - spike gate
  - Ensure all tests pass, ask the user if questions arise.
  - Do not start task 3 until the R1 to R5 findings are recorded and any design change is agreed.

- [x] 3. Settings, exceptions, and startup validation
  - [x] 3.1 Add the domain exceptions
    - Create `PublishNotPermittedException`, `MessageTooLargeException(actual, limit)`,
      `ResourceNameTooLongException(subject, limit)`, `TopologyResourceMissingException(IReadOnlyList<string>)`,
      `TopologyOperationFailedException(operation, resource)`, `TopologyConflictException(resource, existingType)`,
      and `MessageDeliveryRejectedException`
    - No message may include credentials, connection values, or payload bytes
    - _Requirements: 4.5, 5.7, 5.8, 6.8, 8.7, 11.4, 12.6, 14.3_

  - [x] 3.2 Implement `AmazonSqsProviderSettings` and shell settings changes
    - Create `AmazonSqsProviderSettings` with every setting, default, and bound from the design table, a
      lenient `FromProviderSettings(IReadOnlyDictionary<string,string>)`, and `Describe()` returning
      `ProviderSettingDescriptor`s (secret-bearing settings declare `PermittedValuesOrRange: null`)
    - Edit `MassTransitProviderSettings`: stop requiring a connection string for `AmazonSqs`, carry optional
      `AmazonSqsProviderSettings`, return its descriptors from `ToRequiredSettings()`, and set `Host` to the
      `ServiceUrl` host or the region
    - Keep the RabbitMQ behavior unchanged
    - _Requirements: 5.4, 5.5, 6.8, 12.7, 13.10, 14.4_

  - [x] 3.3 Implement `AmazonSqsStartupValidator`
    - Create the static, pure `Validate` returning one aggregated, de-duplicated error list: the core
      `ConfigurationValidator` pass, then non-numeric and non-boolean text, blank required values, region
      format, `ServiceUrl` absolute http(s) with `http` only for loopback or container-network hosts, partial
      credentials, FIFO name length with prefix or suffix, and `MaxReceiveCount` below `Retry.MaxAttempts`
    - Add `ValidateOrThrow(configuration)` for the provider factory
    - Errors name settings only and never echo secret values
    - _Requirements: 10.11, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.8, 13.9, 14.7_

  - [x] 3.4 Write property test for startup validation
    - **Property 13: Startup validation reports each invalid setting exactly once**
    - Generate settings dictionaries mixing valid, absent, out-of-range, non-numeric, and malformed values,
      credential combinations, `ServiceUrl` variants, and `MaxReceiveCount` against `Retry.MaxAttempts`
    - **Validates: Requirements 10.11, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.9, 14.7**

  - [x] 3.5 Write unit tests for settings and validation
    - Each required setting missing; each bounded setting at and outside both bounds; redrive versus retry
      conflict; endpoint override rules; partial credentials; two simultaneous failures reported separately;
      descriptors present; secret values excluded from every error
    - _Requirements: 13.2, 13.3, 13.4, 13.5, 13.7, 13.8, 13.10, 17.4_

- [x] 4. Naming convention and topology planning
  - [x] 4.1 Implement `SnsSqsNamingConvention`
    - `TopicName`, `QueueName`, and `DeadLetterQueueName` as designed: MassTransit-style formatting,
      sanitizing to `[A-Za-z0-9_-]`, prefixes, `.fifo` handling, limits of 256 (topic) and 80 (queue), no
      truncation, `ResourceNameTooLongException` on overflow, dead-letter suffix inserted before `.fifo`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [x] 4.2 Write property test for topic and queue names
    - **Property 1: Topic and queue name derivation**
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7**

  - [x] 4.3 Write property test for dead-letter queue names
    - **Property 2: Dead-letter queue name derivation**
    - **Validates: Requirements 4.8**

  - [x] 4.4 Write unit tests for naming
    - Prefix handling, invalid character replacement, length-limit failure, `.fifo` suffix, and parity with
      `AmazonSqsMessageNameFormatter.GetMessageName` for CLR types whose `FullName` equals the type string
    - _Requirements: 4.1, 4.3, 4.4, 4.5, 4.6, 17.2_

  - [x] 4.5 Implement `SnsSqsTopologyPlanner` and the plan records
    - Create `TopologyPlan`, `TopicSpec`, `QueueSpec`, `SubscriptionSpec`
    - Publisher role: empty plan. Consumer or both: the queue and its dead-letter queue, plus (Publish-
      Subscribe) one topic per distinct `ContractRegistration.MessageType` and one subscription per
      (topic, queue) pair. Include any auxiliary queues the spike (task 1.7) found MassTransit creates
    - _Requirements: 2.3, 5.1, 5.4, 5.5, 11.1, 11.3, 11.6_

  - [x] 4.6 Write unit tests for the planner
    - Role scoping (publisher creates no queue), one queue subscribed to N topics, Message Queue mode has no
      topics, FIFO flags and KMS key carried into specs
    - _Requirements: 11.1, 11.3, 11.5, 11.6, 14.8_

- [x] 5. Wire format, headers, size guard, FIFO identifiers
  - [x] 5.1 Implement `SnsSqsHeaderMapper`, `CorrelationGuid`, and `FifoMessageIds`
    - Header mapper: reserved attributes `x-correlation-id`, `traceparent`, `x-message-type`; eligible user
      headers ordered by ordinal key, first 7 become `h.{key}`, the rest stay in the body; produce the
      `AllowTransportHeader` allow-list
    - `CorrelationGuid.From`: parse a Guid, else the first 16 bytes of the SHA-256
    - `FifoMessageIds`: group id from correlation id or message type with allowed characters and a 128
      character cap; deduplication id is the message id in `D` format
    - _Requirements: 6.6, 6.7, 7.1, 8.2, 8.3_

  - [x] 5.2 Implement `SnsSqsWireFormat` and `SnsSqsMessageSizeGuard`
    - `ToWire(envelope, traceHeaders)` returns `SnsSqsWireMessage`; `FromWire(message, consumeHeaders)`
      rebuilds headers from `ConsumeContext.Headers` plus body headers (design R4) and ignores reserved transport keys; reserved-key collisions stay in the
      body
    - `Check(envelope, attributes, limit)` measures `System.Text.Json` size plus attributes plus the fixed
      2,048-byte allowance and throws `MessageTooLargeException(actual, limit)`
    - _Requirements: 6.3, 6.4, 6.5, 6.7, 6.8, 6.9, 7.2_

  - [x] 5.3 Write property test for the wire round trip
    - **Property 3: Envelope wire round trip**
    - Extend the generator from `EnvelopeMessageFidelityPropertyTests` with empty and non-ASCII payloads, 0 to
      64 headers, and reserved-key collisions
    - **Validates: Requirements 6.3, 6.4, 6.5, 6.7, 6.9, 7.2**

  - [x] 5.4 Write property test for the attribute budget
    - **Property 4: Message attribute budget and header partition**
    - **Validates: Requirements 6.6, 6.7**

  - [x] 5.5 Write property test for correlation mapping
    - **Property 5: Correlation id mapping**
    - **Validates: Requirements 7.1**

  - [x] 5.6 Write property test for the size guard
    - **Property 6: Message size guard**
    - **Validates: Requirements 6.8**

  - [x] 5.7 Write property test for FIFO identifiers
    - **Property 7: FIFO group and deduplication identifiers**
    - **Validates: Requirements 8.2, 8.3**

  - [x] 5.8 Write unit tests for the wire layer
    - Empty and maximum-size header collections, more than 10 headers overflow, header key collision with a
      reserved key, size exactly at and one byte over the limit, non-ASCII payload
    - _Requirements: 6.6, 6.7, 6.8, 17.3, 17.8_

- [x] 6. Checkpoint - pure logic
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Topology management
  - [x] 7.1 Implement `QueuePolicyDocument`
    - Pure `Merge(existingPolicyJson, queueArn, topicArns)`: keep unrelated statements, exactly one `Allow
      sqs:SendMessage` statement per topic ARN with principal `Service: sns.amazonaws.com` and an `ArnEquals`
      `aws:SourceArn` condition, deterministic `Sid`, no wildcard principal, idempotent
    - _Requirements: 5.3, 14.9_

  - [x] 7.2 Write property test for the queue policy merge
    - **Property 8: Queue policy merge is scoped and idempotent**
    - **Validates: Requirements 5.3, 14.9**

  - [x] 7.3 Implement `ISnsSqsTopologyClient` and `AwsSnsSqsTopologyClient`
    - Narrow port: `GetQueueUrlAsync`, `GetQueueAttributesAsync`, `CreateQueueAsync`,
      `SetQueueAttributesAsync`, `GetTopicArnAsync`, `CreateTopicAsync`, `ListSubscriptionsByTopicAsync`,
      `SubscribeAsync`, `PingAsync`
    - The AWS implementation wraps `IAmazonSQS` and `IAmazonSimpleNotificationService` and is the only code
      touching the SDK control plane; authorization and service errors are wrapped by the manager, not here
    - _Requirements: 1.1, 5.1, 16.1_

  - [x] 7.4 Implement `SnsSqsTopologyManager`
    - `ApplyAsync(plan, autoCreate)`: create missing resources in dependency order and leave existing ones
      unchanged; dead-letter queue first with maximum retention; queue with redrive, visibility, retention,
      FIFO, and KMS; topic with FIFO and KMS; subscription only when absent, with `RawMessageDelivery`;
      merge the queue policy
    - Verify-only: no mutating call; collect all missing resources into one
      `TopologyResourceMissingException`
    - Wrap SDK failures in `TopologyOperationFailedException`; throw `TopologyConflictException` on a FIFO
      mismatch; `EnsureTopicAsync` with a per-process cache
    - Emit a structured log per outcome (resource kind, name, outcome)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 8.7, 14.8, 14.10, 15.4_

  - [x] 7.5 Add `FakeSnsSqsTopologyClient`
    - In-memory implementation of the port with a mutation counter, preset state, and failure injection
    - _Requirements: 17.1, 17.6_

  - [x] 7.6 Write property test for topology reconciliation
    - **Property 9: Topology reconciliation is complete, non-destructive, and idempotent**
    - **Validates: Requirements 5.1, 5.2, 5.9, 11.6**

  - [x] 7.7 Write property test for verify-only mode
    - **Property 10: Verify-only never mutates and names every missing resource**
    - **Validates: Requirements 5.6, 5.7**

  - [x] 7.8 Write unit tests for the topology manager
    - Attribute mapping for redrive, visibility, retention, and KMS; no-op when present; authorization
      failure names the operation and resource without credentials; type conflict; policy principal scoping
    - _Requirements: 5.4, 5.5, 5.8, 5.10, 8.7, 14.8, 17.6_

  - [x] 7.9 Tag container-backed test classes
    - Add `[Trait("Category", "Integration")]` to every class in
      the LocalStack and RabbitMQ collections:
      `AmazonSqsSpikeR1R2Tests`, `AmazonSqsSpikeR3Tests`,
      `AmazonSqsSpikeR4Tests`, `AmazonSqsSpikeR5Tests`, and
      `MassTransitRabbitMqIntegrationTests`
    - Confirm with `dotnet test --list-tests` that
      `--filter "Category!=Integration"` lists none of them
    - _Requirements: 18.1_

- [x] 8. Checkpoint - topology
  - Run once, no containers:
    `dotnet test tests/Messaging.MassTransit.Tests --filter "Category!=Integration"`
  - Report pass/fail counts. On failure, stop and report the
    first failing test; do not attempt a fix.
  - Ask the user if questions arise.

- [x] 9. Consume path and dead-letter sink
  - [x] 9.1 Implement `InFlightTrackingConsumeProcessor`
    - Decorator over `CoreConsumeProcessor` using `InFlightTracker.TryBeginProcessing()`; throw
      `MessageDeliveryRejectedException` when not accepting, without calling the inner processor; always call
      `CompleteProcessing()` in `finally`
    - _Requirements: 12.4, 12.6_

  - [x] 9.2 Implement `SqsEnvelopeConsumer`
    - `IConsumer<EnvelopeMessage>`: rebuild the envelope through `FromWire`, parse `traceparent` from
      `ConsumeContext.Headers` (design R4), start a `messaging.receive` activity with that parent on the `Messaging.Core`
      source, then call the consume processor
    - _Requirements: 1.3, 6.1, 6.2, 6.10, 15.5_

  - [x] 9.3 Implement `IDeadLetterQueueSender` and `SqsDeadLetterSink`
    - Send the original `EnvelopeMessage` to the dead-letter queue with `x-dead-letter-reason` (truncated to
      1,024 characters, no payload), `x-attempt-count`, `x-dead-lettered-at`, `x-idempotency-key`, and
      `x-correlation-id`; in FIFO mode use the same group id and a deduplication id `{messageId}:dlq`
    - A send failure throws so `DeadLetterRouter` returns `RetainedUnacknowledged`
    - _Requirements: 10.6, 10.7, 10.8_

  - [x] 9.4 Write property test for in-flight tracking
    - **Property 12: In-flight tracking and rejection**
    - **Validates: Requirements 12.4, 12.6**

  - [x] 9.5 Write property test for dead-letter preservation
    - **Property 11: Dead-letter sink preserves the original message**
    - **Validates: Requirements 10.6**

  - [x] 9.6 Write unit tests for the consume wiring
    - Unknown or unbindable contract dead-letters without handler invocation; sink failure retains; missing
      correlation id dead-letters; success deletes; shutdown rejection; trace link to the publish span;
      processing failure after retries classified as designed
    - _Requirements: 1.9, 6.11, 6.12, 7.4, 10.8, 12.6, 15.5, 17.5_

- [x] 10. Metrics and the bus adapter
  - [x] 10.1 Implement `AmazonSqsMetrics`
    - Counters for published, consumed, dead-lettered, and failed, labeled by topic or queue name only, never
      by correlation id or payload; emission failures are suppressed
    - Queue-depth gauges sampled outside the readiness path
    - _Requirements: 15.6, 15.7, 16.6_

  - [x] 10.2 Implement `AmazonSqsBusAdapter` construction, start, and stop
    - `Create(...)` builds the bus with `Bus.Factory.CreateUsingAmazonSqs`: explicit keys or default chain,
      `ServiceUrl` with `AuthenticationRegion` for both SDK configs, `AllowTransportHeader` allow-list
    - Consumer or both role: exactly one receive endpoint with `ConfigureConsumeTopology = false`, polling
      settings, plan-aligned `QueueAttributes`, `UseMessageRetry(r => r.None())`, and the rethrow error pipe
      in the shape confirmed by spike R3; publisher role: no receive endpoint
    - `StartBusAsync` runs plan then topology then bus start and logs `bus-state=started`; a failure leaves
      `_started=false`. `StopBusAsync` calls `StopAccepting`, stops the bus with the clamped timeout, and
      records `ShutdownResult` with the in-flight count
    - An `internal` constructor takes a ready bus for unit tests
    - _Requirements: 1.2, 1.4, 5.1, 11.1, 11.3, 11.5, 12.1, 12.2, 12.3, 12.4, 12.5, 12.7, 14.1, 14.2, 14.6, 15.3_

  - [x] 10.3 Implement publish, dead-letter send, and reachability on the adapter
    - `PublishAsync`: role gate (`PublishNotPermittedException` for consumer role), `OnPublish` trace headers,
      `ToWire`, size guard, FIFO ids, topic ensure, then send to `topic:{name}` (Publish-Subscribe) or
      `queue:{name}` (Message Queue); record the publish event and metric
    - `SendToDeadLetterQueueAsync` sends to `queue:{dlq}` in any role
    - `IsTransportReachableAsync`: false when not started; otherwise a 1-second bounded
      `PingAsync`; any error, authorization failure, or timeout returns false and never throws
    - _Requirements: 1.5, 1.6, 1.7, 1.8, 2.1, 2.7, 7.1, 8.2, 11.2, 11.4, 15.1, 16.1, 16.2, 16.3, 16.4, 16.5_

  - [x] 10.4 Write unit tests for the adapter with a fake bus
    - Publish-Subscribe sends once to `topic:{name}`; Message Queue sends to `queue:{name}`; role gating for
      publisher, consumer, and both, including dead-letter send from the consumer role; start order topology
      then bus; start failure leaves `_started=false`; stop with and without timeout; polling settings reach
      the endpoint
    - _Requirements: 1.5, 1.6, 11.1, 11.2, 11.3, 11.4, 11.5, 12.1, 12.3, 12.4, 12.5, 12.7, 17.7_

  - [x] 10.5 Write unit tests for reachability
    - Reachable, unreachable, throwing, authorization error, and timeout using an injected slow ping; the
      result names the region or `ServiceUrl` host only
    - _Requirements: 1.7, 1.8, 16.1, 16.2, 16.3, 16.4, 16.5, 17.7_

  - [x] 10.6 Write unit tests for observability
    - Publish and consume events through `ObservabilityContext`, bus-state and topology logs, trace link via
      `ActivityListener`, metrics via `MeterListener` with no correlation id label, a throwing sink is
      suppressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7_

- [x] 11. Hosted service and DI wiring
  - [x] 11.1 Implement `AmazonSqsBusHostedService`
    - Consumer or both role: drive `IMessageConsumerHost.StartAsync/StopAsync` so the shell state machine
      tracks Running and Stopped. Publisher role: call `StartBusAsync/StopBusAsync` on the adapter directly
    - _Requirements: 12.1, 12.2, 12.3, 12.5_

  - [x] 11.2 Extend `AddMassTransitMessagingProvider` for the `AmazonSqs` transport
    - Run the unsupported-mode guard first with a message naming SNS/SQS and the two supported modes
    - Dispatch on `settings.Transport`: the RabbitMQ path stays unchanged; `RegisterAmazonSqsSeams` uses
      `TryAdd` for the bus adapter, `ISnsSqsTopologyClient`, the topology manager, the in-flight-tracking
      processor (consumer or both), `IDeadLetterSink` (`TryAddSingleton` so a host double wins),
      `AmazonSqsMetrics`, and the hosted service
    - Call `AmazonSqsStartupValidator.ValidateOrThrow` in the provider factory when the configuration selects
      `AmazonSqs`; keep `IMessagingProvider` a plain `AddSingleton`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 11.1, 11.3, 11.5, 13.1_

  - [x] 11.3 Write unit tests for DI wiring
    - One registration resolves and returns the same instance; unsupported mode registers nothing and names
      SNS/SQS; zero or two providers for a mode fail in the registry; role-scoped registrations; RabbitMQ path
      unaffected; descriptors present
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 11.1, 11.3, 11.5, 13.10_

  - [x] 11.4 Write property test for diagnostics
    - **Property 14: No secret in diagnostics**
    - Generate access key, secret key, session token, and a `ServiceUrl` with userinfo; capture validation
      errors, exception messages, logs, health dependency names, and metric labels from validation, bus
      start and stop, topology, probe, and publish failure
    - **Validates: Requirements 13.8, 14.3, 14.4, 14.5**

- [x] 12. Checkpoint - adapter and wiring
  - Run once, no containers:
    `dotnet test tests/Messaging.MassTransit.Tests --filter "Category!=Integration"`
  - Report pass/fail counts. On failure, stop and report the
    first failing test; do not attempt a fix.
  - Ask the user if questions arise.

- [x] 13. Local development environment
  - [x] 13.1 Add the LocalStack service to `docker-compose.yml`
    - Service `localstack`: `image: localstack/localstack:${LOCALSTACK_IMAGE_TAG:-4.14.0}`, port 4566,
      `SERVICES: sns,sqs`, a healthcheck on `awslocal sqs list-queues` and `awslocal sns list-topics`, no
      auth token, no real credentials
    - Leave the `kafka` service block untouched
    - _Requirements: 19.1, 19.2, 19.3, 19.6_

  - [x] 13.2 Write compose consistency tests
    - `DockerComposeLocalStackFileTests` read `docker-compose.yml` and assert the image default equals
      `LocalStackImageTag.DefaultTag`, the healthcheck commands, no `LOCALSTACK_AUTH_TOKEN`, access key, or
      `latest`, and that the `kafka` block is unchanged
    - _Requirements: 19.1, 19.2, 19.3, 19.4, 19.6_

  - [x] 13.3 Update the README
    - Document starting the environment with `docker compose up -d --wait`, the settings that select
      `AmazonSqs` (`MassTransit:Transport`, `AmazonSqs:Region`, `AmazonSqs:EndpointName`,
      `AmazonSqs:ServiceUrl`, dummy keys), the pinned tag and `LOCALSTACK_IMAGE_TAG`, and why the tag is
      pinned (newer images need `LOCALSTACK_AUTH_TOKEN`; 4.x does not)
    - Include the IAM actions by role and the R1 outcome (verify-only deployments need no create permissions; temporary bus queue to be verified, design R2)
    - _Requirements: 19.3, 19.5_

  - [x] 13.4 Write property test for image tag resolution
    - **Property 15: LocalStack image tag resolution**
    - **Validates: Requirements 18.4**

- [x] 14. LocalStack integration tests
  - [x] 14.1 Add the integration harness
    - Helper that creates a random `t{8 hex}-` prefix per test class, builds real bus adapters with dummy
      credentials against the fixture endpoint, registers capturing handlers, and waits with a 30-second
      timeout; give each class isolated topic and queue names
    - Every integration test class carries
      `[Trait("Category", "Integration")]` and joins the
      `localstack-integration` collection
    - _Requirements: 18.1, 18.16, 18.17_

  - [x] 14.2 Test fan-out
    - One publish, three endpoints: each handler gets one copy within 30 seconds equal to the published
      envelope; a failing endpoint leaves the other two unaffected; publish to a topic with no subscription
      completes
    - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.7, 7.3, 18.5_

  - [x] 14.3 Test competing consumers
    - Two instances on one queue, 20 messages, each handled exactly once
    - _Requirements: 2.4, 18.6_

  - [x] 14.4 Test late subscription
    - A new endpoint started after a first publish receives only later messages
    - _Requirements: 2.5, 18.7_

  - [x] 14.5 Test retry exhaustion and dead-lettering
    - Exactly `MaxAttempts` attempts, dead-letter within 5 seconds of the final attempt, no redelivery,
      other queues unaffected, not in flight; visibility timeout shorter than total retry time proves renewal;
      a failing sink retains the message
    - _Requirements: 10.1, 10.3, 10.5, 10.7, 10.8, 10.9, 18.8_

  - [x] 14.6 Test SQS redrive
    - Receive a message `MaxReceiveCount` times without deleting and observe it in the dead-letter queue
    - _Requirements: 10.10, 18.9_

  - [x] 14.7 Test topology creation
    - Topics, queues, subscriptions, queue policy without a wildcard principal, dead-letter queue, and
      redrive are created; a second start leaves them unchanged; verify-only fails naming a missing resource
    - _Requirements: 5.1, 5.3, 5.4, 5.6, 5.7, 5.9, 18.10_

  - [x] 14.8 Test raw and wrapped delivery
    - Run the round trip with `RawMessageDelivery` true and false
    - _Requirements: 6.1, 6.2, 18.11_

  - [x] 14.9 Test FIFO delivery
    - Ordering within one group across a FIFO topic and queue, and deduplication of a repeated id
    - _Requirements: 8.4, 8.5, 8.6, 18.12_

  - [x] 14.10 Test idempotent consumption
    - The same message id delivered twice to one queue runs the handler once
    - _Requirements: 9.4, 9.8, 18.13_

  - [x] 14.11 Test graceful shutdown
    - Stop with in-flight messages: completed messages are deleted, unfinished messages are redelivered
      after restart
    - _Requirements: 12.3, 12.4, 18.14_

  - [x] 14.12 Test readiness
    - Use a dedicated container: reachable while it runs, not reachable naming the Amazon SNS/SQS service
      after it stops
    - _Requirements: 16.1, 16.2, 18.15_

  - [x] 14.13 Test header and size boundaries
    - More than 10 headers round trip intact; a payload just below the limit is delivered and just above is
      rejected before send; use the result to confirm or adjust the 2,048-byte allowance (design R6)
    - _Requirements: 6.6, 6.7, 6.8, 18.5_

- [x] 15. Final checkpoint - full suite
  - Step 1, no containers: run the checkpoint command
    `dotnet test tests/Messaging.MassTransit.Tests --filter "Category!=Integration"`
  - Step 2, integration only:
    `dotnet test tests/Messaging.MassTransit.Tests --filter "Category=Integration"`
    with `--blame-hang-timeout 2m`, so a hung test fails and is named
  - Run each step once and report pass/fail counts. On failure,
    stop and report the first failing test; do not attempt a fix.
  - Run `docker ps` before and after step 2 and remove only
    containers that run started.
  - Ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional (test sub-tasks) and can be skipped for a faster MVP. The property
  and integration tests carry most of this feature's correctness guarantees and are strongly advised.
- Task 1 is the risk spike and is not optional. Its tests are kept as characterization tests, and task 1.7
  feeds the findings back into the design before implementation starts.
- Each task references the requirements it implements; each property test names the design property.
- Property tests use `FsCheck.Xunit` with at least 100 cases and a `Feature: sns-sqs-provider, Property {n}`
  comment. Properties 1 to 15 each have one sub-task: 1 (4.2), 2 (4.3), 3 (5.3), 4 (5.4), 5 (5.5), 6 (5.6),
  7 (5.7), 8 (7.2), 9 (7.6), 10 (7.7), 11 (9.5), 12 (9.4), 13 (3.4), 14 (11.4), 15 (13.4).
- Integration tests run against LocalStack `4.14.0` through Testcontainers, with no auth token, dummy
  credentials, and isolated names per test class.
- Checkpoints give incremental validation after the spike, pure logic, topology, adapter, and full suite.
- Checkpoints 8 and 12 run only non-integration tests; only task 15 runs the container tests.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3", "1.4", "1.5", "1.6"] },
    { "id": 3, "tasks": ["1.7"] },
    { "id": 4, "tasks": ["3.1", "3.2", "5.1", "7.1", "10.1", "13.1", "7.9"] },
    { "id": 5, "tasks": ["4.1", "5.2", "5.4", "5.5", "5.7", "7.2", "9.1", "13.2", "13.3", "13.4"] },
    { "id": 6, "tasks": ["3.3", "4.2", "4.3", "4.4", "4.5", "5.3", "5.6", "5.8", "7.3", "9.4"] },
    { "id": 7, "tasks": ["3.4", "3.5", "4.6", "7.4", "9.2", "9.3"] },
    { "id": 8, "tasks": ["7.5", "9.5", "9.6", "10.2"] },
    { "id": 9, "tasks": ["7.6", "7.7", "7.8", "10.3"] },
    { "id": 10, "tasks": ["10.4", "10.5", "10.6", "11.1"] },
    { "id": 11, "tasks": ["11.2"] },
    { "id": 12, "tasks": ["11.3", "11.4", "14.1"] },
    { "id": 13, "tasks": ["14.2", "14.3", "14.4", "14.5", "14.6", "14.7", "14.8", "14.9", "14.10", "14.11", "14.12", "14.13"] }
  ]
}
```
