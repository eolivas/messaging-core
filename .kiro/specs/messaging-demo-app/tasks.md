# Implementation Plan: Messaging Demo App

## Overview

Implementation language: C# (.NET 8) for the Backend_For_Frontend, the three mock microservices and the backend tests. TypeScript (React, Vite, Vitest, fast-check) for the Frontend. The design already uses these languages, so no language question applies.

All demo files live under `demo/`. No task may modify `src/`, `tests/`, `MessagingCore.sln`, `global.json` or the root `docker-compose.yml`. The demo has its own `demo/MessagingDemo.sln` and references the library projects by path.

The plan follows the design's recommended choices for the pending Open Points:
- One BFF process with an isolated `IHost` and DI container per microservice (D1).
- Server-Sent Events with a sequenced, replayable flow log (D2).
- In-memory idempotency store and dead-letter sink (D3).
- Retry applied by the demo: 3 attempts, `Fixed` backoff, 1 s delay; shutdown timeout 10 s (F10).

Build order: scaffold and verify the no-network-I/O assumption, then contracts, then the Application core, then the microservices, then the Infrastructure and provider compositions, then the API, then local infrastructure, then the Frontend, then integration and architecture tests.

## Tasks

- [ ] 1. Scaffold the demo solution and verify the client-construction assumption
  - [x] 1.1 Create the demo solution and project skeleton under `demo/`
    - Create `demo/MessagingDemo.sln`, `demo/Directory.Build.props` (net8.0, nullable, implicit usings, warnings as errors) and the projects `Demo.Contracts`, `Demo.SalesLeads`, `Demo.Customer`, `Demo.Communications`, `Demo.Application`, `Demo.Infrastructure`, `Demo.Api` under `demo/src/`.
    - Create the test projects `Demo.Application.Tests`, `Demo.Services.Tests`, `Demo.Infrastructure.Tests`, `Demo.Api.Tests`, `Demo.IntegrationTests`, `Demo.Architecture.Tests` under `demo/tests/`, using xUnit 2.5.3, FsCheck.Xunit 3.4.0 and Testcontainers 3.10.0 (same versions as the library tests).
    - Add project references per the design's layering diagram. Reference library projects (`Messaging`, `Messaging.Abstractions`, `Messaging.Contracts`, `Messaging.MassTransit`, `Messaging.Kafka`) by path only; copy no library source.
    - Run `git status` to confirm that only files under `demo/` were added.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 1.2 Spike: verify that building the MassTransit and Confluent clients performs no network I/O
    - In `Demo.Infrastructure.Tests`, add tests that build (without starting) the RabbitMQ bus via `RabbitMqBusAdapter.Create`, the Amazon SQS bus via `AmazonSqsBusAdapter.Create`, and the Kafka producer and consumer config via the library factories. Point each at a local `TcpListener` and assert that no connection is accepted and construction finishes quickly.
    - Record the outcome in `demo/docs/spike-no-network-io.md`. If any build performs network I/O, state that the startup self-check (design D4) is limited to validators and descriptor resolution, and note which provider it applies to.
    - This finding gates the scope of the `ProviderCompositionSelfCheck` in task 8.1.
    - _Requirements: 11.1, 11.2_

  - [x] 1.3 Implement `Demo.Contracts`
    - Add `QuoteAcceptedEvent` (with `MessageType = "quote-accepted"`), `MicroserviceId`, `FlowStepKind`, `FlowStep` and `IFlowStepSink` as defined in the design.
    - _Requirements: 5.8, 6.5, 9.12_

  - [ ]* 1.4 Write property test for the Quote Accepted contract round trip
    - **Property 10: Quote accepted contract round trip**
    - Serialize as the publisher does, bind through `JsonContractBinder`, and assert field equality.
    - **Validates: Requirements 14.5**

- [x] 2. Checkpoint - Scaffold builds and tests pass
  - Run only `dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

- [x] 3. Application core (CQRS, ports, pure logic)
  - [x] 3.1 Define `Result<T>`, `AppError`, CQRS interfaces, ports and DTOs
    - Add `Result<T>` with error categories (`Validation`, `Conflict`, `NotFound`, `Unavailable`, `Timeout`) and field errors.
    - Add `ICommandHandler`, `IQueryHandler`, `IProviderCatalog`, `IMicroserviceCluster`, `ISalesLeadsGateway`, `IFlowEventBroadcaster`, `IDependencyProbe`, and all request and response DTOs from the design's data-model section.
    - Add domain exceptions `SessionConflictException` and `MessageValidationException`.
    - _Requirements: 12.1, 12.2_

  - [x] 3.2 Implement `SecretMasker` and `FailureReasonSanitizer`
    - `SecretMasker.Mask` returns the fixed placeholder for any input.
    - `FailureReasonSanitizer` truncates to 200 characters and strips URI user-info, connection strings and token-like values.
    - _Requirements: 3.6, 11.6, 12.4_

  - [ ]* 3.3 Write property test for secret masking
    - **Property 1: Secret masking is constant and leaks nothing**
    - Include empty, 1-character and 256-character inputs, and a serialized `ProviderDefaultsDto` check.
    - **Validates: Requirements 3.6, 14.6**

  - [ ]* 3.4 Write property test for sanitizer output
    - **Property 27: Sanitized output leaks no secrets** (sanitizer part; the log adapter and the 500 body are covered in tasks 6.10 and 8.4)
    - **Validates: Requirements 11.6, 12.4**

  - [x] 3.5 Implement `SessionFlowLog`
    - Thread-safe, bounded to 500 entries, assigns monotonically increasing `sequence`, derives counters (Published, CustomerCreated, EmailSent), supports `Clear()`, newest-first ordering by UTC timestamp with sequence tie-break, and event completion tracking per correlation id.
    - Support replay of all retained steps after the last clear in sequence order.
    - _Requirements: 5.9, 5.10, 9.6, 9.8, 9.9, 9.10, 9.12_

  - [ ]* 3.6 Write property test for counters
    - **Property 20: Counters are monotonic and reset on clear**
    - **Validates: Requirements 5.9, 5.10, 9.9**

  - [ ]* 3.7 Write property test for feed order and bound
    - **Property 21: Activity feed order and bound** (server side)
    - **Validates: Requirements 9.6, 9.12**

  - [ ]* 3.8 Write property test for event completion
    - **Property 22: Event completion marking** (server side)
    - **Validates: Requirements 9.8**

  - [ ]* 3.9 Write property test for replay
    - **Property 24: Replay reproduces the feed** (server side)
    - **Validates: Requirements 9.10**

  - [x] 3.10 Implement `SessionStateMachine` and `SessionCoordinator`
    - Guarded transitions Idle, Starting, Active, Stopping under a `SemaphoreSlim`.
    - A start while not Idle throws `SessionConflictException` naming the active provider. Trigger while not Active is rejected.
    - Hold per-consumer failure modes (default `Off`) and `LastError`.
    - _Requirements: 4.4, 4.7, 4.10, 5.6, 12.5, 12.6_

  - [ ]* 3.11 Write property test for the session state machine
    - **Property 6: Session state machine**
    - **Validates: Requirements 4.4, 4.7, 4.10, 5.6, 12.6**

  - [x] 3.12 Implement provider query handlers and validators
    - `ListProvidersQueryHandler` (order: Amazon SNS/SQS, RabbitMQ, Kafka, Azure Service Bus; unsupported reason 1 to 200 characters).
    - `GetProviderDefaultsQueryHandler` (masks every secret entry via `SecretMasker`; unknown provider returns NotFound; Azure returns a validation error with the reason).
    - `ValidateProviderSettingsQueryHandler` (rejects overrides to secret settings; reports library validator errors with permitted values or range).
    - Add FluentValidation validators for each request.
    - _Requirements: 2.1, 2.5, 2.6, 3.1, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10_

  - [ ]* 3.13 Write property test for catalog invariants
    - **Property 2: Provider catalog invariants**
    - **Validates: Requirements 2.1, 2.6**

  - [ ]* 3.14 Write property test for settings validation
    - **Property 5: Settings validation matches the library validators**
    - **Validates: Requirements 3.7, 3.8, 4.6**

  - [ ]* 3.15 Write unit tests for the provider handlers
    - One test class per handler. Cover defaults per supported provider (Amazon SNS/SQS, RabbitMQ, Kafka), library-wide retry, idempotency and dead-letter defaults, unknown provider, Azure unavailable, and defaults-unavailable error with no partial pairs.
    - _Requirements: 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 3.9, 14.1_

  - [x] 3.16 Implement session command and query handlers
    - `StartSessionCommandHandler` (validate synchronously, return 202, run `IMicroserviceCluster.StartAsync` in a coordinator-owned background task with a 30 s budget and 10 s cleanup budget).
    - `StopSessionCommandHandler`, `TriggerQuoteAcceptedCommandHandler` (10 s linked timeout, one publish per call), `SetFailureModeCommandHandler`, `ClearFlowCommandHandler`, `GetSessionStatusQueryHandler`, `GetIntegrationDetailsQueryHandler` (409 when no Session; never returns defaults).
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6, 4.8, 4.9, 4.10, 6.3, 6.8, 6.9, 6.10, 9.9, 10.5, 11.7, 11.8, 12.5, 12.6_

  - [ ]* 3.17 Write unit tests for the session handlers
    - One test class per handler using fake cluster, fake clock and recording broadcaster. Cover start success, 30 s timeout, validation failure (nothing started), one microservice failing, stop with drain, trigger timeout, trigger with no Session, and the integration-details error path.
    - _Requirements: 4.2, 4.5, 4.6, 4.8, 4.10, 6.8, 11.8, 12.5, 12.6, 14.1_

- [x] 4. Checkpoint - Application core
  - Run only `dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

- [x] 5. Mock microservices
  - [x] 5.1 Implement `Demo.SalesLeads`
    - `ISalesLeadsService.AcceptQuoteAsync` returning `AcceptQuoteResult`. Add `SalesLead` (Pending, Published), `IMockDataGenerator` (name 1 to 100 characters, valid email 3 to 254, amount 0.01 to 999,999.99 with two decimals) and `TimeProvider` usage.
    - Build the `MessageEnvelope` with a unique `MessageId`, a 32-character hex `CorrelationId`, `MessageType` `quote-accepted` and `CreatedAtUtc` from the clock. Publish through `IMessagePublisher`.
    - On success emit `Published`. On a failed result or a narrow transport exception emit `Failed` with a sanitized reason and discard the lead.
    - _Requirements: 5.5, 5.8, 6.3, 6.4, 6.5, 6.6, 6.7, 6.9_

  - [ ]* 5.2 Write property test for published events
    - **Property 8: Published events are valid**
    - **Validates: Requirements 6.4, 6.5**

  - [ ]* 5.3 Write property test for N triggers
    - **Property 9: N triggers produce N distinct events and N results per consumer** (publish-side distinctness here; consumer counts are completed in task 5.7 and the integration tests)
    - **Validates: Requirements 6.9, 14.7**

  - [x] 5.4 Implement `Demo.Customer`
    - `CreateCustomerHandler : IMessageHandler<QuoteAcceptedEvent>` and `ICustomerStore` (`ConcurrentDictionary`, `TryAdd` by `LeadId`, bounded to 10,000 entries).
    - Emit `CustomerCreated` only when a new customer was added. A repeated `LeadId` with a new idempotency key adds nothing. Create the customer in one atomic step after all checks.
    - _Requirements: 5.8, 7.1, 7.2, 7.3, 7.5, 7.7, 7.8_

  - [x] 5.5 Implement `Demo.Communications`
    - `SendQuoteEmailHandler : IMessageHandler<QuoteAcceptedEvent>` and `IMockEmailOutbox` (in-memory, bounded to 10,000 entries).
    - Create one `MockEmail` addressed to the event email with a subject of at most 200 characters and a body of at most 5,000 characters, both containing the amount formatted `F2` with the invariant culture. Emit `EmailSent`. Add no HTTP or SMTP package.
    - _Requirements: 5.8, 8.1, 8.2, 8.3, 8.5_

  - [ ]* 5.6 Write property test for consumer outputs
    - **Property 11: Consumer outputs reflect the event**
    - **Validates: Requirements 7.2, 7.3, 8.2, 8.3**

  - [ ]* 5.7 Write property test for one customer per lead id
    - **Property 14: One customer per lead id**
    - **Validates: Requirements 7.5, 7.8**

  - [ ]* 5.8 Write unit tests for the microservice handlers
    - One test class per handler. Cover publish success and failure, lead discard on failure, customer creation, mock email content bounds and no network call.
    - _Requirements: 6.7, 7.2, 8.2, 8.5, 14.1_

- [x] 6. Infrastructure: handler chain, provider compositions, cluster, broadcaster
  - [x] 6.1 Implement in-memory adapters and log adapters
    - `InMemoryIdempotencyStore` (one instance per microservice host), `InMemoryDeadLetterSink`, a bounded attempt tracker (1,000 entries, keyed by correlation id), `DemoMessagingLog : IMessagingLog` (emits `Delivered` on consume and writes structured entries with correlation id, operation type and message type, with no payload or secrets), and `DemoDeadLetterLog : IDeadLetterLog` (emits `DeadLettered` with attempt count and failure kind from the tracker; "non-transient" when untracked).
    - _Requirements: 9.2, 10.4, 11.5, 11.6_

  - [x] 6.2 Implement the consumer handler chain
    - `QuoteAcceptedHandlerInvoker : IHandlerInvoker` validates the bound event (non-empty `LeadId`, name, email; amount within bounds) and throws a non-transient `MessageValidationException` before any handler runs.
    - `FlowReportingHandler` emits `Consumed` and `Retrying(attempt)` (transient failure with `attempt < Retry.MaxAttempts`). `FailureSimulatingHandler` reads the mode on attempt 1 and caches it per correlation id: `FailOnce` throws a transient failure on attempt 1 only, `AlwaysFail` throws a non-transient failure on every attempt.
    - _Requirements: 7.1, 7.6, 8.1, 8.6, 10.1, 10.2, 10.3, 10.6_

  - [ ]* 6.3 Write property test for invalid payloads
    - **Property 12: Invalid payloads never reach the handler**
    - **Validates: Requirements 7.6**

  - [ ]* 6.4 Write property test for redelivery idempotence
    - **Property 13: Redelivery is idempotent** (real `ConsumePipeline`, `IdempotencyGate` and in-memory store, 2 to 10 deliveries)
    - **Validates: Requirements 7.4, 8.4, 14.8**

  - [ ]* 6.5 Write property test for handler failure side effects
    - **Property 15: Handler failure leaves no side effects**
    - **Validates: Requirements 7.7, 8.6**

  - [ ]* 6.6 Write property test for consumer independence
    - **Property 16: Consumers are independent**
    - **Validates: Requirements 8.7, 10.7**

  - [ ]* 6.7 Write property test for failure simulation
    - **Property 17: Failure simulation behavior**
    - **Validates: Requirements 10.1, 10.2**

  - [ ]* 6.8 Write property test for failure mode at first delivery
    - **Property 18: Failure mode is fixed at first delivery**
    - **Validates: Requirements 10.6**

  - [ ]* 6.9 Write property test for retry and dead-letter steps
    - **Property 19: Retry and dead-letter steps**
    - **Validates: Requirements 10.3, 10.4**

  - [ ]* 6.10 Write property test for structured log entries
    - **Property 26: Structured log entries carry required fields**
    - Also assert the log adapter output contains no payload, credential or connection-string values (log part of Property 27).
    - **Validates: Requirements 11.5, 11.6**

  - [x] 6.11 Implement `IProviderComposition` and the RabbitMQ composition
    - Define `IProviderComposition`, `CompositionContext`, `ConsumerIdentity`.
    - RabbitMQ (`PublishSubscribe`): pre-register `IMassTransitBusAdapter` built with `receiveQueueName` `customer-ms` or `communications-ms` (finding F2) before calling `AddMassTransitMessagingProvider`. Add `TransportLifecycleHostedService` that calls `StartBusAsync`/`StopBusAsync` and `IMessageConsumerHost.StartAsync` (F3). Apply `MessagingConfiguration`: retry 3 / `Fixed` / 1 s, idempotency enabled with 3,600 s retention, shutdown timeout 10 s, per-service `ServiceName`.
    - _Requirements: 3.3, 3.5, 5.5, 5.7, 11.1_

  - [x] 6.12 Implement the Amazon SNS/SQS composition
    - Compose the seams manually (finding F1): `AmazonSqsBusAdapter.Create`, `SnsSqsTopologyManager`, `AwsSnsSqsTopologyClient`, `AmazonSqsMetrics`, `SqsDeadLetterSink` wrapped by a recording decorator, `AmazonSqsBusHostedService`. Register `IMassTransitBusAdapter` first so the library `TryAdd` does not replace it.
    - `EndpointName` is `customer-ms` or `communications-ms`; LocalStack `http://localhost:4566`; use throwaway credentials from configuration; run `AmazonSqsStartupValidator.Validate`.
    - _Requirements: 3.2, 3.5, 5.5, 5.7, 11.1_

  - [x] 6.13 Implement the Kafka composition
    - `AddKafkaMessagingProvider` (`Streaming`): topic `demo.quote-accepted`, groups `demo-customer-ms` and `demo-communications-ms`, publisher group `sales-leads-ms`. Remove the `KafkaConsumerHostedService` descriptor in the Sales Leads host (finding F8). Keep group ids and topic stable across Sessions. Run `KafkaStartupValidator.Validate`.
    - _Requirements: 3.4, 3.5, 5.5, 5.7, 11.1_

  - [x] 6.14 Implement `ProviderCatalog` (`IProviderCatalog`)
    - Register the three compositions and the unsupported Azure Service Bus entry with the reason "Messaging Core has no Azure Service Bus transport implementation yet". Build descriptors with `IsSecret`, `Editable` and permitted-values data. Make editable only numeric and boolean settings. Validate effective settings through `ConfigurationValidator` and the provider validators before any bus is built.
    - _Requirements: 2.3, 2.6, 3.1, 3.2, 3.3, 3.4, 3.7, 3.8, 4.6_

  - [x] 6.15 Implement `MicroserviceHost`, `MicroserviceCluster` and `DependencyPreflight`
    - `MicroserviceHost` wraps an `IHost` built with `Host.CreateApplicationBuilder`, sharing the parent `ILoggerFactory`. `MicroserviceCluster` runs the TCP pre-flight (3 s), starts both consumers in parallel and then the publisher, polls `IMessagingProvider.CheckTransportAsync`, enforces the 30 s start and 10 s cleanup budgets, and on any failure stops every started host and names the failing microservice or the provider, host and matching container command.
    - Implement stop with a 10 s `HostOptions.ShutdownTimeout`, `CheckHealthAsync` (aggregating `MessagingHealthChecks.ReadinessAsync`), `SetFailureMode`, and `Describe()` from the active `MessagingConfiguration`.
    - _Requirements: 4.2, 4.4, 4.5, 4.8, 4.10, 5.5, 5.6, 5.7, 10.6, 11.7_

  - [x] 6.16 Implement the SSE broadcaster
    - One bounded `Channel<SessionEvent>` per subscriber (capacity 1,000). Append to `SessionFlowLog` and write to subscribers under one lock so delivery order equals sequence order. Complete a subscriber whose channel is full so the client reconnects and replays.
    - _Requirements: 9.5, 9.10_

  - [ ]* 6.17 Write infrastructure unit tests
    - Cover composition build and `IMessagingProvider` resolution for each provider, distinct consumer identities per provider, `ProviderCatalog` secret flags and editability, cluster timeout and cleanup with fake hosts, and broadcaster ordering and overflow.
    - _Requirements: 4.5, 4.10, 5.7, 9.5, 14.1_

- [x] 7. Checkpoint - Infrastructure
  - Run only `dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

- [x] 8. Backend_For_Frontend API
  - [x] 8.1 Create the API host and cross-cutting behavior
    - Add `DemoOptions` with `IValidateOptions` that collects every missing or empty connection or credential setting by name and exits non-zero (`appsettings.Development.json` holds throwaway local values; environment variables override).
    - Add `ProviderCompositionSelfCheck` run at startup (scope per the result of task 1.2): on failure log the failed registration, terminate startup and accept no requests. Register all Application handlers and Infrastructure services.
    - Add Problem Details with an `IExceptionHandler` (400 lists each invalid field; 500 has no stack trace or infrastructure detail and includes `correlationId`), status-code pages for 404 and 405, 1 MiB Kestrel body limit mapped to 413, a `StrictJsonBody<T>` binder that rejects unknown fields, the CORS policy limited to `Demo:FrontendOrigin`, and `X-Correlation-Id` middleware.
    - _Requirements: 11.1, 11.2, 11.9, 12.2, 12.3, 12.4, 12.7, 12.8, 12.9, 12.10, 13.5, 13.9_

  - [x] 8.2 Implement the versioned REST endpoints
    - Under `/api/v1`: `GET providers`, `GET providers/{id}/defaults`, `POST providers/{id}/settings/validate`, `POST session`, `DELETE session`, `GET session`, `POST session/quotes` (sets `X-Correlation-Id` to the event correlation id on success and error), `PUT session/consumers/{consumerId}/failure-mode`, `DELETE session/flow-steps`, `GET session/integration-details`. Map `AppError` categories to status codes per the design's endpoint table.
    - _Requirements: 2.6, 3.7, 4.1, 6.3, 6.8, 10.5, 11.7, 11.9, 12.1, 12.5, 12.6_

  - [x] 8.3 Implement SSE, health endpoints and the health monitor
    - `GET /api/v1/events`: send a `session` snapshot, a `counters` snapshot, replay of retained steps after the last clear in sequence order, then live `flowStep`, `session` and `counters` events with `id: <sequence>`, plus a `heartbeat` every 500 ms.
    - `GET /health/live` and `GET /health/ready` (200 with `state: idle` when no Session; the library readiness result within 2 s otherwise; 503 when the transport is unreachable while liveness stays 200). Add `ProviderHealthMonitor` (5 s) that emits a `session` event when health changes.
    - _Requirements: 4.9, 9.5, 9.10, 9.11, 11.3, 11.4_

  - [ ]* 8.4 Write property test for 500 response content
    - **Property 27: Sanitized output leaks no secrets** (500 Problem Details part, including `correlationId`)
    - **Validates: Requirements 11.6, 12.4**

  - [ ]* 8.5 Write property test for trigger correlation header
    - **Property 28: Trigger responses carry the event correlation id**
    - **Validates: Requirements 11.9**

  - [ ]* 8.6 Write property test for request validation
    - **Property 29: Request validation names exactly the invalid fields**
    - **Validates: Requirements 12.3, 12.8**

  - [ ]* 8.7 Write property test for CORS
    - **Property 30: CORS only for the configured origin**
    - **Validates: Requirements 12.7**

  - [ ]* 8.8 Write property test for missing configuration
    - **Property 31: Missing configuration is named**
    - **Validates: Requirements 13.9**

  - [ ]* 8.9 Write API example tests with `WebApplicationFactory`
    - Cover 404, 405, 413 (1 MiB and 1 MiB + 1 byte), 409 trigger with no Session, 409 start with an active Session, health and readiness (idle, unavailable), SSE heartbeat and push within 500 ms, replay on reconnect, and the startup failure path.
    - _Requirements: 9.5, 9.10, 11.2, 11.3, 11.4, 12.5, 12.6, 12.9, 12.10_

- [x] 9. Checkpoint - Backend
  - Run only `dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

- [x] 10. Local infrastructure and developer experience
  - [x] 10.1 Add the RabbitMQ compose file and broker start script
    - `demo/docker-compose.yml` with `rabbitmq:3.13.7-management-alpine` (ports 5672 and 15672, health check `rabbitmq-diagnostics -q ping`, throwaway credentials). No `latest` tags.
    - `demo/scripts/start-broker.sh <rabbitmq|kafka|sns-sqs>`: check that Docker is running and the port is free, run `docker compose up -d --wait --wait-timeout 60 <service>` (Kafka and LocalStack use the unmodified root compose file), and on failure print the failing container or port, remove that service's container, and exit non-zero.
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.7, 13.8_

  - [x] 10.2 Write `demo/README.md`
    - List prerequisites with minimum versions, the exact build, start and stop commands, the single broker start command per provider, the Frontend URL `http://localhost:5173` and BFF URL `http://localhost:5080`, the unit-test and integration-test commands, and the library follow-ups F1 to F8 from the design.
    - _Requirements: 1.7, 13.3, 13.5, 13.6_

- [x] 11. Frontend (React, TypeScript, Vite)
  - [x] 11.1 Scaffold the Frontend and the typed API client
    - Create `demo/src/frontend` with Vite, React, TypeScript, Vitest, React Testing Library and fast-check.
    - Add `config/parseBaseUrl` (reads `VITE_BFF_BASE_URL`; rejects missing or non-absolute URLs and shows a `ConfigError` screen with no requests sent), the typed fetch client with a 10 s `AbortController` timeout, DTO types and a Problem Details parser.
    - _Requirements: 2.7, 13.6, 13.10_

  - [ ]* 11.2 Write property test for base URL parsing
    - **Property 32: Base URL parsing**
    - **Validates: Requirements 13.10**

  - [x] 11.3 Implement the reducer state store
    - Single `useReducer` store in a context: providers, defaults (with a monotonic request token), session, feed (newest first, capped at 500, de-duplicated by `sequence`), counters, connection status, correlation highlight, failure modes. Reducer cases are pure.
    - _Requirements: 2.8, 5.10, 9.6, 9.7, 9.8, 9.9, 9.12_

  - [x] 11.4 Implement `useEventStream`
    - `EventSource` wrapper with a heartbeat watchdog (disconnected after 1.5 s without any event), reconnect delays 1, 2, 4, 8, then 10 s, existing feed entries kept while disconnected, and de-duplication by `sequence` on replay.
    - _Requirements: 9.5, 9.10, 9.11_

  - [x] 11.5 Implement provider components
    - `ProviderSelector` (unsupported providers disabled with the reason always visible; selection of them ignored; stale defaults responses discarded), `SettingsPanel` (secrets read-only with the placeholder; edits validated through the validate endpoint; last valid value kept on error; error state shows no partial pairs), `StartControl` (starting status disables Start and selector; error display with retry).
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.7, 2.8, 3.1, 3.8, 3.9, 3.10, 4.1, 4.3, 4.5, 4.6_

  - [x] 11.6 Implement flow components
    - `FlowDiagram` (SVG: three labelled blocks with roles, one broker element labelled with the provider, three connections, counts per block), `MessageMarker` animations (500 to 2,000 ms, start within 500 ms), 2 s highlight and Mock_Customer / Mock_Email indicators with `LeadId`, `TriggerButton` labelled "Quote Accepted" (enabled only with an Active Session and three ready services; stays enabled while events are in flight), and `FailureToggle` per consumer with exactly Off, Fail once and Always fail (reverts and shows an error when saving fails).
    - _Requirements: 4.4, 5.1, 5.2, 5.3, 5.4, 5.9, 5.10, 6.1, 6.2, 6.7, 6.8, 6.10, 9.1, 9.2, 9.3, 9.4, 10.3, 10.4, 10.5, 10.8_

  - [x] 11.7 Implement activity and integration components
    - `ActivityFeed` (entries with UTC time, service, correlation id, lead id; correlation selection highlight; completed marking; clear control), `SessionBar` (active provider name, health, disconnected indicator, change-provider control) and `IntegrationDetailsView` (messaging mode, provider, idempotency, retry settings; error on failure with no stale values).
    - _Requirements: 4.8, 4.9, 9.6, 9.7, 9.8, 9.9, 9.11, 11.7, 11.8_

  - [x] 11.8 Wire the application
    - Compose the components and the event stream in `App`: hide the Trigger_Button and Flow_Diagram until a Session is Active; show the `ConfigError` screen when the base URL is invalid.
    - _Requirements: 2.2, 4.4, 4.8, 13.10_

  - [ ]* 11.9 Write property test for unavailable providers
    - **Property 3: Unsupported providers are rendered unavailable and cannot be selected**
    - **Validates: Requirements 2.3, 2.4, 14.4**

  - [ ]* 11.10 Write property test for latest selection
    - **Property 4: Latest selection wins**
    - **Validates: Requirements 2.8**

  - [ ]* 11.11 Write property test for trigger availability
    - **Property 7: Trigger availability**
    - **Validates: Requirements 6.1, 6.2**

  - [ ]* 11.12 Write property test for feed order and bound
    - **Property 21: Activity feed order and bound** (client side)
    - **Validates: Requirements 9.6, 9.12**

  - [ ]* 11.13 Write property test for event completion
    - **Property 22: Event completion marking** (client side)
    - **Validates: Requirements 9.8**

  - [ ]* 11.14 Write property test for correlation highlight
    - **Property 23: Correlation highlight**
    - **Validates: Requirements 9.7**

  - [ ]* 11.15 Write property test for replay
    - **Property 24: Replay reproduces the feed** (client side)
    - **Validates: Requirements 9.10**

  - [ ]* 11.16 Write property test for reconnect delays
    - **Property 25: Reconnect delay bounds**
    - **Validates: Requirements 9.11**

  - [ ]* 11.17 Write Frontend component tests
    - At least one passing test each for `ProviderSelector`, `SettingsPanel`, `TriggerButton` and `ActivityFeed`, plus `FlowDiagram` (counts, animation start and duration with fake timers), `useEventStream` (disconnect indicator within 2 s, reconnect) and the reducer.
    - _Requirements: 2.4, 3.10, 5.1, 9.1, 9.11, 14.3, 14.4_

- [x] 12. Checkpoint - Frontend
  - Run only `npm run test -- --run` in `demo/src/frontend` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

- [x] 13. Integration and architecture tests
  - [x] 13.1 Write container-backed integration tests
    - In `Demo.IntegrationTests`, tag every test `[Trait("Category","Integration")]`. For each supported provider (RabbitMQ, SNS/SQS via LocalStack, Kafka) start a Session against a Testcontainers broker, trigger one event through Sales Leads, and assert within 30 s that Customer MS holds exactly one Mock_Customer and Communications MS exactly one Mock_Email for that `LeadId`. Add a fan-out test confirming each consumer gets its own copy, and a retry and dead-letter scenario with `Fail once` and `Always fail`.
    - An unreachable broker after 60 s must fail the test, never skip or pass it.
    - Run with `dotnet test demo/tests/Demo.IntegrationTests --filter "Category=Integration" --blame-hang-timeout 2m` and an explicit timeout. Run `docker ps` before and after, and remove only the containers started by the run.
    - _Requirements: 5.7, 8.7, 10.7, 14.2, 14.9_

  - [x] 13.2 Write architecture and repository-hygiene tests
    - In `Demo.Architecture.Tests`: enforce inward dependencies and that the three microservice projects do not reference each other or ASP.NET; `Demo.Communications` has no HTTP or SMTP package; every command and query handler type has a test class; compose files pin every image and contain no `latest`; and `git diff` is empty for `src/`, `tests/`, `MessagingCore.sln`, `global.json` and the root `docker-compose.yml`, with new files only under `demo/`.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 5.8, 8.5, 12.2, 13.4, 14.1_

- [x] 14. Final checkpoint - All unit and property tests
  - Run only `dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"` once, with an explicit timeout. Report pass and fail counts and the first failure. Do not attempt fixes; ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP. Core implementation tasks are never optional.
- Each task references specific requirements for traceability. Property tasks name the design property they check.
- Container tests run only through task 13.1 with `--blame-hang-timeout 2m`. Plain `dotnet test` without `--filter "Category!=Integration"` must not be run on the demo solution.
- Task 1.2 gates the scope of the startup self-check in task 8.1.
- The Open Points in the design remain pending. The plan uses the recommended choices. Switching to the PostgreSQL variant of D3 would add tasks under group 6 and a compose service in 10.1.
- The workarounds for library findings F1 to F8 live in one composition class per provider (tasks 6.11 to 6.13), so they can be removed when the library changes.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["1.4", "3.1", "3.2", "5.1", "5.4", "5.5", "10.1", "11.1"] },
    { "id": 3, "tasks": ["3.3", "3.4", "3.5", "3.10", "3.12", "5.2", "5.3", "5.6", "5.7", "5.8", "6.1", "11.2", "11.3"] },
    { "id": 4, "tasks": ["3.6", "3.7", "3.8", "3.9", "3.11", "3.13", "3.14", "3.15", "3.16", "6.2", "6.11", "11.4", "11.5", "11.6", "11.7"] },
    { "id": 5, "tasks": ["3.17", "6.3", "6.4", "6.5", "6.6", "6.7", "6.8", "6.9", "6.10", "6.12", "6.13", "6.16", "11.8", "11.9", "11.10", "11.11", "11.12", "11.13", "11.14", "11.15", "11.16", "11.17"] },
    { "id": 6, "tasks": ["6.14"] },
    { "id": 7, "tasks": ["6.15"] },
    { "id": 8, "tasks": ["6.17", "8.1"] },
    { "id": 9, "tasks": ["8.2"] },
    { "id": 10, "tasks": ["8.3"] },
    { "id": 11, "tasks": ["8.4", "8.5", "8.6", "8.7", "8.8", "8.9", "10.2", "13.1", "13.2"] }
  ]
}
```
