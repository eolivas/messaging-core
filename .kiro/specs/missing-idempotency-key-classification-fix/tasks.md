# Implementation Plan

Run notes: only the .NET 10 runtime is installed, so prefix test runs with `DOTNET_ROLL_FORWARD=Major`. Build with `-m:1`. Docker-backed tests need `DOCKER_HOST=unix:///var/run/docker.sock`. Tests follow `Method_Scenario_ExpectedResult` naming and Arrange-Act-Assert.

- [x] 1. Write bug condition exploration tests
  - **Property 1: Bug Condition** - Missing Idempotency Key Is Non-Transient
  - **CRITICAL**: These tests MUST FAIL on unfixed code - failure confirms the bug exists
  - **DO NOT attempt to fix the test or the code when it fails**
  - **NOTE**: These tests encode the expected behavior - they will validate the fix when they pass after implementation
  - **GOAL**: Surface counterexamples that demonstrate the bug exists
  - **Scoped PBT Approach**: The bug is deterministic, so scope the property to underivable-key envelopes (`MessageId == Guid.Empty` or blank `MessageType`) with idempotency enabled and `MaxAttempts` in 1..10
  - Bug Condition from design: `isBugCondition(X)` = idempotency enabled AND `NOT keyProvider.TryDerive(X.envelope)`
  - Create `tests/Messaging.Tests/Consuming/MissingIdempotencyKeyNonTransientPropertyTests.cs` with `UnderivableKey_AnyMaxAttempts_DeadLettersOnFirstAttempt`
    - Generate underivable envelopes (reuse the generator approach from `IdempotencyUnderivableKeyRejectionPropertyTests`) x `MaxAttempts` 1..10
    - Drive a real `ConsumePipeline` + `IdempotencyGate` + `RetryExecutor` + `FailureClassifier` with a recording dead-letter sink
    - Assert `AttemptCount == 1`, `CountingHandlerInvoker` has zero calls, `FakeIdempotencyStore` has zero lookups and writes
    - FsCheck.Xunit 3.4.0, `MaxTest = 200`, traits `Feature = missing-idempotency-key-classification-fix`, `Property = 1`
  - Create `tests/Messaging.Tests/Resilience/FailureClassifierTests.cs` with `Classify_MissingIdempotencyKeyException_ReturnsNonTransient`
  - Un-skip `ProcessAsync_MissingIdempotencyKey_IsNotRetried` in `tests/Messaging.Kafka.Tests/KafkaStreamMessageProcessorTests.cs` (remove `Skip`); optionally also assert `Handler.InvocationCount == 0` and `Store.LookupCount == 0`
  - Run the three tests on UNFIXED code
  - **EXPECTED OUTCOME**: Tests FAIL (this is correct - it proves the bug exists)
  - Expected counterexamples: `AttemptCount == MaxAttempts` (for example 3) instead of 1; classifier returns `Transient`
  - Note: the `MaxAttempts = 1` case passes on unfixed code, confirming only multi-attempt configs expose the bug
  - If failures differ (for example the gate not throwing), re-hypothesize the root cause
  - Mark task complete when tests are written, run, and failures are documented
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3_

- [x] 2. Write preservation property tests (BEFORE implementing fix)
  - **Property 2: Preservation** - Other Failure Classification and Retry Behavior
  - **IMPORTANT**: Follow observation-first methodology
  - Observe on UNFIXED code: transient exceptions (`MessageTimeoutException`, `TimeoutException`, `TransportConnectionException`, `DownstreamUnavailableException`) retry up to `MaxAttempts` and dead-letter with `AttemptCount == MaxAttempts`
  - Observe on UNFIXED code: `MessageValidationException`, `MalformedPayloadException`, `InvalidEnvelopeException`, `UnknownContractException` classify `NonTransient` and dead-letter with `AttemptCount == 1`
  - Observe on UNFIXED code: unrecognized exceptions (`InvalidOperationException`) classify `Transient` and use all `MaxAttempts`
  - Create `tests/Messaging.Tests/Resilience/FailureClassifierPreservationPropertyTests.cs`
    - `Classify_OtherFailureKinds_KeepExistingClassification`: for all exceptions in the fixed set, the observed kind is unchanged
    - `Execute_TransientFailure_RetriesUpToMaxAttempts`: for all transient/unknown exceptions and arbitrary `MaxAttempts`, attempts equal `MaxAttempts`; for non-transient, attempts equal 1
    - FsCheck.Xunit 3.4.0, `MaxTest >= 100`, traits `Feature = missing-idempotency-key-classification-fix`, `Property = 2`
  - Add unit test `Classify_UnrecognizedException_ReturnsTransient` in `FailureClassifierTests.cs` (Req 3.4)
  - Confirm existing gate tests (`IdempotencyUnderivableKeyRejectionPropertyTests`, duplicate/new-key/disabled tests) pass on unfixed code
  - Run tests on UNFIXED code
  - **EXPECTED OUTCOME**: Tests PASS (this confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing on unfixed code
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

- [x] 3. Fix for missing idempotency key being classified as transient

  - [x] 3.1 Implement the fix
    - Re-grep `src` (all transport projects: Kafka, MassTransit, RabbitMQ, core) for `catch (MessagingFailureException)` and `is MessagingFailureException` BEFORE editing; confirm no handler's control flow changes (design audit found none)
    - In `src/Messaging/Consuming/MissingIdempotencyKeyException.cs`, change the base type from `Exception` to `MessagingFailureException`
    - Keep the class `sealed`, keep the `(string messageType)` constructor, keep the `MessageType` property, and pass the existing payload-free message text to `base(...)`
    - Add `/// <inheritdoc />` `public override FailureKind Kind => FailureKind.NonTransient;`
    - Update XML remarks: derives from `MessagingFailureException` as non-transient; `FailureClassifier` routes it to dead-letter with zero retries; message names only the message type, never payload content
    - Doc-only update in `src/Messaging/Resilience/FailureClassifier.cs`: add `MissingIdempotencyKeyException` to the non-transient list in `<remarks>`; no logic change
    - _Bug_Condition: isBugCondition(input) where idempotency enabled AND NOT keyProvider.TryDerive(input.envelope)_
    - _Expected_Behavior: classify'(MissingIdempotencyKeyException) = NonTransient AND AttemptCount = 1 AND retryCount = 0 AND handlerInvocations = 0 AND idempotencyStoreCalls = 0_
    - _Preservation: other exception classifications, transient retry up to MaxAttempts, gate behavior for derivable keys and disabled idempotency unchanged_
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9_

  - [x] 3.2 Verify bug condition exploration tests now pass
    - **Property 1: Expected Behavior** - Missing Idempotency Key Is Non-Transient
    - **IMPORTANT**: Re-run the SAME tests from task 1 - do NOT write new tests
    - Run `MissingIdempotencyKeyNonTransientPropertyTests`, `Classify_MissingIdempotencyKeyException_ReturnsNonTransient` and the un-skipped `ProcessAsync_MissingIdempotencyKey_IsNotRetried`
    - **EXPECTED OUTCOME**: Tests PASS (confirms bug is fixed)
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 3.3 Verify preservation tests still pass
    - **Property 2: Preservation** - Other Failure Classification and Retry Behavior
    - **IMPORTANT**: Re-run the SAME tests from task 2 - do NOT write new tests
    - Run `FailureClassifierPreservationPropertyTests`, `Classify_UnrecognizedException_ReturnsTransient`, and existing idempotency gate tests
    - **EXPECTED OUTCOME**: Tests PASS (confirms no regressions)

- [x] 4. Add unit tests for the fixed exception and the MassTransit path
  - In `tests/Messaging.Tests/Resilience/FailureClassifierTests.cs`, add `MissingIdempotencyKeyException_Constructed_PreservesMessageTypeAndPayloadFreeMessage`: `MessageType` equals the argument, message contains the message type and no payload content, `Kind == NonTransient` (Req 2.5)
  - In `tests/Messaging.MassTransit.Tests/CoreConsumeProcessorTests.cs`, add `ProcessAsync_MissingIdempotencyKey_WithMaxAttempts3_DeadLettersWithAttemptCountOne`, based on the existing missing-key test (~line 140) with `MaxAttempts = 3`, asserting `AttemptCount == 1` (Req 2.4)
  - Run both; **EXPECTED OUTCOME**: PASS on fixed code
  - _Requirements: 2.4, 2.5_

- [x] 5. Checkpoint - Ensure all tests pass
  - Run `dotnet build -m:1` for the solution
  - Run `DOTNET_ROLL_FORWARD=Major dotnet test` for `Messaging.Tests`, `Messaging.Kafka.Tests`, `Messaging.MassTransit.Tests` and `Messaging.Architecture.Tests`
  - Run RabbitMQ/Kafka container tests with `DOCKER_HOST=unix:///var/run/docker.sock` as a regression pass
  - Confirm `InwardDependencyRuleTests` still pass
  - Ensure all tests pass, ask the user if questions arise.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9_
