# Missing Idempotency Key Classification Bugfix Design

## Overview

When idempotent consumption is enabled and the idempotency key cannot be derived, `IdempotencyGate` throws `MissingIdempotencyKeyException`. The exception is `sealed : Exception`, and `FailureClassifier.Classify` only recognizes `MessagingFailureException` (by `Kind`), `InvalidEnvelopeException`, `UnknownContractException` and `TimeoutException`. Everything else defaults to `FailureKind.Transient`, so `RetryExecutor` retries a deterministic failure up to `MaxAttempts` times with backoff. The dead-letter then records `AttemptCount == MaxAttempts` instead of 1.

**Recommended fix:** make `MissingIdempotencyKeyException` derive from `MessagingFailureException` and override `Kind => FailureKind.NonTransient`. This is the same pattern `MissingRequiredHeaderException` (Kafka) and `MessageValidationException` already use. `FailureClassifier` is not modified. The exception type name, namespace, constructor, `MessageType` property and message text are unchanged.

## Glossary

- **Bug_Condition (C)**: Idempotency is enabled and `IIdempotencyKeyProvider.TryDerive` fails for the consumed envelope (for example `MessageId == Guid.Empty` or a blank `MessageType`).
- **Property (P)**: The failure is classified `NonTransient`, so the message is dead-lettered on the first attempt with `AttemptCount == 1`, zero handler invocations and zero idempotency store calls.
- **Preservation**: Classification of every other exception type, retry behavior for transient failures, and gate behavior for derivable keys or when idempotency is disabled.
- **MissingIdempotencyKeyException**: `src/Messaging/Consuming/MissingIdempotencyKeyException.cs`. Thrown by `IdempotencyGate` before the store or handler is touched.
- **MessagingFailureException**: `src/Messaging.Abstractions/MessagingFailureException.cs`. Abstract base with protected constructors `(string)` and `(string, Exception)` and `abstract FailureKind Kind`.
- **FailureClassifier**: `src/Messaging/Resilience/FailureClassifier.cs`. Type-driven switch that maps an exception to `FailureKind`.
- **RetryExecutor**: `src/Messaging/Resilience/RetryExecutor.cs`. Catches non-cancellation exceptions, asks the classifier, and retries only `Transient` failures.

## Bug Details

### Bug Condition

The bug manifests when idempotency is enabled and the key provider cannot derive a key. `IdempotencyGate` correctly throws `MissingIdempotencyKeyException` before any side effect, but the exception carries no classification. The classifier's default arm treats it as `Transient`.

**Formal Specification:**
```
FUNCTION isBugCondition(input)
  INPUT: input of type ConsumeAttempt  // envelope + MessagingConfiguration
  OUTPUT: boolean

  RETURN input.config.idempotency.enabled = true
         AND NOT keyProvider.TryDerive(input.envelope)
         AND classify(MissingIdempotencyKeyException) = Transient   // defect: should be NonTransient
END FUNCTION
```

### Examples

- Idempotency enabled, `MaxAttempts = 3`, `MessageId = Guid.Empty`: expected `AttemptCount == 1`, actual `3`. This is the skipped test `ProcessAsync_MissingIdempotencyKey_IsNotRetried`.
- Idempotency enabled, `MaxAttempts = 5`, blank `MessageType`: expected 1 attempt, actual 5 attempts with backoff delays between them.
- The same envelope through `CoreConsumeProcessor` (MassTransit) shows the same retry-then-dead-letter behavior, because it delegates to the same `RetryExecutor` and `FailureClassifier`.
- Edge case: `MaxAttempts = 1`. Behavior is already correct (`AttemptCount == 1`) and must remain so.

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- `MessageTimeoutException`, `TimeoutException`, `TransportConnectionException` and `DownstreamUnavailableException` still retry up to `MaxAttempts`.
- Other `MessagingFailureException` subtypes (`MessageValidationException`, `MalformedPayloadException`, `MissingRequiredHeaderException`) keep their own `Kind`.
- `InvalidEnvelopeException` and `UnknownContractException` remain `NonTransient` through their explicit classifier arms.
- Unrecognized exception types still default to `Transient` (Req 6.5).
- Duplicate-key acknowledgement, new-key handling, and the idempotency-disabled bypass are untouched.
- The gate still throws before consulting the store or invoking the handler (Req 7.6).

**Scope:**
All inputs where the bug condition does not hold are unaffected. `FailureClassifier` and `RetryExecutor` are not edited, so no code path other than the one that handles this exception changes.

## Hypothesized Root Cause

Based on code inspection (confirmed, not speculative):

1. **Missing classification contract**: `MissingIdempotencyKeyException` extends `Exception` directly, so it exposes no `Kind` and `FailureClassifier` has no arm for it. The `_ => FailureKind.Transient` default catches it.
2. **Inconsistent pattern**: `UnknownContractException` and `InvalidEnvelopeException` were handled by adding explicit classifier arms. `MissingIdempotencyKeyException` was added later (gate, Req 7.6) without either approach, and the existing tests only asserted dead-lettering, not attempt count.
3. **Doc claim not enforced**: The spec text (kafka design 5.5, rabbitmq tasks) states the exception is non-transient, but no test in core asserted it. The Kafka test that does assert it is skipped.

## Correctness Properties

Property 1: Bug Condition - Missing Idempotency Key Is Non-Transient

_For any_ consumed envelope where idempotency is enabled and the key cannot be derived (isBugCondition returns true) and for any `MaxAttempts` in the valid range, the fixed pipeline SHALL classify `MissingIdempotencyKeyException` as `FailureKind.NonTransient`, dead-letter the message on the first attempt with `AttemptCount == 1`, perform zero retries, invoke the handler zero times, and make zero idempotency store calls, while still surfacing the failure as `MissingIdempotencyKeyException` with its `MessageType` and payload-free message.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**

Property 2: Preservation - Other Failure Classification and Retry Behavior

_For any_ input where the bug condition does NOT hold (a different exception type, a derivable key, or idempotency disabled), the fixed code SHALL produce the same result as the original code: every other exception keeps its classification, transient failures retry up to `MaxAttempts`, non-transient failures dead-letter with `AttemptCount == 1`, and unrecognized exceptions default to `Transient`.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9**

## Fix Implementation

### Design Decision: Base Class vs Explicit Classifier Rule

| Aspect | A. Derive from `MessagingFailureException` (recommended) | B. Add `MissingIdempotencyKeyException => NonTransient` arm to `FailureClassifier` |
|---|---|---|
| Source of truth | The exception declares its own category, which is the documented design ("single source of truth", classifier remark). | Category lives in the classifier, separated from the type. |
| Files changed | 1 (`MissingIdempotencyKeyException.cs`) | 1 (`FailureClassifier.cs`) plus remarks |
| Consistency | Matches `MissingRequiredHeaderException`, `MessageValidationException`, `MalformedPayloadException`. | Matches `InvalidEnvelopeException` and `UnknownContractException`. |
| Public API | Base type changes and `Kind` is added. Source and binary compatible for callers catching the type. A caller catching `MessagingFailureException` would now also catch it (see risks). | No public API change. |
| Third-party classifiers | Any `IFailureClassifier` implementation that honors `MessagingFailureException.Kind` is correct automatically. | A custom classifier would still misclassify it. |
| Regression resistance | A new exception of this kind cannot be forgotten in a switch. | Each new non-transient exception needs another arm, which is how this bug arose. |

Why the existing explicit arms do not argue for B: `InvalidEnvelopeException` lives in `Messaging.Contracts` and `UnknownContractException` is a sealed `Exception` in core. Neither was moved onto the base type, and `Messaging.Contracts` is the innermost layer, so it may not depend on `Messaging.Abstractions`. Changing that is a separate concern and out of scope.

Layering check: `MissingIdempotencyKeyException` is in `Messaging` (Application), which already references `Messaging.Abstractions` (Domain). `FailureClassifier` in the same project already imports `Messaging.Abstractions`. The dependency points inward, so the architecture tests (`InwardDependencyRuleTests`) are not affected. The architecture test project has no exception-specific assertions.

Base class feasibility: `MessagingFailureException` is `abstract`, not sealed, with protected `(string)` and `(string, Exception)` constructors and an abstract `Kind`. Derivation is straightforward. The `MessageType` property and the existing message text are preserved by passing the same interpolated string to `base(...)`. The exception stays `sealed`.

Confidence: high for A. What would change the recommendation: a requirement for zero public API change, or a decision that `MessagingFailureException` catch sites must not see this type (none exist today, see below).

### Changes Required

**File**: `src/Messaging/Consuming/MissingIdempotencyKeyException.cs`

**Specific Changes**:
1. **Base type**: change `: Exception` to `: MessagingFailureException`.
2. **Constructor**: keep `MissingIdempotencyKeyException(string messageType)`; pass the existing payload-free message to `base(...)`. Keep assigning `MessageType`.
3. **Classification**: add `public override FailureKind Kind => FailureKind.NonTransient;` with `<inheritdoc />`.
4. **Docs**: update the remarks to state that it derives from `MessagingFailureException` as non-transient, so `FailureClassifier` routes it to dead-letter with zero retries via its existing rule, and that the message names only the message type, never payload content.

**Resulting shape:**
```csharp
public sealed class MissingIdempotencyKeyException : MessagingFailureException
{
    public string MessageType { get; }

    public MissingIdempotencyKeyException(string messageType)
        : base($"Idempotency is enabled but no idempotency key could be derived for message type "
            + $"'{messageType}'; the message was rejected before any side effects.")
    {
        MessageType = messageType;
    }

    /// <inheritdoc />
    public override FailureKind Kind => FailureKind.NonTransient;
}
```

**File**: `src/Messaging/Resilience/FailureClassifier.cs` (documentation only)
5. Optionally add `MissingIdempotencyKeyException` to the non-transient list in the `<remarks>`. No logic change.

### Consumer Audit

- `IdempotencyGate`: the only thrower. Throws `new MissingIdempotencyKeyException(envelope.MessageType)`. Unchanged.
- `ConsumePipeline`, `IdempotencyGateDecision`: XML doc references only.
- `RetryExecutor`: catches `Exception` with `when (error is not OperationCanceledException)` and classifies. No catch ordering depends on the exception type.
- `DeadLetterRouter`: catches `Exception` around the sink call only. Unaffected.
- `FailureClassifier`: the first switch arm (`MessagingFailureException`) now matches this type. Arm order is irrelevant because the type matches no other arm.
- `IdempotencyUnderivableKeyRejectionPropertyTests`: `catch (MissingIdempotencyKeyException)` followed by a bare `catch`. Still catches by the same type, so it is unaffected.
- `KafkaStreamMessageProcessorTests`, `CoreConsumeProcessorTests` (MassTransit): reference the exception by name or in comments only. No dependence on it being a plain `Exception`.
- Architecture tests: no references to the exception.
- No `catch (MessagingFailureException)` or `is MessagingFailureException` exists in `src/Messaging`, so the new inheritance cannot change any handler's control flow. Transport projects (Kafka, MassTransit, RabbitMQ) were not found to catch it by name. This should be re-checked with a final grep over `src` when implementing.

### Risks and Non-Functional Notes

- **Public API**: the base type changes and a `Kind` member is added. Requirement 3.9 explicitly allows this. It is source compatible, and binary compatible for callers that only catch the type.
- **Observability**: dead-letter reason text is unchanged. `AttemptCount` changes from `MaxAttempts` to 1, and retry metrics or logs for this failure disappear, which is the intended effect. Dashboards that counted retries for this case will drop.
- **Security**: the message contains only the message type. This is a contract-defined identifier, not payload content (Req 2.5). No payload or key values are added.
- **Rollout**: no data or config migration. The change is behavior-only and reversible by reverting one file.

## Testing Strategy

### Validation Approach

Surface the defect on unfixed code first, then verify the fix and preservation. Property tests use FsCheck.Xunit 3.4.0 with `MaxTest >= 100`, matching existing property tests (for example `IdempotencyUnderivableKeyRejectionPropertyTests`, `MaxTest = 200`). Tests are named `Method_Scenario_ExpectedResult` and follow Arrange-Act-Assert.

Run notes: only the .NET 10 runtime is installed, so use `DOTNET_ROLL_FORWARD=Major`; build with `-m:1`; Docker integration tests need `DOCKER_HOST=unix:///var/run/docker.sock`.

### Exploratory Bug Condition Checking

**Goal**: Confirm the root cause on unfixed code.

**Test Plan**: Un-skip and run the new tests below before applying the fix.

**Test Cases**:
1. **Kafka not retried**: `ProcessAsync_MissingIdempotencyKey_IsNotRetried` with `MaxAttempts = 3` (will fail on unfixed code with `AttemptCount == 3`).
2. **Classifier unit test**: `Classify_MissingIdempotencyKeyException_ReturnsNonTransient` (will fail, returns `Transient`).
3. **Fix-checking property** below (will fail for `MaxAttempts > 1`).
4. **MaxAttempts = 1 edge case**: passes on unfixed code, confirming only multi-attempt configs expose the bug.

**Expected Counterexamples**:
- `AttemptCount == MaxAttempts` instead of 1 for any underivable-key envelope.
- Cause: classifier default arm, as hypothesized. If the tests fail differently (for example the gate not throwing), re-hypothesize.

### Fix Checking

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition(input) DO
  result := consume_fixed(input)
  ASSERT classify_fixed(MissingIdempotencyKeyException) = NonTransient
     AND result.deadLetter.AttemptCount = 1
     AND handlerInvocations(result) = 0
     AND idempotencyStoreCalls(result) = 0
END FOR
```

### Preservation Checking

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition(input) DO
  ASSERT consume_original(input) = consume_fixed(input)
END FOR
```

**Testing Approach**: Property-based testing, because it generates many exception kinds and `MaxAttempts` values and catches edge cases a few hand-picked examples would miss.

**Test Plan**: Observe current behavior on unfixed code for non-bug inputs (it already passes), then keep these tests as regression guards.

**Test Cases**:
1. **Classification preservation**: every other known exception keeps its kind.
2. **Transient retry preservation**: transient exceptions still retry exactly `MaxAttempts` times, then dead-letter with `AttemptCount == MaxAttempts`.
3. **Non-transient preservation**: `InvalidEnvelopeException`, `UnknownContractException`, `MessageValidationException` still dead-letter with `AttemptCount == 1`.
4. **Gate preservation**: existing `IdempotencyUnderivableKeyRejectionPropertyTests` and duplicate/new-key/disabled tests pass unchanged.

### Unit Tests

- `tests/Messaging.Tests/Resilience/FailureClassifierTests.cs` (new): `Classify_MissingIdempotencyKeyException_ReturnsNonTransient`.
- Same file: `MissingIdempotencyKeyException_Constructed_PreservesMessageTypeAndPayloadFreeMessage` (Req 2.5): `MessageType` equals the argument, the message contains the message type, and `Kind == NonTransient`.
- Same file: `Classify_UnrecognizedException_ReturnsTransient` (Req 3.4).
- `tests/Messaging.Kafka.Tests/KafkaStreamMessageProcessorTests.cs`: remove the `Skip` from `ProcessAsync_MissingIdempotencyKey_IsNotRetried`. Optionally extend it to assert `Handler.InvocationCount == 0` and `Store.LookupCount == 0`.
- `tests/Messaging.MassTransit.Tests/CoreConsumeProcessorTests.cs`: add `ProcessAsync_MissingIdempotencyKey_WithMaxAttempts3_DeadLettersWithAttemptCountOne`, based on the existing missing-key test at line ~140 with `MaxAttempts = 3`, to cover the `CoreConsumeProcessor` path (Req 2.4).

### Property-Based Tests

- **Fix checking** (new, `tests/Messaging.Tests/Consuming/MissingIdempotencyKeyNonTransientPropertyTests.cs`): `UnderivableKey_AnyMaxAttempts_DeadLettersOnFirstAttempt`. Generate underivable envelopes (empty `MessageId` or blank `MessageType`, all else valid, reusing the generator approach from `IdempotencyUnderivableKeyRejectionPropertyTests`) and `MaxAttempts` in 1..10. Drive a real `ConsumePipeline` + `IdempotencyGate` through `RetryExecutor` with `FailureClassifier` and a recording dead-letter sink. Assert `AttemptCount == 1`, `CountingHandlerInvoker` has zero calls, `FakeIdempotencyStore` has zero lookups and writes. `MaxTest = 200`. Traits: `Feature = missing-idempotency-key-classification-fix`, `Property = 1`.
- **Preservation** (new, `FailureClassifierPreservationPropertyTests.cs`): `Classify_OtherFailureKinds_KeepExistingClassification` and `Execute_TransientFailure_RetriesUpToMaxAttempts`. Generate exceptions from a fixed set (transient: `MessageTimeoutException`, `TimeoutException`, `TransportConnectionException`, `DownstreamUnavailableException`; non-transient: `MessageValidationException`, `MalformedPayloadException`, `InvalidEnvelopeException`, `UnknownContractException`; unknown: `InvalidOperationException`) with arbitrary `MaxAttempts`. Assert the expected kind, and that attempts equal `MaxAttempts` for transient and unknown failures and 1 for non-transient. `MaxTest = 100` or more. `Property = 2`. Must pass before and after the fix.

### Integration Tests

- Kafka: `KafkaStreamMessageProcessorTests` un-skipped test, run through the full processor with a recording sink and a harness retry executor (no Docker needed).
- MassTransit: `CoreConsumeProcessorTests` new case above. The existing RabbitMQ container integration tests need no change; run them with `DOCKER_HOST=unix:///var/run/docker.sock` as a regression pass.
- Full regression: `dotnet build -m:1` then `DOTNET_ROLL_FORWARD=Major dotnet test` across `Messaging.Tests`, `Messaging.Kafka.Tests`, `Messaging.MassTransit.Tests` and `Messaging.Architecture.Tests`.
