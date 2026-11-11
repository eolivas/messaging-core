# Bugfix Requirements Document

## Introduction

When idempotent consumption is enabled and an idempotency key cannot be derived from a consumed message (for example `MessageId == Guid.Empty`), `IdempotencyGate` throws `MissingIdempotencyKeyException` before consulting the store or invoking the handler. That exception derives from plain `Exception`, and it is not one of the types `FailureClassifier` recognizes. The classifier therefore falls through to its default of `FailureKind.Transient`, and `RetryExecutor` retries the message up to `RetryPolicyConfig.MaxAttempts` times (with backoff) before dead-lettering it. With `MaxAttempts = 3` the resulting dead-letter has `AttemptCount == 3`.

A missing key is a deterministic property of the message: retrying the same envelope can never succeed. It wastes attempts and backoff delay, delays the offset commit or acknowledgement, and records a misleading attempt count. The handler is still not invoked and the message is still dead-lettered, so the defect is a partial violation of:

- messaging-core Requirement 6.3 (non-transient errors are dead-lettered with no retry) together with Requirement 7.6 (a missing key is rejected without side effects).
- kafka-provider Requirement 5.5 (a missing key is rejected before the handler runs). This clause does not itself state "zero retries". The no-retry expectation comes from reading it with messaging-core 6.3.

The defect lives in the shared core pipeline (`Messaging`), so it affects every transport that uses it, including Kafka (`KafkaStreamMessageProcessor`) and MassTransit (`CoreConsumeProcessor`).

Evidence: `tests/Messaging.Kafka.Tests/KafkaStreamMessageProcessorTests.cs` contains `ProcessAsync_MissingIdempotencyKey_IsNotRetried`, currently skipped. It asserts a single dead-letter with `AttemptCount == 1` and fails with 3 when unskipped.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN idempotency is enabled AND the idempotency key cannot be derived from a consumed envelope THEN the system classifies the resulting `MissingIdempotencyKeyException` as `FailureKind.Transient` (the default for an unrecognized exception type).

1.2 WHEN a consumed envelope triggers `MissingIdempotencyKeyException` AND `RetryPolicyConfig.MaxAttempts` is greater than 1 THEN the system retries the message, applying backoff delays, until `MaxAttempts` is exhausted, and only then dead-letters it.

1.3 WHEN a message with an underivable idempotency key is dead-lettered THEN the system records an `AttemptCount` equal to `MaxAttempts` (for example 3) instead of 1.

1.4 WHEN a consumed envelope with an underivable idempotency key is processed through the Kafka stream processor or the MassTransit core consume processor THEN the system exhibits the same retry-then-dead-letter behavior, because both delegate to the shared core pipeline.

### Expected Behavior (Correct)

2.1 WHEN idempotency is enabled AND the idempotency key cannot be derived from a consumed envelope THEN the system SHALL classify the resulting missing-idempotency-key failure as `FailureKind.NonTransient`.

2.2 WHEN a consumed envelope triggers a missing-idempotency-key failure, regardless of `MaxAttempts` THEN the system SHALL perform zero retries and SHALL route the message to the dead-letter sink on the first attempt.

2.3 WHEN a message with an underivable idempotency key is dead-lettered THEN the system SHALL record `AttemptCount == 1`, and SHALL still preserve the original payload and failure reason per messaging-core Requirement 6.6.

2.4 WHEN a consumed envelope with an underivable idempotency key is processed through any transport using the core pipeline (including `KafkaStreamMessageProcessor` and `CoreConsumeProcessor`) THEN the system SHALL dead-letter it on the first attempt, SHALL NOT invoke the application handler, and SHALL NOT consult or write to the idempotency store.

2.5 WHEN the missing-key failure is raised THEN the system SHALL continue to surface it as `MissingIdempotencyKeyException`, preserving the `MessageType` diagnostic and the payload-free message text, so existing callers that catch this type are unaffected.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a failure is a genuinely transient error (`MessageTimeoutException`, `TimeoutException`, `TransportConnectionException`, `DownstreamUnavailableException`) THEN the system SHALL CONTINUE TO retry up to `MaxAttempts` with the configured backoff before dead-lettering.

3.2 WHEN a failure is any other `MessagingFailureException` subtype THEN the system SHALL CONTINUE TO classify it by its own `Kind`, with no change to existing kinds (including `MessageValidationException`, `MalformedPayloadException`, and `MissingRequiredHeaderException`).

3.3 WHEN a failure is `InvalidEnvelopeException` or `UnknownContractException` THEN the system SHALL CONTINUE TO classify it as `NonTransient` and dead-letter it with zero retries.

3.4 WHEN a failure is an unrecognized exception type unrelated to the missing key THEN the system SHALL CONTINUE TO classify it as `Transient` (the documented default, messaging-core Requirement 6.5).

3.5 WHEN idempotency is enabled AND the key can be derived AND the key is already recorded as processed THEN the system SHALL CONTINUE TO acknowledge the duplicate without invoking the handler.

3.6 WHEN idempotency is enabled AND the key can be derived AND the key is new THEN the system SHALL CONTINUE TO invoke the handler, record the key on success, and expose the key to the consumer.

3.7 WHEN idempotency is disabled THEN the system SHALL CONTINUE TO skip the idempotency gate entirely, so a `Guid.Empty` `MessageId` does not cause rejection.

3.8 WHEN `IdempotencyGate` rejects an underivable key THEN the system SHALL CONTINUE TO do so before consulting the store and before invoking the handler (messaging-core Requirement 7.6).

3.9 WHEN public core contracts are consumed by existing code THEN the system SHALL CONTINUE TO expose the same public API surface, with no changes beyond the base type of `MissingIdempotencyKeyException` and its `Kind`.

## Bug Condition

```pascal
FUNCTION isBugCondition(X)
  INPUT: X of type ConsumeAttempt  // envelope + config
  OUTPUT: boolean

  RETURN X.config.idempotency.enabled = true
     AND NOT keyProvider.TryDerive(X.envelope)   // e.g. MessageId = Guid.Empty or blank MessageType
END FUNCTION
```

```pascal
// Property: Fix Checking - Missing Idempotency Key Is Non-Transient
FOR ALL X WHERE isBugCondition(X) DO
  result ← consume'(X)
  ASSERT classify'(MissingIdempotencyKeyException) = NonTransient
     AND result.deadLetter.AttemptCount = 1
     AND retryCount(result) = 0
     AND handlerInvocations(result) = 0
     AND idempotencyStoreCalls(result) = 0
END FOR
```

```pascal
// Property: Preservation Checking
FOR ALL X WHERE NOT isBugCondition(X) DO
  ASSERT consume(X) = consume'(X)
END FOR
// Includes: every other exception type keeps its classification, so transient failures
// still retry up to MaxAttempts and existing MessagingFailureException kinds are unchanged.
```

Counterexample: with idempotency enabled, `MaxAttempts = 3` and an envelope with `MessageId = Guid.Empty`, `ProcessAsync_MissingIdempotencyKey_IsNotRetried` observes `AttemptCount == 3` where `1` is expected.
