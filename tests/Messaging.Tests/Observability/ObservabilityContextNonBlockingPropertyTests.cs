using System;
using System.Collections.Generic;
using System.Diagnostics;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Observability;

namespace Messaging.Tests.Observability;

// Feature: messaging-core, Property 23: Observability failures never fail the message operation.
// Validates: Requirement 9.8.
//
// For ANY correlation id and ANY subset of throwing sinks (the structured log and/or the metrics
// counters), every observability hook on the facade — OnPublish, OnConsume, OnRetry, OnDeadLetter —
// ALWAYS completes without propagating the sink's exception. The best-effort swallow policy
// (Req 9.8) lives in ObservabilityContext, so a logger or metrics counter that throws on invocation
// must never change the publish/consume outcome. The property additionally asserts the durability
// clause of OnPublish: even when the sinks throw, OnPublish still returns trace headers carrying at
// least the correlation id, so propagation and correlation survive an observability failure.
//
// The trace source is a concrete sealed type (not a port), so a throwing trace source is not
// substitutable here; the throwing-subset space covered is {log, metrics} x {throws, succeeds}. The
// real MessagingTraceSource is exercised unchanged so the headers assertion runs against production
// behavior.
[Trait("Feature", "messaging-core")]
[Trait("Property", "23")]
public class ObservabilityContextNonBlockingPropertyTests
{
    /// <summary>
    /// A generated scenario: a non-empty correlation id and which sinks throw when invoked. Driving
    /// both throws and successes from one generator proves the hooks are non-blocking across every
    /// combination of failing sinks, not just the all-failing case.
    /// </summary>
    public sealed record NonBlockingScenario(string CorrelationId, bool LogThrows, bool MetricsThrows);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(ObservabilityContextNonBlockingPropertyTests) })]
    public Property Hooks_SinksThrow_CompleteWithoutFailingAndRetainCorrelationHeader(NonBlockingScenario scenario)
    {
        // Arrange: sinks that throw on every invocation per the scenario, plus a real trace source
        // and redactor so the facade runs its full production emission path.
        var log = new ThrowingMessagingLog(scenario.LogThrows);
        var metrics = new ThrowingMessagingMetrics(scenario.MetricsThrows);
        using var traceSource = new MessagingTraceSource(tracingEnabled: true, serviceName: "property-test");
        var context = new ObservabilityContext(log, metrics, traceSource, new PayloadRedactor());

        var headers = new Dictionary<string, string>
        {
            [MessagingTraceSource.CorrelationIdHeader] = scenario.CorrelationId,
        };

        // Act + Assert: no hook may propagate the sink exception, regardless of which sinks throw.
        var hooksDidNotThrow = DidNotThrow(() =>
        {
            using (context.OnPublish(scenario.CorrelationId, out _)) { }
            using (context.OnConsume(scenario.CorrelationId, headers)) { }
            context.OnRetry(scenario.CorrelationId);
            context.OnDeadLetter(scenario.CorrelationId);
        });

        // OnPublish must still hand back the correlation id even when the sinks throw (Req 9.8), so
        // propagation and correlation survive an observability failure.
        var publishHandedBackCorrelationId = false;
        var publishDidNotThrow = DidNotThrow(() =>
        {
            using (context.OnPublish(scenario.CorrelationId, out var traceHeaders))
            {
                publishHandedBackCorrelationId =
                    traceHeaders.TryGetValue(MessagingTraceSource.CorrelationIdHeader, out var propagated)
                    && propagated == scenario.CorrelationId;
            }
        });

        var holds = hooksDidNotThrow && publishDidNotThrow && publishHandedBackCorrelationId;

        return holds
            .ToProperty()
            .Label(
                $"logThrows={scenario.LogThrows}, metricsThrows={scenario.MetricsThrows} => " +
                $"hooksDidNotThrow={hooksDidNotThrow}, publishDidNotThrow={publishDidNotThrow}, " +
                $"correlationHeaderRetained={publishHandedBackCorrelationId}")
            .Classify(scenario.LogThrows && scenario.MetricsThrows, "all sinks throw")
            .Classify(!scenario.LogThrows && !scenario.MetricsThrows, "no sinks throw")
            .Classify(scenario.LogThrows ^ scenario.MetricsThrows, "one sink throws");
    }

    private static bool DidNotThrow(Action action)
    {
        try
        {
            action();
            return true;
        }
        catch
        {
            return false;
        }
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<NonBlockingScenario> NonBlockingScenarioArbitrary() => Arb.From(NonBlockingScenarioGen());

    /// <summary>
    /// Generates a non-empty, non-whitespace correlation id and an arbitrary subset of throwing sinks,
    /// so the property spans every combination of failing observability sinks across arbitrary ids.
    /// </summary>
    private static Gen<NonBlockingScenario> NonBlockingScenarioGen() =>
        from correlationId in NonEmptyIdentifier()
        from logThrows in Gen.Elements(true, false)
        from metricsThrows in Gen.Elements(true, false)
        select new NonBlockingScenario(correlationId, logThrows, metricsThrows);

    /// <summary>A non-empty, non-whitespace token used as the correlation id.</summary>
    private static Gen<string> NonEmptyIdentifier() =>
        from chars in Gen.NonEmptyListOf(Gen.Elements("abcdefghijklmnopqrstuvwxyz0123456789-".ToCharArray()))
        select new string(chars.ToArray());

    // ----- Throwing sink doubles --------------------------------------------------------------

    /// <summary>An <see cref="IMessagingLog"/> that throws on every invocation when configured to fail.</summary>
    private sealed class ThrowingMessagingLog : IMessagingLog
    {
        private readonly bool _throws;

        public ThrowingMessagingLog(bool throws) => _throws = throws;

        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
            if (_throws)
            {
                throw new InvalidOperationException("log sink unavailable");
            }
        }
    }

    /// <summary>An <see cref="IMessagingMetrics"/> that throws on every invocation when configured to fail.</summary>
    private sealed class ThrowingMessagingMetrics : IMessagingMetrics
    {
        private readonly bool _throws;

        public ThrowingMessagingMetrics(bool throws) => _throws = throws;

        public void RecordPublished() => Throw();

        public void RecordConsumed() => Throw();

        public void RecordRetry() => Throw();

        public void RecordDeadLetter() => Throw();

        private void Throw()
        {
            if (_throws)
            {
                throw new InvalidOperationException("metrics sink unavailable");
            }
        }
    }
}
