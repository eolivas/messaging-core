using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Observability;

namespace Messaging.Tests.Observability;

// Feature: messaging-core, Property 21: Metrics counters are monotonic and count events.
// Validates: Requirement 9.5.
//
// For ANY sequence of recorded lifecycle events (published / consumed / retry / dead-letter) in any
// interleaving and any count, each of the four counters exposed by MessagingMetrics is:
//   - monotonically non-decreasing: every observed cumulative value is >= the previous one, and
//   - exactly counting: its final cumulative value equals the number of events of that type recorded.
//
// The counters are observed the way a real exporter would observe them: a MeterListener subscribed to
// the "Messaging.Core" meter receives each Counter<long>.Add measurement synchronously, so the test
// reconstructs the exact per-counter cumulative series the runtime publishes and asserts the property
// against it — no reliance on MessagingMetrics internals.
[Trait("Feature", "messaging-core")]
[Trait("Property", "21")]
public class MessagingMetricsMonotonicCountingPropertyTests
{
    /// <summary>The four message-lifecycle event kinds the core counts (Req 9.5).</summary>
    public enum MetricEvent
    {
        Published,
        Consumed,
        Retry,
        DeadLetter,
    }

    private const string PublishedCounter = "messaging.published.count";
    private const string ConsumedCounter = "messaging.consumed.count";
    private const string RetryCounter = "messaging.retry.count";
    private const string DeadLetterCounter = "messaging.deadletter.count";

    private static readonly IReadOnlyDictionary<MetricEvent, string> CounterNames =
        new Dictionary<MetricEvent, string>
        {
            [MetricEvent.Published] = PublishedCounter,
            [MetricEvent.Consumed] = ConsumedCounter,
            [MetricEvent.Retry] = RetryCounter,
            [MetricEvent.DeadLetter] = DeadLetterCounter,
        };

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(MessagingMetricsMonotonicCountingPropertyTests) })]
    public Property RecordEvents_AnyInterleaving_CountersAreMonotonicAndCountEvents(MetricEvent[] events)
    {
        // Arrange: a dedicated metrics instance plus a listener that captures every per-counter
        // measurement in order, exactly as an OpenTelemetry/Prometheus exporter would observe them.
        using var metrics = new MessagingMetrics();

        // Per-counter ordered list of cumulative values as published to the listener.
        var observed = new Dictionary<string, List<long>>
        {
            [PublishedCounter] = new List<long>(),
            [ConsumedCounter] = new List<long>(),
            [RetryCounter] = new List<long>(),
            [DeadLetterCounter] = new List<long>(),
        };

        // Running cumulative total per counter, folded from each Add measurement the listener delivers.
        var running = new Dictionary<string, long>
        {
            [PublishedCounter] = 0,
            [ConsumedCounter] = 0,
            [RetryCounter] = 0,
            [DeadLetterCounter] = 0,
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == MessagingMetrics.MeterName && observed.ContainsKey(instrument.Name))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            // Fold each delta into the cumulative series the exporter would see.
            var cumulative = running[instrument.Name] += measurement;
            observed[instrument.Name].Add(cumulative);
        });
        listener.Start();

        // Act: replay the generated event sequence through the real recording methods.
        foreach (var e in events)
        {
            switch (e)
            {
                case MetricEvent.Published:
                    metrics.RecordPublished();
                    break;
                case MetricEvent.Consumed:
                    metrics.RecordConsumed();
                    break;
                case MetricEvent.Retry:
                    metrics.RecordRetry();
                    break;
                case MetricEvent.DeadLetter:
                    metrics.RecordDeadLetter();
                    break;
            }
        }

        // Ensure any buffered measurements are flushed to the callback before asserting.
        listener.RecordObservableInstruments();

        // Expected final count per event type, derived independently from the generated sequence.
        var expectedCounts = CounterNames.ToDictionary(
            kvp => kvp.Value,
            kvp => (long)events.Count(x => x == kvp.Key));

        // Assert: for every counter, the observed cumulative series is non-decreasing AND its final
        // value equals the exact number of events of that type (0 observations => count 0).
        var allHold = true;
        foreach (var counterName in observed.Keys)
        {
            var series = observed[counterName];

            var monotonic = IsNonDecreasing(series);
            var finalValue = series.Count == 0 ? 0L : series[^1];
            var countsExactly = finalValue == expectedCounts[counterName];

            allHold &= monotonic && countsExactly;
        }

        return allHold
            .ToProperty()
            .Label(
                $"events={events.Length} " +
                $"(pub={expectedCounts[PublishedCounter]}, con={expectedCounts[ConsumedCounter]}, " +
                $"retry={expectedCounts[RetryCounter]}, dlq={expectedCounts[DeadLetterCounter]}) => " +
                $"finals: pub={Final(observed[PublishedCounter])}, con={Final(observed[ConsumedCounter])}, " +
                $"retry={Final(observed[RetryCounter])}, dlq={Final(observed[DeadLetterCounter])}")
            .Classify(events.Length == 0, "empty sequence")
            .Classify(events.Distinct().Count() == 4, "all four event types present");
    }

    private static long Final(List<long> series) => series.Count == 0 ? 0L : series[^1];

    private static bool IsNonDecreasing(List<long> series)
    {
        for (var i = 1; i < series.Count; i++)
        {
            if (series[i] < series[i - 1])
            {
                return false;
            }
        }

        return true;
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MetricEvent[]> MetricEventSequenceArbitrary() => Arb.From(MetricEventSequenceGen());

    /// <summary>
    /// Generates an arbitrary-length sequence (0..200) of the four event kinds in any interleaving, so
    /// the single property covers empty sequences, single-type runs, and fully mixed streams across
    /// the counting and monotonicity surface.
    /// </summary>
    private static Gen<MetricEvent[]> MetricEventSequenceGen() =>
        from length in Gen.Choose(0, 200)
        from events in Gen.ArrayOf<MetricEvent>(
            Gen.Elements(
                MetricEvent.Published,
                MetricEvent.Consumed,
                MetricEvent.Retry,
                MetricEvent.DeadLetter),
            length)
        select events;
}
