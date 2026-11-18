using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Consuming;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 12: In-flight tracking and rejection.
// Validates: Requirements 12.4, 12.6.
//
// InFlightTrackingConsumeProcessor counts each admitted delivery as in-flight for exactly the
// duration of the inner processing, releases it in a finally (even when the inner throws), and,
// once the tracker stops accepting, rejects with MessageDeliveryRejectedException without ever
// calling the inner processor.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "12")]
public class InFlightTrackingPropertyTests
{
    // Feature: sns-sqs-provider, Property 12: In-flight tracking and rejection
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public Property ProcessAsync_SequentialDeliveries_CountIsOneDuringAndBaselineAfterAndRejectsWhenStopped(
        bool[] innerSucceeds,
        NonNegativeInt stopAfterRaw)
    {
        // Arrange
        bool[] outcomes = innerSucceeds ?? Array.Empty<bool>();
        int stopAfter = outcomes.Length == 0 ? 0 : stopAfterRaw.Get % (outcomes.Length + 1);
        var tracker = new InFlightTracker();
        var inner = new ProbeProcessor(tracker);
        var sut = new InFlightTrackingConsumeProcessor(inner, tracker);
        bool holds = true;
        string failure = string.Empty;

        // Act / Assert per delivery
        for (int i = 0; i < outcomes.Length; i++)
        {
            if (i == stopAfter)
            {
                tracker.StopAccepting();
            }

            bool accepting = tracker.IsAccepting;
            inner.Succeeds = outcomes[i];
            int callsBefore = inner.CallCount;
            Exception? thrown = Run(sut);

            if (!accepting)
            {
                holds &= thrown is MessageDeliveryRejectedException
                    && inner.CallCount == callsBefore
                    && tracker.InFlightCount == 0;
            }
            else
            {
                holds &= inner.CallCount == callsBefore + 1
                    && inner.CountObservedDuringCall == 1
                    && tracker.InFlightCount == 0
                    && (outcomes[i] ? thrown is null : thrown is InvalidOperationException);
            }

            if (!holds)
            {
                failure = $"failed at delivery {i}, thrown={thrown?.GetType().Name ?? "none"}";
                break;
            }
        }

        return holds.ToProperty().Label($"outcomes=[{string.Join(",", outcomes)}] stopAfter={stopAfter} {failure}");
    }

    // Feature: sns-sqs-provider, Property 12: In-flight tracking and rejection
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public Property ProcessAsync_ConcurrentDeliveries_CountEqualsNumberInFlightThenReturnsToZero(
        PositiveInt concurrentRaw,
        bool stopWhileInFlight)
    {
        // Arrange
        int concurrent = (concurrentRaw.Get % 20) + 1;
        var tracker = new InFlightTracker();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new CountdownEvent(concurrent);
        var inner = new BlockingProcessor(release.Task, started);
        var sut = new InFlightTrackingConsumeProcessor(inner, tracker);

        // Act: start N deliveries that block inside the inner processor.
        Task[] deliveries = Enumerable.Range(0, concurrent)
            .Select(_ => Task.Run(() => sut.ProcessAsync(Envelope(), CancellationToken.None)))
            .ToArray();
        bool allStarted = started.Wait(TimeSpan.FromSeconds(30));
        int countWhileBlocked = tracker.InFlightCount;

        bool lateRejected = true;
        if (stopWhileInFlight)
        {
            tracker.StopAccepting();
            Exception? late = Run(sut);
            lateRejected = late is MessageDeliveryRejectedException;
        }

        int countAfterReject = tracker.InFlightCount;
        release.SetResult();
        Task.WaitAll(deliveries, TimeSpan.FromSeconds(30));
        int countAfter = tracker.InFlightCount;

        bool holds = allStarted
            && countWhileBlocked == concurrent
            && lateRejected
            && countAfterReject == concurrent
            && inner.CallCount == concurrent
            && countAfter == 0;

        return holds.ToProperty().Label(
            $"concurrent={concurrent} stop={stopWhileInFlight} during={countWhileBlocked} " +
            $"afterReject={countAfterReject} after={countAfter} calls={inner.CallCount}");
    }

    private static Exception? Run(IMassTransitConsumeProcessor sut)
    {
        try
        {
            sut.ProcessAsync(Envelope(), CancellationToken.None).GetAwaiter().GetResult();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static MessageEnvelope Envelope() => MassTransitTestSupport.ValidEnvelope();

    private sealed class ProbeProcessor : IMassTransitConsumeProcessor
    {
        private readonly InFlightTracker _tracker;

        public ProbeProcessor(InFlightTracker tracker) => _tracker = tracker;

        public bool Succeeds { get; set; } = true;

        public int CallCount { get; private set; }

        public int CountObservedDuringCall { get; private set; }

        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            CallCount++;
            CountObservedDuringCall = _tracker.InFlightCount;
            return Succeeds
                ? Task.CompletedTask
                : throw new InvalidOperationException("inner failure");
        }
    }

    private sealed class BlockingProcessor : IMassTransitConsumeProcessor
    {
        private readonly Task _gate;
        private readonly CountdownEvent _started;
        private int _calls;

        public BlockingProcessor(Task gate, CountdownEvent started)
        {
            _gate = gate;
            _started = started;
        }

        public int CallCount => Volatile.Read(ref _calls);

        public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
            _started.Signal();
            await _gate.ConfigureAwait(false);
        }
    }
}
