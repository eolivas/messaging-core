using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Publishing;
using Messaging.Resilience;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Broker-shaped (MassTransit / RabbitMQ) round-trip + dead-letter integration test (task 22.2), run
/// against a <b>real</b> RabbitMQ broker in a Testcontainer. It exercises the actual adapter shells
/// (<see cref="MassTransitPublisher"/>, <see cref="MassTransitConsumerHost"/>) over a native
/// MassTransit RabbitMQ bus, proving two guarantees end-to-end:
/// <list type="number">
/// <item><b>Publish/consume round-trip:</b> a message published through the publisher shell is
/// delivered to the consumer over RabbitMQ with every envelope field preserved (Req 4.4).</item>
/// <item><b>Dead-letter on retry exhaustion (Req 8.1):</b> a handler that keeps failing transiently
/// exhausts the uniform retry policy; the core routes the message to the dead-letter sink
/// <em>within 5 seconds</em> of the final failed attempt, and the message then leaves the active
/// processing pipeline (the broker queue drains and the handler is never invoked again).</item>
/// </list>
/// The one-engine / real-dependency rule forbids the MassTransit in-memory transport here: both
/// behaviours are broker behaviours, so only a real RabbitMQ demonstrates them.
/// </summary>
[Collection(RabbitMqCollection.Name)]
public sealed class MassTransitRabbitMqIntegrationTests
{
    private readonly RabbitMqContainerFixture _rabbit;

    public MassTransitRabbitMqIntegrationTests(RabbitMqContainerFixture rabbit) => _rabbit = rabbit;

    [Fact]
    public async Task PublishThenConsume_OverRealBroker_RoundTripsEveryEnvelopeField()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var cancellationToken = cts.Token;

        var connectionString = await _rabbit.GetConnectionStringAsync(cancellationToken);

        // A unique queue per test run keeps this test independent of any other test's messages.
        var queueName = $"roundtrip-{Guid.NewGuid():N}";
        var correlationId = $"corr-{Guid.NewGuid():N}";
        var payload = EnvelopeFactory.Utf8("round-trip payload");
        var sent = EnvelopeFactory.Create(correlationId, payload);

        // The processor captures the first delivered envelope and completes a signal so the test can
        // await the round-trip. A normal return acknowledges the message (Req matches other modes).
        var received = new TaskCompletionSource<MessageEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);
        var processor = new DelegatingConsumeProcessor((envelope, _) =>
        {
            received.TrySetResult(envelope);
            return Task.CompletedTask;
        });

        await using var bus = RabbitMqBusAdapter.Create(connectionString, queueName, processor);
        var host = new MassTransitConsumerHost(bus, processor, NullLogger<MassTransitConsumerHost>.Instance);
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);

        await host.StartAsync(cancellationToken);
        try
        {
            // Act: publish through the publisher shell; the live bus fans it out to the receive queue.
            var publishResult = await publisher.PublishAsync(sent, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);

            var delivered = await WaitAsync(received.Task, TimeSpan.FromSeconds(30), cancellationToken);

            // Assert: the round-trip preserved every envelope field across the real transport (Req 4.4).
            Assert.Equal(sent.MessageId, delivered.MessageId);
            Assert.Equal(sent.CorrelationId, delivered.CorrelationId);
            Assert.Equal(sent.MessageType, delivered.MessageType);
            Assert.Equal(sent.ContractVersion, delivered.ContractVersion);
            Assert.Equal(payload, delivered.Payload.ToArray());
            Assert.Equal(sent.Headers["tenant"], delivered.Headers["tenant"]);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task HandlerExhaustsRetries_DeadLettersWithin5s_AndMessageLeavesActivePipeline()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var cancellationToken = cts.Token;

        var connectionString = await _rabbit.GetConnectionStringAsync(cancellationToken);

        var queueName = $"deadletter-{Guid.NewGuid():N}";
        var correlationId = $"corr-{Guid.NewGuid():N}";
        var payload = EnvelopeFactory.Utf8("poison payload");
        var sent = EnvelopeFactory.Create(correlationId, payload);

        // The core dead-letter path: a capturing sink, plus the router + classifier + backoff that the
        // RetryExecutor drives. A fixed 1s per-attempt delay with 2 attempts keeps the whole retry
        // sequence short so the final failed attempt and the dead-letter are both well inside the test
        // window, while still exercising the real (non-faked) retry/backoff/dead-letter code.
        var deadLetterSink = new CapturingDeadLetterSink();
        var router = new DeadLetterRouter(deadLetterSink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());
        var retryExecutor = new RetryExecutor(new FailureClassifier(), new BackoffCalculator(), router);
        var policy = new RetryPolicyConfig
        {
            MaxAttempts = 2,
            Backoff = BackoffStrategy.Fixed,
            PerAttemptDelaySeconds = 1,
        };

        // Count handler invocations and stamp the final failed attempt, so the test can assert the
        // message is not reprocessed after dead-lettering and can bound the final-failure → dead-letter
        // elapsed time to 5s (Req 8.1).
        var handlerInvocations = 0;
        var lastFailedAttemptAtUtc = DateTimeOffset.MinValue;

        // The processor stands in for "the core steps run ahead of the handler": it drives the handler
        // through the real RetryExecutor. On a routed+acknowledged dead-letter it returns normally, so
        // MassTransit acknowledges and the message leaves the active pipeline (Req 8.1).
        var processor = new DelegatingConsumeProcessor(async (envelope, ct) =>
        {
            var result = await retryExecutor.ExecuteAsync(
                envelope,
                policy,
                processAsync: _ =>
                {
                    Interlocked.Increment(ref handlerInvocations);
                    lastFailedAttemptAtUtc = DateTimeOffset.UtcNow;

                    // A transient failure: eligible for retry, dead-lettered only on exhaustion (Req 6.2, 6.5).
                    throw new TransportConnectionException("Simulated broker-side downstream failure.");
                },
                idempotencyKey: null,
                ct);

            // On exhaustion the router acknowledges (removes from the pipeline). If the sink had failed
            // the result would be RetainedUnacknowledged and we would rethrow to keep the message; here
            // the sink always accepts, so a normal return acknowledges the source.
            if (result.DeadLetterRouting == DeadLetterRoutingResult.RetainedUnacknowledged)
            {
                throw new InvalidOperationException("Dead-letter sink rejected the message; retaining it.");
            }
        });

        await using var bus = RabbitMqBusAdapter.Create(connectionString, queueName, processor);
        var host = new MassTransitConsumerHost(bus, processor, NullLogger<MassTransitConsumerHost>.Instance);
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);

        await host.StartAsync(cancellationToken);
        try
        {
            var publishResult = await publisher.PublishAsync(sent, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);

            // Act: wait for the message to be dead-lettered.
            await WaitAsync(deadLetterSink.FirstCapture, TimeSpan.FromSeconds(30), cancellationToken);

            // Assert 1 — the message was dead-lettered, preserving its context (Req 8.1, 8.2).
            var captured = Assert.Single(deadLetterSink.Captured);
            Assert.Equal(sent.MessageId, captured.Message.Envelope.MessageId);
            Assert.Equal(correlationId, captured.Message.CorrelationId);
            Assert.Equal(policy.MaxAttempts, captured.Message.AttemptCount);
            Assert.Equal(payload, captured.Message.Envelope.Payload.ToArray());

            // Assert 2 — routing happened within 5s of the final failed attempt (Req 8.1).
            var sinceFinalFailure = captured.AtUtc - lastFailedAttemptAtUtc;
            Assert.True(
                sinceFinalFailure >= TimeSpan.Zero && sinceFinalFailure <= TimeSpan.FromSeconds(5),
                $"Expected dead-letter within 5s of the final failed attempt but it took {sinceFinalFailure}.");

            // Assert 3 — the handler ran exactly MaxAttempts times: the bounded policy terminated and
            // the message was not reprocessed after dead-lettering (Req 6.5).
            Assert.Equal(policy.MaxAttempts, Volatile.Read(ref handlerInvocations));

            // Assert 4 — the message left the active pipeline: give the broker a moment to settle, then
            // confirm no redelivery occurred (handler count unchanged). A dead-lettered-and-acknowledged
            // message is removed from active processing (Req 8.1).
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.Equal(policy.MaxAttempts, Volatile.Read(ref handlerInvocations));
            Assert.Single(deadLetterSink.Captured);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Awaits <paramref name="task"/> but fails fast with a clear message if it does not complete
    /// within <paramref name="timeout"/>, so a hung broker surfaces as a test timeout rather than the
    /// whole suite stalling on the outer token.
    /// </summary>
    private static async Task<T> WaitAsync<T>(Task<T> task, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout, cancellationToken));
        if (completed != task)
        {
            throw new TimeoutException($"The awaited operation did not complete within {timeout}.");
        }

        return await task;
    }

    private static async Task WaitAsync(Task task, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout, cancellationToken));
        if (completed != task)
        {
            throw new TimeoutException($"The awaited operation did not complete within {timeout}.");
        }

        await task;
    }

    /// <summary>
    /// A test consume-steps seam that runs an arbitrary async action per envelope, letting one test
    /// capture the round-trip and another drive the real retry/dead-letter pipeline without a bespoke
    /// fake per case.
    /// </summary>
    private sealed class DelegatingConsumeProcessor : IMassTransitConsumeProcessor
    {
        private readonly Func<MessageEnvelope, CancellationToken, Task> _onProcess;

        public DelegatingConsumeProcessor(Func<MessageEnvelope, CancellationToken, Task> onProcess) =>
            _onProcess = onProcess;

        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken) =>
            _onProcess(envelope, cancellationToken);
    }
}
