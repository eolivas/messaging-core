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
/// <item><b>Dead-letter on retry exhaustion (Req 8.4, 11.5):</b> a handler that keeps failing
/// transiently exhausts the uniform retry policy; the core routes the poison message to the
/// dead-letter sink <em>within 5 seconds</em> of the final failed attempt, the message is not
/// redelivered to the application handler, and it is not reported in-flight — the broker's receive
/// queue drains to zero ready messages, so the message has left the active processing pipeline.</item>
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

        await using var bus = IntegrationBus.Create(connectionString, queueName, processor);
        var host = new MassTransitConsumerHost(bus, processor, NullLogger<MassTransitConsumerHost>.Instance);
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);

        await host.StartAsync(cancellationToken);
        try
        {
            // Act: publish through the publisher shell; the live bus fans it out to the receive queue.
            var publishResult = await publisher.PublishAsync(sent, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);

            var delivered = await WaitAsync(received.Task, TimeSpan.FromSeconds(30), cancellationToken);

            // Assert: the round-trip preserved every envelope field across the real transport
            // (Req 6.1, 6.2, 6.3). Scalar metadata first.
            Assert.Equal(sent.MessageId, delivered.MessageId);
            Assert.Equal(sent.CorrelationId, delivered.CorrelationId);
            Assert.Equal(sent.MessageType, delivered.MessageType);
            Assert.Equal(sent.ContractVersion, delivered.ContractVersion);

            // Req 6.1 — the UTC creation timestamp survives the wire verbatim. The wire contract
            // carries CreatedAtUtc as a DateTimeOffset serialized/deserialized by MassTransit's JSON
            // envelope, which preserves full tick precision, so exact equality is the right bar here
            // (no sub-millisecond truncation to tolerate).
            Assert.Equal(sent.CreatedAtUtc, delivered.CreatedAtUtc);

            // Req 6.2 — the delivered header collection matches the published one exactly: same number
            // of entries, every published key present, each mapped to a value equal to its published
            // value. Comparing counts then every key/value pair catches both dropped and spurious keys.
            Assert.Equal(sent.Headers.Count, delivered.Headers.Count);
            foreach (var (key, expectedValue) in sent.Headers)
            {
                Assert.True(
                    delivered.Headers.TryGetValue(key, out var deliveredValue),
                    $"Delivered envelope is missing the published header '{key}'.");
                Assert.Equal(expectedValue, deliveredValue);
            }

            // Req 6.3 — payload bytes equal in length and in every byte position.
            Assert.Equal(payload, delivered.Payload.ToArray());
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

        await using var bus = IntegrationBus.Create(connectionString, queueName, processor);
        var host = new MassTransitConsumerHost(bus, processor, NullLogger<MassTransitConsumerHost>.Instance);
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);

        await host.StartAsync(cancellationToken);
        try
        {
            var publishResult = await publisher.PublishAsync(sent, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);

            // Act: wait for the message to be dead-lettered.
            await WaitAsync(deadLetterSink.FirstCapture, TimeSpan.FromSeconds(30), cancellationToken);

            // Assert 1 — the poison message was routed to the dead-letter sink, preserving its context:
            // message id, correlation id, attempt count, and payload bytes (Req 11.5 routing; Req 8.5).
            var captured = Assert.Single(deadLetterSink.Captured);
            Assert.Equal(sent.MessageId, captured.Message.Envelope.MessageId);
            Assert.Equal(correlationId, captured.Message.CorrelationId);
            Assert.Equal(policy.MaxAttempts, captured.Message.AttemptCount);
            Assert.Equal(payload, captured.Message.Envelope.Payload.ToArray());

            // Assert 2 — routing happened within 5s of the final failed attempt (Req 8.4).
            var sinceFinalFailure = captured.AtUtc - lastFailedAttemptAtUtc;
            Assert.True(
                sinceFinalFailure >= TimeSpan.Zero && sinceFinalFailure <= TimeSpan.FromSeconds(5),
                $"Expected dead-letter within 5s of the final failed attempt but it took {sinceFinalFailure}.");

            // Assert 3 — the handler ran exactly MaxAttempts times: the bounded policy terminated and
            // the poison message was not redelivered to the application handler after dead-lettering
            // (Req 11.5 — not redelivered to the handler).
            Assert.Equal(policy.MaxAttempts, Volatile.Read(ref handlerInvocations));

            // Assert 4 — the message left the active pipeline: give the broker a moment to settle, then
            // confirm no redelivery occurred (handler count unchanged) and the sink captured it exactly
            // once. A dead-lettered-and-acknowledged message is removed from active processing (Req 8.6).
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.Equal(policy.MaxAttempts, Volatile.Read(ref handlerInvocations));
            Assert.Single(deadLetterSink.Captured);

            // Assert 5 — the poison message is not reported in-flight on the real broker (Req 11.5).
            // After the dead-letter-and-acknowledge the receive queue has drained: the broker reports
            // zero messages in the ready state, so nothing is sitting in the queue awaiting redelivery.
            // Combined with Assert 3/4 (the handler is never re-invoked), this proves the message is
            // neither queued nor being reprocessed — it has left the active processing pipeline.
            var readyMessages = await BrokerQueueInspector.GetReadyMessageCountAsync(
                connectionString,
                queueName,
                cancellationToken);
            Assert.Equal(0u, readyMessages);
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
