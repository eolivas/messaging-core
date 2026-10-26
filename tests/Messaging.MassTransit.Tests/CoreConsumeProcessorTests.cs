using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.MassTransit;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Messaging.Resilience;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Example/unit tests for the production <see cref="CoreConsumeProcessor"/> consume flows (task 2.6).
/// Each test drives the REAL processor wired to the REAL core collaborators (<see cref="ObservabilityContext"/>,
/// <see cref="RetryExecutor"/> with an injected no-op delay, <see cref="ConsumePipeline"/>,
/// <see cref="DeadLetterRouter"/>) and substitutes only the leaf seams (contract binder, handler
/// invoker, dead-letter sink, idempotency ports, observability log) so the generated scenario flows
/// deterministically through the production processor without a broker and without wall-clock waits.
/// </summary>
/// <remarks>
/// Scenarios (one logical assertion focus each):
/// <list type="bullet">
///   <item><description>Unknown/unbindable contract → dead-letter, handler NOT invoked, reason names the contract (Req 6.5, 6.6).</description></item>
///   <item><description>Idempotent duplicate → handler NOT invoked, message acked (returns) (Req 7.4).</description></item>
///   <item><description>Idempotency disabled → handler invoked with a null idempotency key (Req 7.6).</description></item>
///   <item><description>Missing idempotency key → dead-letter, key NOT recorded (Req 7.5).</description></item>
///   <item><description>Observability hook invoked with the correlation id on consume (Req 9.2).</description></item>
///   <item><description>A throwing observability sink does NOT fail the operation (Req 9.8).</description></item>
/// </list>
/// </remarks>
public class CoreConsumeProcessorTests
{
    private const string ContractType = "orders.created.v1";
    private const string CorrelationId = "corr-abc-123";

    // ----- Req 6.5, 6.6: unknown/unbindable contract -----------------------------------------

    [Fact]
    public async Task ProcessAsync_WhenContractUnknown_DeadLettersWithoutInvokingHandler()
    {
        // Arrange: binder rejects the message type with UnknownContractException (non-transient), so
        // the message is dead-lettered on a single attempt and the handler is never reached.
        var binder = new ThrowingContractBinder(
            new UnknownContractException(
                ContractType,
                $"No contract is registered for message type '{ContractType}'."));
        var handler = new RecordingHandlerInvoker();
        var sink = new CapturingDeadLetterSink();

        CoreConsumeProcessor processor = Build(
            binder: binder,
            handlerInvoker: handler,
            deadLetterSink: sink);

        // Act
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: dead-lettered, handler never invoked.
        Assert.Single(sink.Captured);
        Assert.Equal(0, handler.InvocationCount);
    }

    [Fact]
    public async Task ProcessAsync_WhenContractUnknown_DeadLetterReasonNamesTheContract()
    {
        // Arrange
        var binder = new ThrowingContractBinder(
            new UnknownContractException(
                ContractType,
                $"No contract is registered for message type '{ContractType}'."));
        var sink = new CapturingDeadLetterSink();

        CoreConsumeProcessor processor = Build(
            binder: binder,
            deadLetterSink: sink);

        // Act
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: the dead-letter record's failure reason surfaces the offending contract type.
        DeadLetterMessage deadLetter = Assert.Single(sink.Captured);
        Assert.Contains(ContractType, deadLetter.FailureReason);
    }

    // ----- Req 7.4: idempotent duplicate ------------------------------------------------------

    [Fact]
    public async Task ProcessAsync_WhenDuplicateIdempotencyKey_AcksWithoutInvokingHandler()
    {
        // Arrange: the store reports the derived key as already processed, so the gate short-circuits
        // the duplicate before the handler and the processor acknowledges by returning normally.
        var handler = new RecordingHandlerInvoker();
        var sink = new CapturingDeadLetterSink();
        IdempotencyGate gate = EnabledGate(
            keyProvider: new FixedKeyProvider(),
            store: new StubIdempotencyStore(alreadyProcessed: true));

        CoreConsumeProcessor processor = Build(
            handlerInvoker: handler,
            deadLetterSink: sink,
            idempotencyGate: gate);

        // Act: returning (not throwing) is the acknowledge signal.
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: duplicate suppressed — handler never invoked, nothing dead-lettered.
        Assert.Equal(0, handler.InvocationCount);
        Assert.Empty(sink.Captured);
    }

    // ----- Req 7.6: idempotency disabled ------------------------------------------------------

    [Fact]
    public async Task ProcessAsync_WhenIdempotencyDisabled_InvokesHandlerWithNullIdempotencyKey()
    {
        // Arrange: no gate configured (idempotency disabled) → the handler runs and observes a null
        // idempotency key on its consume context.
        var handler = new RecordingHandlerInvoker();

        CoreConsumeProcessor processor = Build(handlerInvoker: handler);

        // Act
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: handler invoked exactly once with a null idempotency key.
        Assert.Equal(1, handler.InvocationCount);
        Assert.Null(handler.LastContext!.IdempotencyKey);
    }

    // ----- Req 7.5: missing idempotency key ---------------------------------------------------

    [Fact]
    public async Task ProcessAsync_WhenIdempotencyKeyMissing_DeadLettersWithoutRecordingKey()
    {
        // Arrange: idempotency enabled but the key cannot be derived, so the gate throws
        // MissingIdempotencyKeyException before consulting the store or invoking the handler. The
        // message is dead-lettered and the processed-key record is never written.
        var handler = new RecordingHandlerInvoker();
        var sink = new CapturingDeadLetterSink();
        var store = new StubIdempotencyStore(alreadyProcessed: false);
        IdempotencyGate gate = EnabledGate(
            keyProvider: new UnderivableKeyProvider(),
            store: store);

        CoreConsumeProcessor processor = Build(
            handlerInvoker: handler,
            deadLetterSink: sink,
            idempotencyGate: gate);

        // Act
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: dead-lettered, handler never invoked, key never recorded (Req 7.5).
        Assert.Single(sink.Captured);
        Assert.Equal(0, handler.InvocationCount);
        Assert.Equal(0, store.RecordProcessedCount);
    }

    // ----- Req 9.2: observability hook carries the correlation id -----------------------------

    [Fact]
    public async Task ProcessAsync_OnConsume_RecordsObservabilityEventWithCorrelationId()
    {
        // Arrange: a capturing log records every observability event; the consume hook must fire with
        // the envelope's correlation id.
        var log = new CapturingMessagingLog();

        CoreConsumeProcessor processor = Build(messagingLog: log);

        // Act
        await processor.ProcessAsync(Envelope(CorrelationId), CancellationToken.None);

        // Assert: a consume event was recorded carrying the correlation id.
        Assert.Contains(
            log.Events,
            e => e.EventType == MessagingEventType.Consume && e.CorrelationId == CorrelationId);
    }

    // ----- Req 9.8: a throwing observability sink does not fail the operation -----------------

    [Fact]
    public async Task ProcessAsync_WhenObservabilitySinkThrows_StillProcessesSuccessfully()
    {
        // Arrange: the observability log throws on every event. The operation must still succeed
        // (handler invoked, no exception surfaced) because observability is best-effort (Req 9.8).
        var handler = new RecordingHandlerInvoker();

        CoreConsumeProcessor processor = Build(
            handlerInvoker: handler,
            messagingLog: new ThrowingMessagingLog());

        // Act + Assert: no exception propagates out of ProcessAsync.
        await processor.ProcessAsync(Envelope(), CancellationToken.None);

        // Assert: the consume still completed — the handler ran.
        Assert.Equal(1, handler.InvocationCount);
    }

    // ===== Composition helpers ================================================================

    /// <summary>
    /// Builds a real <see cref="CoreConsumeProcessor"/> wired with real core collaborators, allowing a
    /// test to substitute the leaf seams it needs. Defaults give a succeeding single-attempt flow: a
    /// binder that always binds, a handler that completes, an accepting dead-letter sink, no
    /// idempotency gate, and inert observability. <c>MaxAttempts = 1</c> keeps every failing scenario
    /// to a single attempt with no backoff wait.
    /// </summary>
    private static CoreConsumeProcessor Build(
        IContractBinder? binder = null,
        IHandlerInvoker? handlerInvoker = null,
        IDeadLetterSink? deadLetterSink = null,
        IdempotencyGate? idempotencyGate = null,
        IMessagingLog? messagingLog = null)
    {
        var observability = new ObservabilityContext(
            messagingLog ?? new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        var retryPolicy = new RetryPolicyConfig { MaxAttempts = 1 };

        var deadLetterRouter = new DeadLetterRouter(
            deadLetterSink ?? new CapturingDeadLetterSink(),
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());

        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            deadLetterRouter,
            delay: static (_, _) => Task.CompletedTask);

        var pipeline = idempotencyGate is null
            ? new ConsumePipeline(
                new ConsumerHostStateMachine(),
                binder ?? new AlwaysBindContractBinder(),
                handlerInvoker ?? new RecordingHandlerInvoker())
            : new ConsumePipeline(
                new ConsumerHostStateMachine(),
                binder ?? new AlwaysBindContractBinder(),
                handlerInvoker ?? new RecordingHandlerInvoker(),
                idempotencyGate);

        // The pipeline only accepts deliveries while Running.
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new CoreConsumeProcessor(
            observability,
            retryExecutor,
            pipeline,
            deadLetterRouter,
            retryPolicy);
    }

    private static IdempotencyGate EnabledGate(IIdempotencyKeyProvider keyProvider, IIdempotencyStore store) =>
        new(keyProvider, store, new IdempotencyConfig { Enabled = true });

    private static MessageEnvelope Envelope(string correlationId = CorrelationId) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = ContractType,
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(StringComparer.Ordinal),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
        ContractVersion = 1,
    };

    // ===== Leaf-seam fakes ====================================================================

    /// <summary>Binds any envelope to a trivial contract object; binding never fails.</summary>
    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    /// <summary>Rejects every bind with the configured exception (used for the unknown-contract path).</summary>
    private sealed class ThrowingContractBinder : IContractBinder
    {
        private readonly Exception _error;

        public ThrowingContractBinder(Exception error) => _error = error;

        public bool IsKnown(string messageType) => false;

        public object Bind(MessageEnvelope envelope) => throw _error;
    }

    /// <summary>A handler invoker that records how many times it ran and the last consume context.</summary>
    private sealed class RecordingHandlerInvoker : IHandlerInvoker
    {
        public int InvocationCount { get; private set; }

        public ConsumeContext? LastContext { get; private set; }

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken)
        {
            InvocationCount++;
            LastContext = context;
            return Task.CompletedTask;
        }
    }

    /// <summary>A dead-letter sink that accepts and captures every dead-lettered message.</summary>
    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterMessage> Captured { get; } = new();

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            Captured.Add(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>Derives a fixed, always-available idempotency key.</summary>
    private sealed class FixedKeyProvider : IIdempotencyKeyProvider
    {
        public bool TryDerive(MessageEnvelope envelope, out string idempotencyKey)
        {
            idempotencyKey = "fixed-idempotency-key";
            return true;
        }
    }

    /// <summary>Never derives a key, forcing the missing-idempotency-key rejection (Req 7.5).</summary>
    private sealed class UnderivableKeyProvider : IIdempotencyKeyProvider
    {
        public bool TryDerive(MessageEnvelope envelope, out string idempotencyKey)
        {
            idempotencyKey = string.Empty;
            return false;
        }
    }

    /// <summary>A store with a test-controlled processed answer that counts record-processed calls.</summary>
    private sealed class StubIdempotencyStore : IIdempotencyStore
    {
        private readonly bool _alreadyProcessed;

        public StubIdempotencyStore(bool alreadyProcessed) => _alreadyProcessed = alreadyProcessed;

        public int RecordProcessedCount { get; private set; }

        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(_alreadyProcessed);

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken)
        {
            RecordProcessedCount++;
            return Task.CompletedTask;
        }
    }

    /// <summary>Captures every observability event so a test can assert the correlation id (Req 9.2).</summary>
    private sealed class CapturingMessagingLog : IMessagingLog
    {
        public List<(MessagingEventType EventType, string CorrelationId)> Events { get; } = new();

        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc) =>
            Events.Add((eventType, correlationId));
    }

    /// <summary>An observability log that throws on every event, exercising the best-effort guarantee (Req 9.8).</summary>
    private sealed class ThrowingMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc) =>
            throw new InvalidOperationException("Observability sink failure.");
    }

    private sealed class NoOpMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMessagingMetrics : IMessagingMetrics
    {
        public void RecordPublished()
        {
        }

        public void RecordConsumed()
        {
        }

        public void RecordRetry()
        {
        }

        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
    {
        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterLog : IDeadLetterLog
    {
        public void DeadLettered(string correlationId, string failureReason, int attemptCount)
        {
        }

        public void DeadLetterRoutingFailed(string correlationId, string failureReason, int attemptCount, Exception error)
        {
        }
    }
}
