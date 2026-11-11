using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

public class KafkaConsumerHostedServiceTests
{
    private static readonly TimeSpan Backoff = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Safety = TimeSpan.FromSeconds(10);

    private static KafkaConsumeResult Record(string correlationId = "corr-1") =>
        new(KafkaTestSupport.ValidEnvelope(correlationId), Partition: 0, Offset: 5);

    private static KafkaConsumerHost CreateHost(IKafkaConsumer consumer, IStreamMessageProcessor processor) =>
        new(consumer, processor, KafkaTestSupport.ValidSettings(), NullLogger<KafkaConsumerHost>.Instance);

    private static KafkaConsumerHostedService CreateService(
        KafkaConsumerHost host,
        CapturingLogger logger,
        Func<TimeSpan, CancellationToken, Task> delay,
        int shutdownSeconds = 30,
        TimeSpan? backoff = null) =>
        new(host,
            new LifecycleConfig { ShutdownTimeoutSeconds = shutdownSeconds },
            logger,
            backoff ?? Backoff,
            delay);

    /// <summary>A delay that never elapses on its own; it only ends (cancelled) with its token.</summary>
    private static Task UntilCancelled(CancellationToken ct)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ct.Register(() => tcs.TrySetCanceled(ct));
        return tcs.Task;
    }

    [Fact]
    public async Task StartAsync_WhenHostRunning_CallsConsumeOnce()
    {
        var consumer = new ScriptedConsumer();
        var host = CreateHost(consumer, new ScriptedProcessor());
        var service = CreateService(host, new CapturingLogger(), (_, ct) => UntilCancelled(ct));

        await service.StartAsync(CancellationToken.None);
        try
        {
            await consumer.FirstConsume.Task.WaitAsync(Safety);

            Assert.True(consumer.ConsumeCalls >= 1);
            Assert.Equal(ConsumerHostState.Running, host.State);
        }
        finally
        {
            consumer.Release();
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Loop_WhenHostNotRunning_DoesNotCallConsumeOnce()
    {
        var consumer = new ScriptedConsumer();
        var host = CreateHost(consumer, new ScriptedProcessor());
        var idleObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var service = CreateService(host, new CapturingLogger(), (span, ct) =>
        {
            if (span == TimeSpan.FromMilliseconds(500))
            {
                idleObserved.TrySetResult();
            }

            return UntilCancelled(ct);
        });

        await service.StartAsync(CancellationToken.None);
        await consumer.FirstConsume.Task.WaitAsync(Safety);

        // Take the host out of Running while the first poll is parked, then let that poll return.
        await host.StopAsync(CancellationToken.None);
        consumer.Release();
        await idleObserved.Task.WaitAsync(Safety);

        var callsWhileNotRunning = consumer.ConsumeCalls;

        // Restore Running so the hosted service can stop cleanly; the loop is parked in its idle delay.
        await host.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(1, callsWhileNotRunning);
        Assert.Equal(1, consumer.ConsumeCalls);
    }

    [Fact]
    public async Task Loop_WhenIterationThrowsRetainOffset_LogsCorrelationIdOnlyBacksOffAndRepolls()
    {
        var consumer = new ScriptedConsumer(Record());
        var processor = new ScriptedProcessor((_, _, attempt) =>
        {
            if (attempt == 1)
            {
                var ex = new RetainOffsetException("secret-payload-text");
                ex.Data[KafkaConsumerHostedService.CorrelationIdDataKey] = "corr-xyz";
                throw ex;
            }

            return Task.CompletedTask;
        });
        var host = CreateHost(consumer, processor);
        var logger = new CapturingLogger();
        var delays = new List<TimeSpan>();
        var service = CreateService(host, logger, (span, ct) =>
        {
            lock (delays)
            {
                delays.Add(span);
            }

            return span == Backoff ? Task.CompletedTask : UntilCancelled(ct);
        });

        await service.StartAsync(CancellationToken.None);
        try
        {
            await consumer.CommittedSignal.Task.WaitAsync(Safety);
        }
        finally
        {
            consumer.Release();
            await service.StopAsync(CancellationToken.None);
        }

        Assert.Equal(2, processor.Attempts);
        Assert.Single(consumer.Committed);
        lock (delays)
        {
            Assert.Contains(Backoff, delays);
        }

        var warnings = logger.Messages.Where(m => m.Level == LogLevel.Warning).ToList();
        var failure = Assert.Single(warnings);
        Assert.Contains("corr-xyz", failure.Text);
        Assert.Contains(nameof(RetainOffsetException), failure.Text);
        Assert.DoesNotContain("secret-payload-text", failure.Text);
    }

    [Fact]
    public async Task Loop_WhenFailureCarriesNoCorrelationId_LogsUnavailable()
    {
        var consumer = new ScriptedConsumer(Record());
        var processor = new ScriptedProcessor((_, _, attempt) =>
            attempt == 1 ? throw new RetainOffsetException() : Task.CompletedTask);
        var logger = new CapturingLogger();
        var service = CreateService(
            CreateHost(consumer, processor),
            logger,
            (span, ct) => span == Backoff ? Task.CompletedTask : UntilCancelled(ct));

        await service.StartAsync(CancellationToken.None);
        try
        {
            await consumer.CommittedSignal.Task.WaitAsync(Safety);
        }
        finally
        {
            consumer.Release();
            await service.StopAsync(CancellationToken.None);
        }

        Assert.Contains("unavailable", Assert.Single(logger.Messages, m => m.Level == LogLevel.Warning).Text);
    }

    [Theory]
    [InlineData(500)]
    [InlineData(60001)]
    public void Constructor_WithBackoffOutOfBounds_Throws(int backoffMs)
    {
        var host = CreateHost(new ScriptedConsumer(), new ScriptedProcessor());

        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaConsumerHostedService(
            host,
            new LifecycleConfig(),
            new CapturingLogger(),
            TimeSpan.FromMilliseconds(backoffMs)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(301)]
    public void Constructor_WithShutdownTimeoutOutOfBounds_Throws(int seconds)
    {
        var host = CreateHost(new ScriptedConsumer(), new ScriptedProcessor());

        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaConsumerHostedService(
            host,
            new LifecycleConfig { ShutdownTimeoutSeconds = seconds },
            new CapturingLogger()));
    }

    [Fact]
    public async Task StopAsync_WhileIterationInFlight_LetsItFinishAndCommit()
    {
        var consumer = new ScriptedConsumer(Record());
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processor = new ScriptedProcessor((_, _, _) => gate.Task);
        var host = CreateHost(consumer, processor);
        var service = CreateService(host, new CapturingLogger(), (_, ct) => UntilCancelled(ct));

        await service.StartAsync(CancellationToken.None);
        await processor.Entered.Task.WaitAsync(Safety);

        var stopTask = service.StopAsync(CancellationToken.None);

        // The record is mid-processing, so the drain cannot be complete and nothing is committed yet.
        Assert.False(stopTask.IsCompleted);
        Assert.Empty(consumer.Committed);

        gate.SetResult();
        consumer.Release();
        await stopTask.WaitAsync(Safety);

        Assert.Single(consumer.Committed);
        Assert.True(consumer.Closed);
        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StopAsync_WhenDrainExceedsTimeout_StopsWithoutCommittingAndLogs()
    {
        var consumer = new ScriptedConsumer(Record());
        // Blocks until the abandon token (cancelled only on drain timeout) fires.
        var processor = new ScriptedProcessor(async (_, ct, _) => await UntilCancelled(ct));
        var host = CreateHost(consumer, processor);
        var logger = new CapturingLogger();
        // The shutdown wait elapses immediately while the iteration is still blocked.
        var service = CreateService(
            host,
            logger,
            (span, ct) => span == ShutdownTimeout ? Task.CompletedTask : UntilCancelled(ct));

        await service.StartAsync(CancellationToken.None);
        await processor.Entered.Task.WaitAsync(Safety);

        await service.StopAsync(CancellationToken.None).WaitAsync(Safety);

        Assert.Empty(consumer.Committed);
        Assert.True(consumer.Closed);
        Assert.Equal(ConsumerHostState.Stopped, host.State);
        var warning = Assert.Single(logger.Messages, m => m.Level == LogLevel.Warning);
        Assert.Contains("shutdown timeout", warning.Text);
        Assert.Contains("30", warning.Text);
    }

    // ---- fakes -------------------------------------------------------------------------------

    private sealed class CapturingLogger : ILogger<KafkaConsumerHostedService>
    {
        private readonly List<(LogLevel Level, string Text)> _messages = new();

        public IReadOnlyList<(LogLevel Level, string Text)> Messages
        {
            get
            {
                lock (_messages)
                {
                    return _messages.ToList();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_messages)
            {
                _messages.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    /// <summary>
    /// Re-serves its record until committed; once nothing is pending it parks until <see cref="Release"/>,
    /// then returns empty polls. Never busy-spins before release.
    /// </summary>
    private sealed class ScriptedConsumer : IKafkaConsumer
    {
        private readonly KafkaConsumeResult? _record;
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _consumeCalls;
        private volatile bool _committed;

        public ScriptedConsumer(KafkaConsumeResult? record = null) => _record = record;

        public TaskCompletionSource FirstConsume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource CommittedSignal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<KafkaConsumeResult> Committed { get; } = new();

        public bool Closed { get; private set; }

        public int ConsumeCalls => Volatile.Read(ref _consumeCalls);

        public void Release() => _released.TrySetResult();

        public void Subscribe(string topic)
        {
        }

        public async Task<KafkaConsumeResult?> ConsumeAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _consumeCalls);
            FirstConsume.TrySetResult();

            if (_record is not null && !_committed)
            {
                return _record;
            }

            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await Task.Yield();
            return null;
        }

        public Task CommitAsync(KafkaConsumeResult result, CancellationToken cancellationToken)
        {
            lock (Committed)
            {
                Committed.Add(result);
            }

            _committed = true;
            CommittedSignal.TrySetResult();
            return Task.CompletedTask;
        }

        public void Close() => Closed = true;
    }

    private sealed class ScriptedProcessor : IStreamMessageProcessor
    {
        private readonly Func<MessageEnvelope, CancellationToken, int, Task> _behavior;
        private int _attempts;

        public ScriptedProcessor(Func<MessageEnvelope, CancellationToken, int, Task>? behavior = null) =>
            _behavior = behavior ?? ((_, _, _) => Task.CompletedTask);

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Attempts => Volatile.Read(ref _attempts);

        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref _attempts);
            Entered.TrySetResult();
            return _behavior(envelope, cancellationToken, attempt);
        }
    }
}
