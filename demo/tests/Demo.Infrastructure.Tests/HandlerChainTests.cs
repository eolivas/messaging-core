using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Messaging;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Resilience;
using Xunit;

namespace Demo.Infrastructure.Tests;

public sealed class HandlerChainTests
{
    private static readonly QuoteAcceptedEvent Valid =
        new("lead-1", "Ann", "ann@example.com", 10.50m, DateTimeOffset.UnixEpoch);

    private sealed class Sink : IFlowStepSink
    {
        public List<FlowStep> Steps { get; } = new();
        public void Emit(FlowStep step) => Steps.Add(step);
    }

    private sealed class Real : IMessageHandler<QuoteAcceptedEvent>
    {
        public int Calls { get; private set; }
        public Task HandleAsync(MessageEnvelope e, QuoteAcceptedEvent m, CancellationToken ct)
        {
            Calls++;
            return Task.CompletedTask;
        }
    }

    private static MessageEnvelope Envelope(string correlationId) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = QuoteAcceptedEvent.MessageType,
        CreatedAtUtc = DateTimeOffset.UnixEpoch,
        Headers = new Dictionary<string, string>(),
        Payload = ReadOnlyMemory<byte>.Empty,
    };

    private static (IMessageHandler<QuoteAcceptedEvent> Chain, Real Real, Sink Sink, AttemptTracker Tracker)
        Build(Func<FailureMode> mode, int maxAttempts = 3)
    {
        var real = new Real();
        var sink = new Sink();
        var tracker = new AttemptTracker();
        var sim = new FailureSimulatingHandler(real, mode, tracker);
        var flow = new FlowReportingHandler(
            MicroserviceId.Customer, sim, new FailureClassifier(), tracker, sink, maxAttempts);
        return (flow, real, sink, tracker);
    }

    [Fact]
    public async Task Handle_ModeOff_EmitsConsumedAndCallsRealHandler()
    {
        var (chain, real, sink, _) = Build(() => FailureMode.Off);

        await chain.HandleAsync(Envelope("c1"), Valid, CancellationToken.None);

        Assert.Equal(1, real.Calls);
        Assert.Equal(FlowStepKind.Consumed, Assert.Single(sink.Steps).Kind);
    }

    [Fact]
    public async Task Handle_FailOnce_FailsTransientlyOnAttempt1ThenSucceeds()
    {
        var (chain, real, sink, tracker) = Build(() => FailureMode.FailOnce);
        var env = Envelope("c2");

        await Assert.ThrowsAsync<DownstreamUnavailableException>(
            () => chain.HandleAsync(env, Valid, CancellationToken.None));
        Assert.True(tracker.TryGet("c2", out var info));
        Assert.Equal(new AttemptInfo(1, "transient"), info);

        await chain.HandleAsync(env, Valid, CancellationToken.None);

        Assert.Equal(1, real.Calls);
        Assert.Contains(sink.Steps, s => s.Kind == FlowStepKind.Retrying && s.Attempt == 1);
    }

    [Fact]
    public async Task Handle_AlwaysFail_ThrowsNonTransientEveryAttemptWithoutRetrying()
    {
        var (chain, real, sink, tracker) = Build(() => FailureMode.AlwaysFail);
        var env = Envelope("c3");

        await Assert.ThrowsAsync<MessageValidationException>(
            () => chain.HandleAsync(env, Valid, CancellationToken.None));
        await Assert.ThrowsAsync<MessageValidationException>(
            () => chain.HandleAsync(env, Valid, CancellationToken.None));

        Assert.Equal(0, real.Calls);
        Assert.DoesNotContain(sink.Steps, s => s.Kind == FlowStepKind.Retrying);
        Assert.True(tracker.TryGet("c3", out var info));
        Assert.Equal(new AttemptInfo(2, "non-transient"), info);
    }

    [Fact]
    public async Task Handle_ModeChangedAfterAttempt1_KeepsCachedMode()
    {
        var mode = FailureMode.FailOnce;
        var (chain, real, _, _) = Build(() => mode);
        var env = Envelope("c4");

        await Assert.ThrowsAsync<DownstreamUnavailableException>(
            () => chain.HandleAsync(env, Valid, CancellationToken.None));
        mode = FailureMode.AlwaysFail;
        await chain.HandleAsync(env, Valid, CancellationToken.None);

        Assert.Equal(1, real.Calls);
    }

    [Fact]
    public async Task Handle_TransientOnLastAttempt_DoesNotEmitRetrying()
    {
        var (chain, _, sink, _) = Build(() => FailureMode.FailOnce, maxAttempts: 1);

        await Assert.ThrowsAsync<DownstreamUnavailableException>(
            () => chain.HandleAsync(Envelope("c5"), Valid, CancellationToken.None));

        Assert.DoesNotContain(sink.Steps, s => s.Kind == FlowStepKind.Retrying);
    }

    [Theory]
    [InlineData("", "n", "e@x.com", 1.0)]
    [InlineData("l", " ", "e@x.com", 1.0)]
    [InlineData("l", "n", "", 1.0)]
    [InlineData("l", "n", "e@x.com", 0.0)]
    [InlineData("l", "n", "e@x.com", 1000000.0)]
    public async Task Invoke_InvalidEvent_ThrowsBeforeAnyHandlerRuns(
        string lead, string name, string email, double amount)
    {
        var real = new Real();
        var sut = new QuoteAcceptedHandlerInvoker(real);
        var bad = new QuoteAcceptedEvent(lead, name, email, (decimal)amount, DateTimeOffset.UnixEpoch);

        await Assert.ThrowsAsync<MessageValidationException>(() => sut.InvokeAsync(
            Envelope("c6"), bad, new ConsumeContext(null, false), CancellationToken.None));

        Assert.Equal(0, real.Calls);
    }

    [Fact]
    public async Task Invoke_ValidEvent_CallsChain()
    {
        var real = new Real();
        var sut = new QuoteAcceptedHandlerInvoker(real);

        await sut.InvokeAsync(
            Envelope("c7"), Valid, new ConsumeContext(null, false), CancellationToken.None);

        Assert.Equal(1, real.Calls);
    }
}
