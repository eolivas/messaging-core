using Demo.Application.Sessions;
using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Simulates consumer failures. The mode is read on attempt 1 and cached per correlation id,
/// so a mode change affects only events first delivered afterwards (Req 10.6).
/// <see cref="FailureMode.FailOnce"/> throws a transient failure on attempt 1 only;
/// <see cref="FailureMode.AlwaysFail"/> throws a non-transient failure on every attempt.
/// </summary>
public sealed class FailureSimulatingHandler : IMessageHandler<QuoteAcceptedEvent>
{
    public const int ModeCacheCapacity = 1_000;

    private readonly IMessageHandler<QuoteAcceptedEvent> _inner;
    private readonly Func<FailureMode> _modeSource;
    private readonly AttemptTracker _tracker;
    private readonly object _gate = new();
    private readonly Dictionary<string, FailureMode> _modes = new();
    private readonly Queue<string> _order = new();

    public FailureSimulatingHandler(
        IMessageHandler<QuoteAcceptedEvent> inner,
        Func<FailureMode> modeSource,
        AttemptTracker tracker)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _modeSource = modeSource ?? throw new ArgumentNullException(nameof(modeSource));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
    }

    public Task HandleAsync(
        MessageEnvelope envelope,
        QuoteAcceptedEvent message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var attempt = _tracker.NextAttempt(envelope.CorrelationId);
        var mode = ResolveMode(envelope.CorrelationId, attempt);

        if (mode == FailureMode.FailOnce && attempt == 1)
        {
            throw new DownstreamUnavailableException("Simulated transient failure (fail once).");
        }
        if (mode == FailureMode.AlwaysFail)
        {
            throw new MessageValidationException("Simulated non-transient failure (always fail).");
        }
        return _inner.HandleAsync(envelope, message, cancellationToken);
    }

    private FailureMode ResolveMode(string correlationId, int attempt)
    {
        lock (_gate)
        {
            if (attempt > 1 && _modes.TryGetValue(correlationId, out var cached))
            {
                return cached;
            }
            var mode = _modeSource();
            if (!_modes.ContainsKey(correlationId))
            {
                _order.Enqueue(correlationId);
            }
            _modes[correlationId] = mode;
            while (_modes.Count > ModeCacheCapacity && _order.TryDequeue(out var oldest))
            {
                _modes.Remove(oldest);
            }
            return mode;
        }
    }
}
