using Messaging.Abstractions;
namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Decorates a real dead-letter sink (the SQS sink) and records each message that was
/// delivered to it in an <see cref="InMemoryDeadLetterSink"/>, so the demo can show them.
/// A message is recorded only after the inner sink accepted it; an inner failure propagates
/// unchanged so the library's dead-letter routing sees it.
/// </summary>
public sealed class RecordingDeadLetterSink : IDeadLetterSink
{
    private readonly IDeadLetterSink _inner;
    private readonly InMemoryDeadLetterSink _recorder;

    public RecordingDeadLetterSink(IDeadLetterSink inner, InMemoryDeadLetterSink recorder)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _recorder = recorder ?? throw new ArgumentNullException(nameof(recorder));
    }

    public async Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await _inner.SendAsync(message, cancellationToken).ConfigureAwait(false);
        await _recorder.SendAsync(message, cancellationToken).ConfigureAwait(false);
    }
}
