using System.Globalization;
using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Demo.Communications;

/// <summary>Creates one mock quote email per accepted quote. No network I/O.</summary>
public sealed class SendQuoteEmailHandler : IMessageHandler<QuoteAcceptedEvent>
{
    public const int MaxSubjectLength = 200;
    public const int MaxBodyLength = 5_000;
    private const int MaxNameLength = 100;

    private readonly IMockEmailOutbox _outbox;
    private readonly IFlowStepSink _sink;
    private readonly TimeProvider _timeProvider;

    public SendQuoteEmailHandler(IMockEmailOutbox outbox, IFlowStepSink sink, TimeProvider timeProvider)
    {
        _outbox = outbox ?? throw new ArgumentNullException(nameof(outbox));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    public Task HandleAsync(MessageEnvelope envelope, QuoteAcceptedEvent message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var amount = message.QuoteAmount.ToString("F2", CultureInfo.InvariantCulture);
        var name = Truncate(message.CustomerName, MaxNameLength);

        var subject = Truncate($"Your quote of {amount} has been accepted", MaxSubjectLength);
        var body = Truncate(
            $"Dear {name}, your quote of {amount} has been accepted. Thank you for your business.",
            MaxBodyLength);

        _outbox.Add(new MockEmail(message.LeadId, message.CustomerEmail, subject, body));

        _sink.Emit(new FlowStep(
            MicroserviceId.Communications,
            FlowStepKind.EmailSent,
            envelope.CorrelationId,
            message.LeadId,
            _timeProvider.GetUtcNow()));

        return Task.CompletedTask;
    }

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..max];
}
