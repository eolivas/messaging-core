using System.Text.Json;
using Demo.Contracts;
using Demo.SalesLeads.Domain;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Demo.SalesLeads.Application;

/// <inheritdoc />
public sealed class SalesLeadsService : ISalesLeadsService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IMessagePublisher _publisher;
    private readonly IMockDataGenerator _generator;
    private readonly IFlowStepSink _sink;
    private readonly TimeProvider _clock;

    public SalesLeadsService(
        IMessagePublisher publisher,
        IMockDataGenerator generator,
        IFlowStepSink sink,
        TimeProvider clock)
    {
        _publisher = publisher;
        _generator = generator;
        _sink = sink;
        _clock = clock;
    }

    public async Task<AcceptQuoteResult> AcceptQuoteAsync(CancellationToken cancellationToken)
    {
        var lead = new SalesLead(
            Guid.NewGuid().ToString("N"),
            _generator.GenerateName(),
            _generator.GenerateEmail(),
            _generator.GenerateAmount());

        string correlationId = Guid.NewGuid().ToString("N");
        DateTimeOffset now = _clock.GetUtcNow();

        var payload = new QuoteAcceptedEvent(
            lead.LeadId, lead.CustomerName, lead.CustomerEmail, lead.QuoteAmount, now);

        PublishResult result;
        try
        {
            var envelope = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = correlationId,
                MessageType = QuoteAcceptedEvent.MessageType,
                CreatedAtUtc = now,
                Headers = new Dictionary<string, string>(),
                Payload = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions),
            };

            result = await _publisher.PublishAsync(envelope, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is MessagingFailureException
                                       or InvalidEnvelopeException
                                       or TimeoutException
                                       or IOException
                                       or HttpRequestException)
        {
            // Narrow transport failures only; cancellation and programming errors propagate.
            return Fail(lead, correlationId, ex.Message);
        }

        if (!result.Succeeded)
        {
            return Fail(lead, correlationId, result.Error);
        }

        lead.MarkPublished();
        _sink.Emit(new FlowStep(
            MicroserviceId.SalesLeads, FlowStepKind.Published, correlationId, lead.LeadId, _clock.GetUtcNow()));
        return new AcceptQuoteResult(true, correlationId, lead.LeadId, null);
    }

    private AcceptQuoteResult Fail(SalesLead lead, string correlationId, string? rawReason)
    {
        // The lead is discarded: it was never stored, so dropping the reference is enough (Req 6.7).
        string reason = ReasonSanitizer.Sanitize(rawReason);
        _sink.Emit(new FlowStep(
            MicroserviceId.SalesLeads, FlowStepKind.Failed, correlationId, lead.LeadId,
            _clock.GetUtcNow(), Reason: reason));
        return new AcceptQuoteResult(false, correlationId, lead.LeadId, reason);
    }
}
