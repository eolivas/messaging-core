using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Validates the bound <see cref="QuoteAcceptedEvent"/> and then calls the handler chain.
/// An invalid event throws a non-transient <see cref="MessageValidationException"/> before
/// any handler runs, so it dead-letters at attempt 1 (Req 7.6, 8.1).
/// </summary>
public sealed class QuoteAcceptedHandlerInvoker : IHandlerInvoker
{
    public const decimal MinAmount = 0.01m;
    public const decimal MaxAmount = 999_999.99m;

    private readonly IMessageHandler<QuoteAcceptedEvent> _chain;

    public QuoteAcceptedHandlerInvoker(IMessageHandler<QuoteAcceptedEvent> chain)
    {
        _chain = chain ?? throw new ArgumentNullException(nameof(chain));
    }

    public Task InvokeAsync(
        MessageEnvelope envelope,
        object boundMessage,
        ConsumeContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(boundMessage);

        if (boundMessage is not QuoteAcceptedEvent message)
        {
            throw new MessageValidationException(
                $"Unexpected message contract {boundMessage.GetType().Name}.");
        }

        Validate(message);
        return _chain.HandleAsync(envelope, message, cancellationToken);
    }

    private static void Validate(QuoteAcceptedEvent message)
    {
        if (string.IsNullOrWhiteSpace(message.LeadId))
        {
            throw new MessageValidationException("LeadId must not be empty.");
        }
        if (string.IsNullOrWhiteSpace(message.CustomerName))
        {
            throw new MessageValidationException("CustomerName must not be empty.");
        }
        if (string.IsNullOrWhiteSpace(message.CustomerEmail))
        {
            throw new MessageValidationException("CustomerEmail must not be empty.");
        }
        if (message.QuoteAmount < MinAmount || message.QuoteAmount > MaxAmount)
        {
            throw new MessageValidationException(
                $"QuoteAmount must be between {MinAmount} and {MaxAmount}.");
        }
    }
}
