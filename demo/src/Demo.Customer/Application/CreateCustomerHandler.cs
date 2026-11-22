using Demo.Contracts;
using Demo.Customer.Domain;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Demo.Customer.Application;

/// <summary>Creates a mock customer from a <see cref="QuoteAcceptedEvent"/>.</summary>
public sealed class CreateCustomerHandler : IMessageHandler<QuoteAcceptedEvent>
{
    private readonly ICustomerStore _store;
    private readonly IFlowStepSink _sink;
    private readonly TimeProvider _time;

    public CreateCustomerHandler(ICustomerStore store, IFlowStepSink sink, TimeProvider time)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _time = time ?? throw new ArgumentNullException(nameof(time));
    }

    public Task HandleAsync(
        MessageEnvelope envelope,
        QuoteAcceptedEvent message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        // All checks are done above; the customer is created in a single atomic step.
        var now = _time.GetUtcNow();
        var customer = new MockCustomer(message.LeadId, message.CustomerName, message.CustomerEmail, now);

        if (_store.TryAdd(customer))
        {
            _sink.Emit(new FlowStep(
                MicroserviceId.Customer,
                FlowStepKind.CustomerCreated,
                envelope.CorrelationId,
                message.LeadId,
                now));
        }

        return Task.CompletedTask;
    }
}
