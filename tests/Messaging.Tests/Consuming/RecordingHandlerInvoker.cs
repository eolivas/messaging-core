using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

/// <summary>
/// Test double for the consume pipeline: an <see cref="IHandlerInvoker"/> that records the
/// <see cref="MessageEnvelope.CorrelationId"/> of every envelope handed to it at the end of the
/// consume pipeline. End-to-end correlation-id propagation tests (Property 7) assert that the
/// correlation id observed here equals the one submitted at publish, so the invoker captures exactly
/// what the handler would see without needing a real registered handler. Only the correlation id is
/// retained; the bound message is intentionally ignored.
/// </summary>
internal sealed class RecordingHandlerInvoker : IHandlerInvoker
{
    private readonly List<string> _observedCorrelationIds = new();

    /// <summary>The correlation ids observed by the handler, in invocation order.</summary>
    public IReadOnlyList<string> ObservedCorrelationIds => _observedCorrelationIds;

    public Task InvokeAsync(
        MessageEnvelope envelope,
        object boundMessage,
        ConsumeContext context,
        CancellationToken cancellationToken)
    {
        _observedCorrelationIds.Add(envelope.CorrelationId);
        return Task.CompletedTask;
    }
}
