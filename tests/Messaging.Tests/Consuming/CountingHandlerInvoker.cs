using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

/// <summary>
/// Test double for the consume pipeline: an <see cref="IHandlerInvoker"/> that records every
/// invocation instead of dispatching to a real handler. Lets a test assert both that the handler
/// was invoked (and with which bound message / envelope) and — the case this file exists for — that
/// the handler is NEVER invoked when the pipeline rejects an envelope before the consumer.
/// </summary>
/// <remarks>
/// Mirrors the style of <c>CapturingMessagingProvider</c> on the publish side: a trivial, honest
/// test double that captures real calls rather than mocking behavior. One public type per file.
/// </remarks>
internal sealed class CountingHandlerInvoker : IHandlerInvoker
{
    private readonly List<(MessageEnvelope Envelope, object BoundMessage)> _invocations = new();

    /// <summary>The number of times the handler was invoked.</summary>
    public int InvocationCount => _invocations.Count;

    /// <summary>Every (envelope, bound message) pair handed to the invoker, in invocation order.</summary>
    public IReadOnlyList<(MessageEnvelope Envelope, object BoundMessage)> Invocations => _invocations;

    public Task InvokeAsync(
        MessageEnvelope envelope,
        object boundMessage,
        ConsumeContext context,
        CancellationToken cancellationToken)
    {
        _invocations.Add((envelope, boundMessage));
        return Task.CompletedTask;
    }
}
