using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

/// <summary>
/// A minimal <see cref="IContractBinder"/> stub used only by the consumer host lifecycle tests
/// (task 9.5). The lifecycle sequence does not exercise contract binding, so this stub treats every
/// message type as known and returns the envelope's payload bytes unchanged. Named distinctly from
/// other consume-side doubles to avoid collisions with fakes added by sibling tasks.
/// </summary>
internal sealed class LifecycleStubContractBinder : IContractBinder
{
    public bool IsKnown(string messageType) => true;

    public object Bind(MessageEnvelope envelope) => envelope.Payload;
}
