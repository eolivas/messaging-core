namespace Messaging.Tests.Consuming;

/// <summary>
/// A minimal, JSON-serializable contract DTO used only by the end-to-end correlation-id propagation
/// property test (Property 7). Generated payloads serialize an instance of this type so the consume
/// pipeline's bind step succeeds against a registered contract, letting the test exercise the full
/// publish → delivery → consume path. The field carries no behavior; it exists so the payload has a
/// recognizable, bindable shape.
/// </summary>
public sealed class CorrelationProbeContract
{
    /// <summary>An arbitrary payload value so generated instances differ; irrelevant to the property.</summary>
    public int Value { get; set; }
}
