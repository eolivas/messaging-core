namespace Messaging.MassTransit.Configuration;

/// <summary>
/// The underlying MassTransit transport the adapter is configured against. The broker-shaped modes
/// (Publish-Subscribe, Message Queue) all plug into the same adapter; the transport chooses the
/// concrete broker MassTransit talks to (design: "Transport is chosen by provider-specific settings").
/// </summary>
public enum MassTransitTransport
{
    /// <summary>RabbitMQ — the default broker for Publish-Subscribe fan-out.</summary>
    RabbitMq,

    /// <summary>Amazon SQS — point-to-point competing-consumer Message Queue delivery.</summary>
    AmazonSqs,

    /// <summary>Azure Service Bus — point-to-point competing-consumer Message Queue delivery.</summary>
    AzureServiceBus,

    /// <summary>In-memory transport — single-process testing without an external broker.</summary>
    InMemory,
}
