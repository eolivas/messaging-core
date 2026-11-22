using Demo.Contracts;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// The distinct, stable identity of a consuming microservice, so each one receives its own copy
/// of every event. Queue-based providers use <see cref="EndpointName"/>; Kafka uses
/// <see cref="ConsumerGroupId"/>.
/// </summary>
public sealed record ConsumerIdentity(MicroserviceId Service, string EndpointName, string ConsumerGroupId)
{
    public static ConsumerIdentity Customer { get; } =
        new(MicroserviceId.Customer, "customer-ms", "demo-customer-ms");

    public static ConsumerIdentity Communications { get; } =
        new(MicroserviceId.Communications, "communications-ms", "demo-communications-ms");
}
