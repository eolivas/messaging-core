using Messaging.Abstractions;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Workaround for library finding F12. <c>AddMassTransitMessagingProvider</c> always builds
/// <c>MassTransitMessagingProvider</c> with an <see cref="IMessageConsumerHost"/>, falling back to
/// <c>MassTransitConsumerHost</c>, which needs a consume processor the library registers only for
/// consuming roles. A publisher-only host therefore cannot resolve <see cref="IMessagingProvider"/>.
/// Registered before the library call (which uses TryAdd), this host consumes nothing and stays
/// <see cref="ConsumerHostState.Stopped"/>.
/// </summary>
public sealed class PublisherOnlyConsumerHost : IMessageConsumerHost
{
    public ConsumerHostState State => ConsumerHostState.Stopped;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
