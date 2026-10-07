using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Tests.Startup;

/// <summary>
/// A configurable <see cref="IMessagingProvider"/> test double for the DI registration and startup
/// tests. Unlike the resolution-only stub, this one exposes a usable publisher/consumer host and a
/// controllable <see cref="TransportHealth"/> so the readiness path and the pipeline wiring can be
/// exercised through a built <see cref="IServiceProvider"/>. It also lets a test declare required
/// provider settings so startup validation of provider-specific settings can be driven.
/// </summary>
internal sealed class TestMessagingProvider : IMessagingProvider
{
    private readonly TransportHealth _transportHealth;

    public TestMessagingProvider(
        MessagingMode mode,
        IReadOnlyCollection<ProviderSettingDescriptor>? requiredSettings = null,
        TransportHealth? transportHealth = null)
    {
        Mode = mode;
        RequiredSettings = requiredSettings ?? Array.Empty<ProviderSettingDescriptor>();
        _transportHealth = transportHealth ?? new TransportHealth(Reachable: true, UnavailableDependency: null);
        Publisher = new NoOpPublisher();
        ConsumerHost = new NoOpConsumerHost();
    }

    public MessagingMode Mode { get; }

    public IMessagePublisher Publisher { get; }

    public IMessageConsumerHost ConsumerHost { get; }

    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings { get; }

    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_transportHealth);

    private sealed class NoOpPublisher : IMessagePublisher
    {
        public Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken) =>
            Task.FromResult(PublishResult.Ok());
    }

    private sealed class NoOpConsumerHost : IMessageConsumerHost
    {
        public ConsumerHostState State => ConsumerHostState.Stopped;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
