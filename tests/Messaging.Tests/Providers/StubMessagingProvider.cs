using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;

namespace Messaging.Tests.Providers;

/// <summary>
/// Minimal <see cref="IMessagingProvider"/> stub used as test data for provider-resolution tests.
/// <see cref="ProviderRegistry"/> only reads <see cref="Mode"/>, so every other member throws
/// <see cref="NotSupportedException"/> to make any accidental use obvious. Each instance carries a
/// distinct identity so tests can assert reference-stable resolution.
/// </summary>
internal sealed class StubMessagingProvider : IMessagingProvider
{
    public StubMessagingProvider(MessagingMode mode)
    {
        Mode = mode;
    }

    public MessagingMode Mode { get; }

    public IMessagePublisher Publisher => throw new NotSupportedException(
        "StubMessagingProvider exposes no publisher; resolution tests read only Mode.");

    public IMessageConsumerHost ConsumerHost => throw new NotSupportedException(
        "StubMessagingProvider exposes no consumer host; resolution tests read only Mode.");

    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => Array.Empty<ProviderSettingDescriptor>();

    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "StubMessagingProvider performs no transport check; resolution tests read only Mode.");
}
