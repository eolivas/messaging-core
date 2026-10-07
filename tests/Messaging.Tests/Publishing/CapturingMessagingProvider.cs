using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Tests.Publishing;

/// <summary>
/// Test double for the publish pipeline: an <see cref="IMessagingProvider"/> whose publisher
/// captures every delivered <see cref="MessageEnvelope"/> and returns a configurable
/// <see cref="PublishResult"/>. Lets tests assert both that delivery happened (and with which
/// envelope) and that an invalid envelope is never delivered. Only the publisher is exercised;
/// other members throw to surface accidental use.
/// </summary>
internal sealed class CapturingMessagingProvider : IMessagingProvider
{
    private readonly CapturingPublisher _publisher;

    public CapturingMessagingProvider(MessagingMode mode, PublishResult? result = null)
    {
        Mode = mode;
        _publisher = new CapturingPublisher(result ?? PublishResult.Ok());
    }

    public MessagingMode Mode { get; }

    public IMessagePublisher Publisher => _publisher;

    /// <summary>The envelopes handed to the publisher, in delivery order.</summary>
    public IReadOnlyList<MessageEnvelope> DeliveredEnvelopes => _publisher.Delivered;

    public IMessageConsumerHost ConsumerHost => throw new NotSupportedException(
        "CapturingMessagingProvider exposes no consumer host; publish-pipeline tests use only the publisher.");

    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => Array.Empty<ProviderSettingDescriptor>();

    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "CapturingMessagingProvider performs no transport check; publish-pipeline tests use only the publisher.");

    private sealed class CapturingPublisher : IMessagePublisher
    {
        private readonly List<MessageEnvelope> _delivered = new();
        private readonly PublishResult _result;

        public CapturingPublisher(PublishResult result)
        {
            _result = result;
        }

        public IReadOnlyList<MessageEnvelope> Delivered => _delivered;

        public Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            _delivered.Add(envelope);
            return Task.FromResult(_result);
        }
    }
}
