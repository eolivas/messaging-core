using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Consuming;

namespace Messaging.Tests.Fakes;

/// <summary>
/// A complete in-memory <see cref="IMessagingProvider"/> for exercising the core publish and consume
/// pipelines in isolation — no RabbitMQ/Kafka/batch transport required (task 10). It composes a
/// <see cref="FakePublisher"/> and a <see cref="FakeConsumerHost"/>: a confirmed publish is looped
/// straight into the consumer host, so a test can drive a message from the publish pipeline all the
/// way to a consumer handler through an in-memory round trip.
/// </summary>
/// <remarks>
/// <para>
/// This is the single plug point the ports-and-adapters design expects: the registry resolves it by
/// <see cref="Mode"/> and the publish pipeline delegates to <see cref="Publisher"/>, exactly as it
/// would a real provider (Req 3.1, 3.4). The consume side wraps a real <see cref="ConsumePipeline"/>,
/// so contract binding, lifecycle, and handler invocation all run for real; only the transport is
/// faked.
/// </para>
/// <para>
/// What it captures and simulates:
/// </para>
/// <list type="bullet">
///   <item><description><b>Published envelopes</b> via <see cref="FakePublisher.PublishedEnvelopes"/>.</description></item>
///   <item><description><b>Delivered envelopes</b> via <see cref="FakeConsumerHost.DeliveredEnvelopes"/>.</description></item>
///   <item><description><b>Confirm/fail outcomes</b> via <see cref="FakePublisher.Outcome"/> so both the success and transport-failure paths are reachable.</description></item>
/// </list>
/// <para>
/// Build it two ways: <see cref="CreateWithLoopback"/> for a publish → consume round trip against a
/// real consume pipeline, or the constructor with a null consumer host for a publish-only capture
/// target. <see cref="RequiredSettings"/> is empty and <see cref="CheckTransportAsync"/> reports a
/// configurable reachability so lifecycle/readiness code (task 18) can be exercised too.
/// </para>
/// </remarks>
public sealed class FakeMessagingProvider : IMessagingProvider
{
    private readonly FakePublisher _publisher;
    private readonly FakeConsumerHost? _consumerHost;
    private TransportHealth _transportHealth = new(Reachable: true, UnavailableDependency: null);

    private FakeMessagingProvider(
        MessagingMode mode,
        FakePublisher publisher,
        FakeConsumerHost? consumerHost)
    {
        Mode = mode;
        _publisher = publisher;
        _consumerHost = consumerHost;
    }

    /// <summary>
    /// Creates a fake provider wired as a full publish → consume loopback over a real consume
    /// pipeline. A confirmed publish is delivered straight to the consumer host, so a test can assert
    /// what the handler received without a transport.
    /// </summary>
    /// <param name="pipeline">The real Application-layer consume pipeline the consumer host drives.</param>
    /// <param name="mode">The mode this provider serves (defaults to <see cref="MessagingMode.PublishSubscribe"/>).</param>
    /// <param name="outcome">The initial publish outcome (defaults to confirm).</param>
    /// <returns>The composed provider.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="pipeline"/> is null.</exception>
    public static FakeMessagingProvider CreateWithLoopback(
        ConsumePipeline pipeline,
        MessagingMode mode = MessagingMode.PublishSubscribe,
        FakePublishOutcome outcome = FakePublishOutcome.Confirm)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        var consumerHost = new FakeConsumerHost(pipeline);
        var publisher = new FakePublisher(consumerHost.DeliverAsync, outcome);
        return new FakeMessagingProvider(mode, publisher, consumerHost);
    }

    /// <summary>
    /// Creates a fake provider that only captures and confirms/fails publishes, with no consume side
    /// (publish-only pipeline tests). Accessing <see cref="ConsumerHost"/> throws.
    /// </summary>
    /// <param name="mode">The mode this provider serves (defaults to <see cref="MessagingMode.PublishSubscribe"/>).</param>
    /// <param name="outcome">The initial publish outcome (defaults to confirm).</param>
    /// <returns>The composed provider.</returns>
    public static FakeMessagingProvider CreatePublishOnly(
        MessagingMode mode = MessagingMode.PublishSubscribe,
        FakePublishOutcome outcome = FakePublishOutcome.Confirm)
    {
        var publisher = new FakePublisher(deliverToConsumer: null, outcome: outcome);
        return new FakeMessagingProvider(mode, publisher, consumerHost: null);
    }

    /// <inheritdoc />
    public MessagingMode Mode { get; }

    /// <inheritdoc />
    public IMessagePublisher Publisher => _publisher;

    /// <summary>The fake publisher, typed for test inspection and outcome control.</summary>
    public FakePublisher FakePublisher => _publisher;

    /// <inheritdoc />
    /// <exception cref="NotSupportedException">If this provider was created publish-only.</exception>
    public IMessageConsumerHost ConsumerHost => _consumerHost
        ?? throw new NotSupportedException(
            "This FakeMessagingProvider was created publish-only; use CreateWithLoopback for a consumer host.");

    /// <summary>The fake consumer host, typed for test inspection; null when publish-only.</summary>
    public FakeConsumerHost? FakeConsumerHost => _consumerHost;

    /// <inheritdoc />
    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => Array.Empty<ProviderSettingDescriptor>();

    /// <summary>
    /// Sets the <see cref="TransportHealth"/> that <see cref="CheckTransportAsync"/> reports, so a
    /// test can drive both the reachable and the unavailable readiness paths (Req 10.3, 10.4).
    /// </summary>
    /// <param name="health">The health to report.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="health"/> is null.</exception>
    public void SetTransportHealth(TransportHealth health)
    {
        ArgumentNullException.ThrowIfNull(health);
        _transportHealth = health;
    }

    /// <inheritdoc />
    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_transportHealth);
}
