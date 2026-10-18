using MassTransit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Transport;

/// <summary>
/// The production <see cref="IMassTransitBusAdapter"/> over a configured MassTransit RabbitMQ
/// <see cref="IBusControl"/>. It starts and stops the bus, shapes a publish into either a fan-out
/// publish (Publish-Subscribe) or a point-to-point send (Message Queue) per the active
/// <see cref="MessagingMode"/>, and answers a bounded reachability probe. No core policy (retry,
/// idempotency, binding, dead-letter) lives here — that is owned by the consume seam — so this type
/// stays a thin transport boundary (SRP).
/// </summary>
/// <remarks>
/// <para>
/// <b>Role-scoped topology (Req 4.2, 4.3, 4.5).</b> The bus is built from the provider settings and a
/// <see cref="MassTransitRole"/>. A <see cref="MassTransitRole.Publisher"/> build registers
/// <em>no</em> receive endpoint; a <see cref="MassTransitRole.Consumer"/> or
/// <see cref="MassTransitRole.Both"/> build registers exactly one receive endpoint hosting the
/// <see cref="EnvelopeConsumer"/>.
/// </para>
/// <para>
/// <b>No transport-level redelivery (Req 8.8).</b> The receive endpoint disables MassTransit-level
/// redelivery with <c>UseMessageRetry(r =&gt; r.None())</c> so the core <c>RetryExecutor</c> owns the
/// single attempt bound; the consume processor returns to acknowledge and throws to retain.
/// </para>
/// <para>
/// <b>Secrets.</b> The secret-bearing connection string is never logged; diagnostics reference the
/// broker only by its non-secret host display name (Req 9.5).
/// </para>
/// </remarks>
public sealed class RabbitMqBusAdapter : IMassTransitBusAdapter, IAsyncDisposable
{
    /// <summary>
    /// The default receive queue name used when a caller does not supply one. Stable so a restarted
    /// instance rebinds to the same queue rather than orphaning messages on a fresh one.
    /// </summary>
    public const string DefaultReceiveQueueName = "messaging-core";

    private static readonly TimeSpan ReachabilityBudget = TimeSpan.FromSeconds(1);

    private readonly IBusControl _bus;
    private readonly MessagingMode _mode;
    private readonly string _receiveQueueName;
    private readonly string _host;
    private readonly ObservabilityContext _observability;
    private readonly ILogger<RabbitMqBusAdapter> _logger;

    private volatile bool _started;

    // Internal (not private) so the adapter's seam operations can be unit tested against a fake
    // IBusControl without a live broker (Req 1.5, 1.6, 5.5, 10.x). The public Create factory remains
    // the only supported production entry point; this seam injects an already-constructed bus.
    internal RabbitMqBusAdapter(
        IBusControl bus,
        MessagingMode mode,
        string receiveQueueName,
        string host,
        ObservabilityContext observability,
        ILogger<RabbitMqBusAdapter> logger)
    {
        _bus = bus;
        _mode = mode;
        _receiveQueueName = receiveQueueName;
        _host = host;
        _observability = observability;
        _logger = logger;
    }

    /// <summary>
    /// Builds a MassTransit RabbitMQ bus from the provider settings and role, registering the
    /// <see cref="EnvelopeConsumer"/> on exactly one receive endpoint for a consuming role and no
    /// receive endpoint for a publisher-only role.
    /// </summary>
    /// <param name="settings">
    /// The provider settings carrying the AMQP(S) connection string (secret-bearing) and the
    /// non-secret host display name used in diagnostics.
    /// </param>
    /// <param name="role">
    /// The role driving endpoint registration: <see cref="MassTransitRole.Publisher"/> registers none;
    /// <see cref="MassTransitRole.Consumer"/>/<see cref="MassTransitRole.Both"/> register exactly one.
    /// </param>
    /// <param name="mode">
    /// The active messaging mode shaping <see cref="PublishAsync"/>:
    /// <see cref="MessagingMode.PublishSubscribe"/> publishes (fan-out);
    /// <see cref="MessagingMode.MessageQueue"/> sends point-to-point.
    /// </param>
    /// <param name="processor">
    /// The core consume-steps seam hosted by the receive endpoint. Required for a consuming role;
    /// ignored (and permitted to be <c>null</c>) for <see cref="MassTransitRole.Publisher"/>.
    /// </param>
    /// <param name="observability">The observability facade applied around each publish (Req 9.1, 9.7).</param>
    /// <param name="logger">Structured logger for redacted bus-state diagnostics (Req 9.3, 9.4).</param>
    /// <param name="receiveQueueName">
    /// The receive queue name for a consuming role; defaults to <see cref="DefaultReceiveQueueName"/>
    /// when not supplied.
    /// </param>
    /// <returns>The constructed adapter over a not-yet-started bus.</returns>
    /// <exception cref="ArgumentNullException">
    /// If <paramref name="settings"/>, <paramref name="observability"/>, or <paramref name="logger"/>
    /// is <c>null</c>, or if a consuming role is requested without a <paramref name="processor"/>.
    /// </exception>
    public static RabbitMqBusAdapter Create(
        MassTransitProviderSettings settings,
        MassTransitRole role,
        MessagingMode mode,
        IMassTransitConsumeProcessor? processor,
        ObservabilityContext observability,
        ILogger<RabbitMqBusAdapter> logger,
        string? receiveQueueName = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(observability);
        ArgumentNullException.ThrowIfNull(logger);

        var queueName = string.IsNullOrWhiteSpace(receiveQueueName)
            ? DefaultReceiveQueueName
            : receiveQueueName!;

        var registersConsumer = role is MassTransitRole.Consumer or MassTransitRole.Both;
        if (registersConsumer && processor is null)
        {
            // A consuming role must host the core consume seam; fail fast rather than build a bus
            // with a receive endpoint wired to a null processor (DIP).
            throw new ArgumentNullException(
                nameof(processor),
                $"A '{nameof(IMassTransitConsumeProcessor)}' is required for role '{role}'.");
        }

        var bus = Bus.Factory.CreateUsingRabbitMq(cfg =>
        {
            cfg.Host(new Uri(settings.ConnectionString));

            if (registersConsumer)
            {
                cfg.ReceiveEndpoint(queueName, endpoint =>
                {
                    // No MassTransit-level redelivery: the core RetryExecutor owns the single attempt
                    // bound, so a throw maps to exactly one broker requeue, not a multiplied count (Req 8.8).
                    endpoint.UseMessageRetry(r => r.None());
                    endpoint.Consumer(() => new EnvelopeConsumer(processor!));
                });
            }
        });

        return new RabbitMqBusAdapter(bus, mode, queueName, settings.Host, observability, logger);
    }

    /// <inheritdoc />
    public async Task StartBusAsync(CancellationToken cancellationToken)
    {
        // On failure do not set started: StartAsync throwing leaves the adapter stopped (Req 5.5).
        await _bus.StartAsync(cancellationToken).ConfigureAwait(false);
        _started = true;

        _logger.LogInformation(
            "MassTransit RabbitMQ bus {BusState} for broker {Host}.",
            "started",
            _host);
    }

    /// <inheritdoc />
    public async Task StopBusAsync(CancellationToken cancellationToken)
    {
        // Draining in-flight consumers is MassTransit's responsibility on StopAsync (Req 5.2).
        await _bus.StopAsync(cancellationToken).ConfigureAwait(false);
        _started = false;

        _logger.LogInformation(
            "MassTransit RabbitMQ bus {BusState} for broker {Host}.",
            "stopped",
            _host);
    }

    /// <inheritdoc />
    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var message = EnvelopeMessage.FromEnvelope(envelope);

        // Record the publish event and obtain the trace/correlation headers to propagate. The
        // activity (if any) is disposed when the publish completes (Req 9.1).
        using var activity = _observability.OnPublish(envelope.CorrelationId, out var traceHeaders);

        if (_mode == MessagingMode.MessageQueue)
        {
            var sendEndpoint = await _bus
                .GetSendEndpoint(BuildSendEndpointUri())
                .ConfigureAwait(false);

            await sendEndpoint
                .Send(message, sendContext => ApplyTraceHeaders(sendContext, traceHeaders), cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        // Default to the Publish-Subscribe fan-out shape.
        await _bus
            .Publish(message, publishContext => ApplyTraceHeaders(publishContext, traceHeaders), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> IsTransportReachableAsync(CancellationToken cancellationToken)
    {
        // Not started means nothing to reach; answer false without touching the broker (Req 10.5).
        if (!_started)
        {
            return false;
        }

        try
        {
            var checkTask = Task.Run(
                () => _bus.CheckHealth().Status == BusHealthStatus.Healthy,
                cancellationToken);
            var timeoutTask = Task.Delay(ReachabilityBudget, cancellationToken);

            var winner = await Task.WhenAny(checkTask, timeoutTask).ConfigureAwait(false);
            if (winner != checkTask)
            {
                // The timeout won: treat an unanswered check as not reachable (Req 10.4).
                return false;
            }

            return await checkTask.ConfigureAwait(false);
        }
        catch
        {
            // Never propagate a probe failure to the readiness caller (Req 10.3).
            return false;
        }
    }

    /// <summary>
    /// Stops the bus on disposal if it is still started, so a leaked adapter does not leave a live
    /// broker connection dangling.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            await _bus.StopAsync(CancellationToken.None).ConfigureAwait(false);
            _started = false;
        }
    }

    /// <summary>
    /// Derives the point-to-point send endpoint URI for <see cref="MessagingMode.MessageQueue"/> from
    /// the configured receive queue name, as MassTransit's short-form <c>queue:name</c> address it
    /// resolves against the configured host. The <c>queue:</c> prefix makes this a scheme-qualified
    /// (absolute) URI, so it must be built as <see cref="UriKind.Absolute"/>; building it as relative
    /// throws a <see cref="UriFormatException"/> because the string already carries a scheme.
    /// </summary>
    private Uri BuildSendEndpointUri() => new($"queue:{_receiveQueueName}", UriKind.Absolute);

    /// <summary>
    /// Merges the trace/correlation headers onto the outgoing message's transport context, delegating
    /// to <see cref="MergeTraceHeaders"/> so the publish and send paths share one merge step.
    /// </summary>
    private static void ApplyTraceHeaders(SendContext context, IReadOnlyDictionary<string, string> traceHeaders) =>
        MergeTraceHeaders(context.Headers, traceHeaders);

    /// <summary>
    /// Copies every trace/correlation header produced for a publish onto the outgoing message's
    /// transport <see cref="SendHeaders"/> verbatim: each key is set to its exact string value, so the
    /// correlation-id entry handed to the transport is character-for-character equal to the value the
    /// observability facade emitted for the envelope (Req 6.4, 9.7). This is the sole header-merge step
    /// on the publish/send path; it performs no transformation, truncation, or re-encoding of a value,
    /// which is what the end-to-end correlation guarantee relies on.
    /// </summary>
    /// <param name="headers">The outgoing message's transport header sink to merge into.</param>
    /// <param name="traceHeaders">
    /// The trace/correlation headers to apply, as returned by the observability facade on publish;
    /// always carries at least the correlation id.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// If <paramref name="headers"/> or <paramref name="traceHeaders"/> is <c>null</c>.
    /// </exception>
    public static void MergeTraceHeaders(SendHeaders headers, IReadOnlyDictionary<string, string> traceHeaders)
    {
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(traceHeaders);

        foreach (var header in traceHeaders)
        {
            headers.Set(header.Key, header.Value);
        }
    }
}
