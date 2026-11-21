using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SimpleNotificationService;
using MassTransit;
using MassTransit.Middleware;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// The production <see cref="IMassTransitBusAdapter"/> over a MassTransit Amazon SQS
/// <see cref="IBusControl"/>. It provisions topology through <see cref="SnsSqsTopologyManager"/>, starts
/// and stops the bus, and (task 10.3) publishes to SNS topics or SQS queues, sends dead letters, and
/// answers the bounded reachability probe.
/// </summary>
/// <remarks>
/// <para>
/// MassTransit owns message I/O only. The receive endpoint is built with
/// <c>ConfigureConsumeTopology = false</c> so the bus performs lookups against the resources the
/// topology manager provisioned (spike R1), uses no MassTransit retry (the core <c>RetryExecutor</c>
/// owns retry), and replaces the error pipe with <see cref="RethrowErrorTransportFilter"/> so a failed
/// message is never deleted (spike R3).
/// </para>
/// <para>
/// A <see cref="MassTransitRole.Publisher"/> build registers no receive endpoint and no queue.
/// Secrets (keys, session token, service URL) are never logged.
/// </para>
/// </remarks>
public sealed class AmazonSqsBusAdapter : IMassTransitBusAdapter, IDeadLetterQueueSender, IAsyncDisposable
{
    private static readonly TimeSpan ReachabilityBudget = TimeSpan.FromSeconds(1);

    private readonly IBusControl _bus;
    private readonly AmazonSqsProviderSettings _settings;
    private readonly MessagingMode _mode;
    private readonly MassTransitRole _role;
    private readonly IReadOnlyList<ContractRegistration> _contracts;
    private readonly SnsSqsTopologyManager _topology;
    private readonly ISnsSqsTopologyClient _topologyClient;
    private readonly ObservabilityContext _observability;
    private readonly AmazonSqsMetrics _metrics;
    private readonly InFlightTracker _tracker;
    private readonly TimeSpan _shutdownTimeout;
    private readonly ILogger<AmazonSqsBusAdapter> _logger;

    private volatile bool _started;

    // Internal so unit tests can inject a fake IBusControl; Create is the production entry point.
    internal AmazonSqsBusAdapter(
        IBusControl bus,
        AmazonSqsProviderSettings settings,
        MessagingMode mode,
        MassTransitRole role,
        IEnumerable<ContractRegistration> contracts,
        SnsSqsTopologyManager topology,
        ISnsSqsTopologyClient topologyClient,
        ObservabilityContext observability,
        AmazonSqsMetrics metrics,
        InFlightTracker tracker,
        int shutdownTimeoutSeconds,
        ILogger<AmazonSqsBusAdapter> logger)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _mode = mode;
        _role = role;
        _contracts = (contracts ?? throw new ArgumentNullException(nameof(contracts))).ToArray();
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _topologyClient = topologyClient ?? throw new ArgumentNullException(nameof(topologyClient));
        _observability = observability ?? throw new ArgumentNullException(nameof(observability));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var bounds = ConfigurationBounds.ShutdownTimeoutSeconds;
        _shutdownTimeout = TimeSpan.FromSeconds(Math.Clamp(shutdownTimeoutSeconds, bounds.Min, bounds.Max));
    }

    // Test seam: lets unit tests inspect the bus that Create built (receive endpoint settings).
    internal IBusControl BusControl => _bus;

    /// <summary>The outcome of the most recent <see cref="StopBusAsync"/>, or <see langword="null"/> before any stop.</summary>
    public ShutdownResult? LastShutdownResult { get; private set; }

    /// <summary>
    /// Builds a MassTransit Amazon SQS bus from the settings and role. A consuming role registers
    /// exactly one receive endpoint hosting <see cref="SqsEnvelopeConsumer"/>; a publisher role registers none.
    /// </summary>
    /// <param name="settings">The Amazon SQS provider settings (secret-bearing; never logged).</param>
    /// <param name="role">The bus role.</param>
    /// <param name="mode">The messaging mode.</param>
    /// <param name="contracts">The registered contracts, used to plan one topic per message type.</param>
    /// <param name="processor">The core consume processor. Required for a consuming role.</param>
    /// <param name="topology">The topology manager (control plane).</param>
    /// <param name="topologyClient">The control-plane client, used by the reachability probe.</param>
    /// <param name="observability">The observability facade applied around each publish.</param>
    /// <param name="metrics">The provider metrics.</param>
    /// <param name="tracker">The in-flight tracker the shutdown drain reads.</param>
    /// <param name="shutdownTimeoutSeconds">The shutdown timeout, clamped to the configured bounds.</param>
    /// <param name="logger">The logger.</param>
    /// <returns>The adapter over a not-yet-started bus.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    /// <exception cref="ResourceNameTooLongException">A derived queue name exceeds its limit.</exception>
    public static AmazonSqsBusAdapter Create(
        AmazonSqsProviderSettings settings,
        MassTransitRole role,
        MessagingMode mode,
        IEnumerable<ContractRegistration> contracts,
        IMassTransitConsumeProcessor? processor,
        SnsSqsTopologyManager topology,
        ISnsSqsTopologyClient topologyClient,
        ObservabilityContext observability,
        AmazonSqsMetrics metrics,
        InFlightTracker tracker,
        int shutdownTimeoutSeconds,
        ILogger<AmazonSqsBusAdapter> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(contracts);
        ArgumentNullException.ThrowIfNull(tracker);

        var registersConsumer = role is MassTransitRole.Consumer or MassTransitRole.Both;
        if (registersConsumer && processor is null)
        {
            throw new ArgumentNullException(
                nameof(processor),
                $"A '{nameof(IMassTransitConsumeProcessor)}' is required for role '{role}'.");
        }

        var queueName = registersConsumer ? SnsSqsNamingConvention.QueueName(settings.EndpointName, settings) : null;
        var decorated = registersConsumer
            ? new InFlightTrackingConsumeProcessor(processor!, tracker)
            : null;

        var bus = Bus.Factory.CreateUsingAmazonSqs(cfg =>
        {
            cfg.Host(settings.Region, host => ConfigureHost(host, settings));

            if (queueName is not null)
            {
                cfg.ReceiveEndpoint(queueName, (IAmazonSqsReceiveEndpointConfigurator endpoint) =>
                {
                    // MassTransit only looks up the queue the topology manager provisioned (spike R1).
                    endpoint.ConfigureConsumeTopology = false;
                    endpoint.WaitTimeSeconds = (ushort)settings.WaitTimeSeconds;
                    endpoint.PrefetchCount = settings.MaxMessagesPerReceive;
                    endpoint.ConcurrentMessageLimit = settings.ConcurrencyLimit;

                    // Aligned with the plan; used by MassTransit only if it has to create the queue.
                    endpoint.QueueAttributes["VisibilityTimeout"] =
                        settings.VisibilityTimeoutSeconds;
                    endpoint.QueueAttributes["MessageRetentionPeriod"] =
                        settings.MessageRetentionSeconds;

                    // The core RetryExecutor is the only retry source (Req 10.9).
                    endpoint.UseMessageRetry(r => r.None());

                    // Rethrow so the lock context faults and the message is not deleted (spike R3).
                    endpoint.ConfigureError(x => x.UseFilter(new RethrowErrorTransportFilter()));

                    endpoint.Consumer(() => new SqsEnvelopeConsumer(decorated!));
                });
            }
        });

        return new AmazonSqsBusAdapter(
            bus, settings, mode, role, contracts, topology, topologyClient, observability, metrics, tracker,
            shutdownTimeoutSeconds, logger);
    }

    /// <inheritdoc />
    public async Task StartBusAsync(CancellationToken cancellationToken)
    {
        // Any failure below propagates and leaves _started false (Req 5.1, 12.5).
        if (_role is MassTransitRole.Consumer or MassTransitRole.Both)
        {
            var plan = SnsSqsTopologyPlanner.Plan(_settings, _mode, _role, _contracts);
            await _topology.ApplyAsync(plan, _settings.AutoCreateTopology, cancellationToken).ConfigureAwait(false);
        }

        await _bus.StartAsync(cancellationToken).ConfigureAwait(false);
        _started = true;

        _logger.LogInformation(
            "MassTransit Amazon SQS bus-state={BusState} region={Region} role={Role}",
            "started",
            _settings.Region,
            _role);
    }

    /// <inheritdoc />
    public async Task StopBusAsync(CancellationToken cancellationToken)
    {
        _tracker.StopAccepting();

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_shutdownTimeout);

        try
        {
            await _bus.StopAsync(timeout.Token).ConfigureAwait(false);
            LastShutdownResult = new ShutdownResult(timedOut: false, _tracker.InFlightCount);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The shutdown timeout elapsed. Unfinished messages are not deleted; SQS redelivers them.
            var remaining = _tracker.InFlightCount;
            LastShutdownResult = new ShutdownResult(timedOut: true, remaining);
            _logger.LogWarning(
                "Amazon SQS bus stop exceeded the {TimeoutSeconds}s shutdown timeout with {InFlightCount} message(s) in flight.",
                _shutdownTimeout.TotalSeconds,
                remaining);
        }
        finally
        {
            _started = false;
        }

        _logger.LogInformation(
            "MassTransit Amazon SQS bus-state={BusState} region={Region} role={Role}",
            "stopped",
            _settings.Region,
            _role);
    }

    /// <inheritdoc />
    /// <exception cref="PublishNotPermittedException">The bus role is Consumer.</exception>
    /// <exception cref="MessageTooLargeException">The estimated wire size exceeds the limit.</exception>
    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (_role == MassTransitRole.Consumer)
        {
            throw new PublishNotPermittedException();
        }

        // Records the publish event and yields the trace and correlation headers (Req 15.1).
        using var activity = _observability.OnPublish(envelope.CorrelationId, out var traceHeaders);

        var wire = SnsSqsWireFormat.ToWire(envelope, traceHeaders, _settings.Fifo, _settings.FifoGroupSelection);
        SnsSqsMessageSizeGuard.Check(wire.Body, wire.AttributeHeaders, _settings.MaxMessageSizeBytes);

        string target;
        Uri address;
        if (_mode == MessagingMode.MessageQueue)
        {
            target = SnsSqsNamingConvention.QueueName(_settings.EndpointName, _settings);
            address = new Uri($"queue:{target}", UriKind.Absolute);
        }
        else
        {
            target = SnsSqsNamingConvention.TopicName(envelope.MessageType, _settings);
            await _topology.EnsureTopicAsync(target, cancellationToken).ConfigureAwait(false);
            address = new Uri($"topic:{target}", UriKind.Absolute);
        }

        var endpoint = await _bus.GetSendEndpoint(address).ConfigureAwait(false);
        await endpoint
            .Send(
                wire.Body,
                context => ApplySendContext(context, wire.AttributeHeaders, wire.GroupId, wire.DeduplicationId,
                    wire.CorrelationGuid),
                cancellationToken)
            .ConfigureAwait(false);

        _metrics.RecordPublished(target);
    }

    /// <inheritdoc />
    public async Task<bool> IsTransportReachableAsync(CancellationToken cancellationToken)
    {
        if (!_started)
        {
            return false;
        }

        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            budget.CancelAfter(ReachabilityBudget);

            var ping = _topologyClient.PingAsync(budget.Token);
            var winner = await Task.WhenAny(ping, Task.Delay(ReachabilityBudget, cancellationToken))
                .ConfigureAwait(false);
            if (winner != ping)
            {
                // Cancel the abandoned ping and report not reachable; observe its fault, if any.
                budget.Cancel();
                _ = ping.ContinueWith(
                    static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                return false;
            }

            await ping.ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Authorization failures, timeouts, and transport errors never reach the readiness caller.
            _logger.LogWarning(
                ex,
                "Amazon SQS reachability probe failed for region {Region}; reporting not reachable.",
                _settings.Region);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task SendToDeadLetterQueueAsync(
        string queueName,
        EnvelopeMessage message,
        IReadOnlyDictionary<string, string> attributes,
        string? messageGroupId,
        string? deduplicationId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(attributes);

        // Not role-gated: a Consumer role must be able to dead-letter (Req 11.4).
        var endpoint = await _bus
            .GetSendEndpoint(new Uri($"queue:{queueName}", UriKind.Absolute))
            .ConfigureAwait(false);

        await endpoint
            .Send(
                message,
                context => ApplySendContext(
                    context, attributes, messageGroupId, deduplicationId, CorrelationGuid.From(message.CorrelationId)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static void ApplySendContext(
        SendContext context,
        IReadOnlyDictionary<string, string> attributes,
        string? groupId,
        string? deduplicationId,
        Guid correlationGuid)
    {
        context.CorrelationId = correlationGuid;
        foreach (var attribute in attributes)
        {
            context.Headers.Set(attribute.Key, attribute.Value);
        }

        if (groupId is not null)
        {
            context.SetGroupId(groupId);
        }

        if (deduplicationId is not null)
        {
            context.SetDeduplicationId(deduplicationId);
        }
    }

    /// <summary>Stops the bus on disposal if it is still started.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            await _bus.StopAsync(CancellationToken.None).ConfigureAwait(false);
            _started = false;
        }
    }

    private static void ConfigureHost(IAmazonSqsHostConfigurator host, AmazonSqsProviderSettings settings)
    {
        if (!string.IsNullOrWhiteSpace(settings.AccessKeyId) && !string.IsNullOrWhiteSpace(settings.SecretAccessKey))
        {
            if (!string.IsNullOrWhiteSpace(settings.SessionToken))
            {
                host.Credentials(new SessionAWSCredentials(
                    settings.AccessKeyId, settings.SecretAccessKey, settings.SessionToken));
            }
            else
            {
                host.AccessKey(settings.AccessKeyId);
                host.SecretKey(settings.SecretAccessKey);
            }
        }

        // Otherwise no credentials are set and the AWS SDK default credential chain applies.
        if (!string.IsNullOrWhiteSpace(settings.ServiceUrl))
        {
            // The SDK ignores RegionEndpoint once ServiceURL is set; AuthenticationRegion signs requests.
            host.Config(new AmazonSQSConfig
            {
                ServiceURL = settings.ServiceUrl,
                AuthenticationRegion = settings.Region,
            });
            host.Config(new AmazonSimpleNotificationServiceConfig
            {
                ServiceURL = settings.ServiceUrl,
                AuthenticationRegion = settings.Region,
            });
        }

        // Only the keys SnsSqsHeaderMapper produces may become attributes; the mapper caps h.* at seven,
        // so the 10-attribute limit holds structurally.
        host.AllowTransportHeader(header => IsAllowedTransportHeader(header.Key));
    }

    private static bool IsAllowedTransportHeader(string key) =>
        key is SnsSqsHeaderMapper.CorrelationIdAttribute
            or SnsSqsHeaderMapper.TraceParentAttribute
            or SnsSqsHeaderMapper.MessageTypeAttribute
            or SqsDeadLetterSink.ReasonAttribute
            or SqsDeadLetterSink.AttemptCountAttribute
            or SqsDeadLetterSink.DeadLetteredAtAttribute
            or SqsDeadLetterSink.IdempotencyKeyAttribute
        || key.StartsWith(SnsSqsHeaderMapper.UserHeaderPrefix, StringComparison.Ordinal);
}
