using Confluent.Kafka;
using Messaging.Abstractions;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Default <see cref="IKafkaTransportProbe"/>: requests cluster metadata from the bootstrap servers
/// through a Confluent admin client built from <see cref="KafkaClientConfigFactory.CreateAdminClientConfig"/>.
/// Reachable when at least one broker is returned (Req 10.3). A missing broker, any thrown error, or a
/// 1 second timeout yields an unreachable <see cref="TransportHealth"/>; the probe never propagates
/// exceptions. The dependency name is a fixed label and never carries SASL material (Req 12.10).
/// </summary>
public sealed class ConfluentKafkaTransportProbe : IKafkaTransportProbe
{
    /// <summary>Fixed, secret-free dependency name reported when the cluster is unreachable.</summary>
    public const string DependencyName = "Kafka (bootstrap servers)";

    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(1);

    private readonly Func<CancellationToken, Task<int>> _brokerCountCheck;

    /// <summary>Creates a probe backed by a real admin client.</summary>
    /// <param name="configFactory">Source of the admin client configuration.</param>
    public ConfluentKafkaTransportProbe(KafkaClientConfigFactory configFactory)
        : this(configFactory, brokerCountCheck: null)
    {
    }

    /// <summary>
    /// Creates a probe with an injectable metadata check (test seam). When
    /// <paramref name="brokerCountCheck"/> is null the Confluent admin client is used.
    /// </summary>
    /// <param name="configFactory">Source of the admin client configuration.</param>
    /// <param name="brokerCountCheck">Returns the number of brokers visible in cluster metadata.</param>
    public ConfluentKafkaTransportProbe(
        KafkaClientConfigFactory configFactory,
        Func<CancellationToken, Task<int>>? brokerCountCheck)
    {
        ArgumentNullException.ThrowIfNull(configFactory);
        _brokerCountCheck = brokerCountCheck ?? (_ => Task.Run(() => GetBrokerCount(configFactory)));
    }

    /// <inheritdoc />
    public async Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var check = _brokerCountCheck(timeoutCts.Token);
            var completed = await Task.WhenAny(check, Task.Delay(ProbeTimeout, cancellationToken))
                .ConfigureAwait(false);

            if (completed != check)
            {
                // Timed out or cancelled; abandon the check and observe its eventual fault.
                timeoutCts.Cancel();
                _ = check.ContinueWith(
                    static t => _ = t.Exception,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
                return Unreachable();
            }

            return await check.ConfigureAwait(false) >= 1
                ? new TransportHealth(Reachable: true, UnavailableDependency: null)
                : Unreachable();
        }
        catch (Exception)
        {
            return Unreachable();
        }
    }

    private static TransportHealth Unreachable() =>
        new(Reachable: false, UnavailableDependency: DependencyName);

    private static int GetBrokerCount(KafkaClientConfigFactory configFactory)
    {
        using var admin = new AdminClientBuilder(configFactory.CreateAdminClientConfig()).Build();
        return admin.GetMetadata(ProbeTimeout).Brokers.Count;
    }
}
