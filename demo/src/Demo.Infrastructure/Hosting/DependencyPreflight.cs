using System.Net.Sockets;
using Demo.Application.Providers;
using Messaging.Kafka.Streaming;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;

namespace Demo.Infrastructure.Hosting;

/// <summary>
/// Opens a TCP connection to the broker before any bus is built, so a missing broker is reported
/// with the provider, the host and the matching container command instead of a bus start timeout
/// (Req 4.5). The connection is closed immediately and no protocol bytes are sent.
/// </summary>
public sealed class DependencyPreflight : IDependencyProbe
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(3);

    private const string StartScript = "demo/scripts/start-broker.sh";

    public async Task<bool> CanReachAsync(
        string host, int port, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(host);
        using var timeoutSource = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, timeoutSource.Token);
        using var client = new TcpClient();
        try
        {
            await client.ConnectAsync(host, port, linked.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception ex) when (ex is SocketException or IOException or ObjectDisposedException)
        {
            // Expected control flow: a refused or failed connection means "not reachable".
            // The caller reports it with the provider, host and start command.
            return false;
        }
    }

    /// <summary>
    /// Throws <see cref="DependencyUnavailableException"/> when none of the provider's broker
    /// endpoints accepts a connection within <see cref="DefaultTimeout"/>. A provider with no
    /// endpoint to probe (for example Amazon SQS without a service URL) passes.
    /// </summary>
    public async Task EnsureReachableAsync(
        ProviderId providerId,
        IReadOnlyDictionary<string, string> settings,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(settings);
        var endpoints = Resolve(providerId, settings);
        if (endpoints.Count == 0)
        {
            return;
        }

        foreach (var endpoint in endpoints)
        {
            if (await CanReachAsync(endpoint.Host, endpoint.Port, DefaultTimeout, cancellationToken)
                    .ConfigureAwait(false))
            {
                return;
            }
        }

        var first = endpoints[0];
        throw new DependencyUnavailableException(
            $"{first.ProviderName} is not reachable at {first.Host}:{first.Port}. " +
            $"Start it with: {first.StartCommand}");
    }

    /// <summary>Maps the effective settings to the broker endpoints to probe.</summary>
    public static IReadOnlyList<BrokerEndpoint> Resolve(
        ProviderId providerId, IReadOnlyDictionary<string, string> settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        if (providerId == ProviderId.RabbitMq)
        {
            var host = Read(settings, MassTransitProviderSettings.HostSettingName, "localhost:5672");
            return Parse(host, 5672, "RabbitMQ", $"{StartScript} rabbitmq");
        }

        if (providerId == ProviderId.Kafka)
        {
            var servers = Read(settings, KafkaProviderSettings.BootstrapServersSettingName, "localhost:9092");
            return servers
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .SelectMany(s => Parse(s, 9092, "Kafka", $"{StartScript} kafka"))
                .ToList();
        }

        if (providerId == ProviderId.AmazonSnsSqs)
        {
            var url = Read(settings, AmazonSqsProviderSettings.ServiceUrlSettingName, string.Empty);
            if (Uri.TryCreate(url, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host))
            {
                return [new BrokerEndpoint("Amazon SNS/SQS (LocalStack)", uri.Host, uri.Port,
                    $"{StartScript} sns-sqs")];
            }

            return [];
        }

        return [];
    }

    private static IReadOnlyList<BrokerEndpoint> Parse(string hostPort, int defaultPort, string name, string command)
    {
        var value = hostPort.Trim();
        if (Uri.TryCreate(value, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host)
            && value.Contains("://", StringComparison.Ordinal))
        {
            return [new BrokerEndpoint(name, uri.Host, uri.IsDefaultPort ? defaultPort : uri.Port, command)];
        }

        var colon = value.LastIndexOf(':');
        if (colon > 0 && int.TryParse(value[(colon + 1)..], out var port) && port is > 0 and <= 65535)
        {
            return [new BrokerEndpoint(name, value[..colon], port, command)];
        }

        return value.Length == 0 ? [] : [new BrokerEndpoint(name, value, defaultPort, command)];
    }

    private static string Read(IReadOnlyDictionary<string, string> settings, string key, string fallback) =>
        settings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;
}
