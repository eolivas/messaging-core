using Messaging.Abstractions;

namespace Messaging.MassTransit.Configuration;

/// <summary>
/// The provider-specific settings the MassTransit adapter needs, bound from the open
/// <c>ProviderSettings</c> extension bag of the messaging configuration. These back the
/// <see cref="RequiredSettings"/> the configuration validator checks at startup (Req 2.5) and tell
/// the adapter which broker MassTransit should talk to (Req 3.4).
/// </summary>
/// <remarks>
/// <para>
/// The adapter shell keeps the surface deliberately shallow: only the transport selector and its
/// connection endpoint. Broker-specific topology, credentials beyond the connection string, and
/// tuning belong to the MassTransit provider's own later spec, not here.
/// </para>
/// <para>
/// The connection string is a secret-bearing value. It is never logged; the adapter references it by
/// its setting name (<see cref="ConnectionSettingName"/>) in diagnostics (code conventions: redact
/// sensitive values; never log secrets).
/// </para>
/// </remarks>
public sealed class MassTransitProviderSettings
{
    /// <summary>The configuration name of the transport selector setting.</summary>
    public const string TransportSettingName = "MassTransit:Transport";

    /// <summary>The configuration name of the broker connection setting (secret-bearing).</summary>
    public const string ConnectionSettingName = "MassTransit:ConnectionString";

    /// <summary>The configuration name of the host/broker display name setting.</summary>
    public const string HostSettingName = "MassTransit:Host";

    /// <summary>
    /// The permitted connection-string format for the RabbitMQ transport, reported as the
    /// connection setting's permitted-format hint in startup validation. TLS is selected by the
    /// <c>amqps://</c> scheme. Contains no secret-bearing value — only the expected shape.
    /// </summary>
    public const string RabbitMqConnectionFormat = "amqp:// or amqps:// URI";

    /// <summary>
    /// Creates the settings for the MassTransit adapter.
    /// </summary>
    /// <param name="transport">The transport MassTransit is configured against.</param>
    /// <param name="connectionString">
    /// The broker connection string (secret-bearing). May be empty only for
    /// <see cref="MassTransitTransport.InMemory"/>, which needs no external endpoint.
    /// </param>
    /// <param name="host">
    /// A non-secret display name for the broker/dependency used in health reporting (for example the
    /// hostname). Falls back to the transport name when not supplied.
    /// </param>
    /// <exception cref="ArgumentException">
    /// If a non-in-memory transport is configured without a connection string, or if the RabbitMQ
    /// transport is configured with a connection string that is empty, whitespace-only, or not a
    /// parseable AMQP(S) URI (Req 12.4). The message names the setting only and never includes the
    /// configured connection-string value (Req 3.5).
    /// </exception>
    public MassTransitProviderSettings(
        MassTransitTransport transport,
        string? connectionString,
        string? host = null)
    {
        Transport = transport;

        if (transport != MassTransitTransport.InMemory && string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException(
                $"A '{ConnectionSettingName}' is required for transport '{transport}'.",
                nameof(connectionString));
        }

        // RabbitMQ reaches its broker over an AMQP(S) URI. A present connection string that is empty,
        // whitespace-only, or not a parseable amqp(s):// URI is invalid and must stop startup before a
        // processing-ready state (Req 12.4). The configured value is secret-bearing, so the failure is
        // reported by setting name only and the value is never emitted into the message (Req 3.5).
        if (transport == MassTransitTransport.RabbitMq && !IsRabbitMqConnectionFormatValid(connectionString))
        {
            throw new ArgumentException(
                $"The '{ConnectionSettingName}' for transport '{transport}' must be a parseable " +
                $"{RabbitMqConnectionFormat}.",
                nameof(connectionString));
        }

        ConnectionString = connectionString ?? string.Empty;
        Host = string.IsNullOrWhiteSpace(host) ? transport.ToString() : host!;
    }

    /// <summary>
    /// Tests whether <paramref name="connectionString"/> is a valid RabbitMQ broker connection string:
    /// present, non-whitespace, and parseable as an absolute URI whose scheme is <c>amqp</c> or
    /// <c>amqps</c> (TLS selected by <c>amqps</c>) (Req 12.4). Takes only the raw value and returns a
    /// boolean, so it can be used both by construction and by callers that must decide validity without
    /// handling or logging the secret-bearing value.
    /// </summary>
    /// <param name="connectionString">The candidate connection string.</param>
    /// <returns><c>true</c> when the value is a parseable AMQP(S) URI; otherwise <c>false</c>.</returns>
    public static bool IsRabbitMqConnectionFormatValid(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return false;
        }

        if (!Uri.TryCreate(connectionString, UriKind.Absolute, out var uri))
        {
            return false;
        }

        return string.Equals(uri.Scheme, "amqp", StringComparison.OrdinalIgnoreCase)
            || string.Equals(uri.Scheme, "amqps", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The transport MassTransit is configured against.</summary>
    public MassTransitTransport Transport { get; }

    /// <summary>The broker connection string (secret-bearing; never logged).</summary>
    public string ConnectionString { get; }

    /// <summary>A non-secret display name for the broker, used when naming an unavailable dependency.</summary>
    public string Host { get; }

    /// <summary>
    /// Declares the provider-specific settings so the configuration validator can confirm their
    /// presence and permitted values during startup validation (Req 2.5). The connection string is
    /// required for every transport except <see cref="MassTransitTransport.InMemory"/>.
    /// </summary>
    /// <returns>The required-setting descriptors for the MassTransit adapter.</returns>
    public IReadOnlyCollection<ProviderSettingDescriptor> ToRequiredSettings()
    {
        var transportValues = string.Join(
            ", ",
            Enum.GetNames<MassTransitTransport>());

        // For RabbitMQ the connection string must be a parseable AMQP(S) URI, so the descriptor carries
        // that format as its permitted-format hint; the hint is non-secret and names the expected shape
        // only (Req 3.5). Other broker transports keep the generic non-empty hint.
        var connectionFormatHint = Transport == MassTransitTransport.RabbitMq
            ? RabbitMqConnectionFormat
            : "non-empty connection string";

        return new[]
        {
            new ProviderSettingDescriptor(
                TransportSettingName,
                Required: true,
                PermittedValuesOrRange: transportValues),
            new ProviderSettingDescriptor(
                ConnectionSettingName,
                Required: Transport != MassTransitTransport.InMemory,
                PermittedValuesOrRange: connectionFormatHint),
        };
    }
}
