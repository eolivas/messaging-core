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
    /// If a non-in-memory transport is configured without a connection string.
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

        ConnectionString = connectionString ?? string.Empty;
        Host = string.IsNullOrWhiteSpace(host) ? transport.ToString() : host!;
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

        return new[]
        {
            new ProviderSettingDescriptor(
                TransportSettingName,
                Required: true,
                PermittedValuesOrRange: transportValues),
            new ProviderSettingDescriptor(
                ConnectionSettingName,
                Required: Transport != MassTransitTransport.InMemory,
                PermittedValuesOrRange: "non-empty connection string"),
        };
    }
}
