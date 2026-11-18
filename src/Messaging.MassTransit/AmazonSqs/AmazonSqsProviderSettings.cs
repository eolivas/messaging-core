using System.Globalization;
using Messaging.Abstractions;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>How the FIFO message group id is chosen for a published message.</summary>
public enum FifoGroupSelection
{
    /// <summary>The message group id is the envelope correlation id.</summary>
    CorrelationId,

    /// <summary>The message group id is the envelope message type.</summary>
    MessageType,
}

/// <summary>
/// Typed settings for the Amazon SNS/SQS provider, bound from the open <c>ProviderSettings</c> bag
/// (setting names are <c>AmazonSqs:{Name}</c>). Parsing is lenient: absent or uninterpretable values
/// fall back to the documented default and strict checking is the startup validator's job.
/// </summary>
/// <remarks>
/// <see cref="ServiceUrl"/>, <see cref="AccessKeyId"/>, <see cref="SecretAccessKey"/>, and
/// <see cref="SessionToken"/> are treated as secret-bearing: they declare no permitted range so the
/// configuration validator never echoes their value into an error message.
/// </remarks>
public sealed record AmazonSqsProviderSettings
{
    /// <summary>The prefix shared by every Amazon SQS setting name.</summary>
    public const string Prefix = "AmazonSqs:";

    /// <summary>Setting name: AWS region identifier.</summary>
    public const string RegionSettingName = Prefix + "Region";

    /// <summary>Setting name: logical endpoint name that derives the consumer queue.</summary>
    public const string EndpointNameSettingName = Prefix + "EndpointName";

    /// <summary>Setting name: service URL override (secret-bearing).</summary>
    public const string ServiceUrlSettingName = Prefix + "ServiceUrl";

    /// <summary>Setting name: access key id (secret-bearing).</summary>
    public const string AccessKeyIdSettingName = Prefix + "AccessKeyId";

    /// <summary>Setting name: secret access key (secret-bearing).</summary>
    public const string SecretAccessKeySettingName = Prefix + "SecretAccessKey";

    /// <summary>Setting name: session token (secret-bearing).</summary>
    public const string SessionTokenSettingName = Prefix + "SessionToken";

    /// <summary>Setting name: topic name prefix.</summary>
    public const string TopicNamePrefixSettingName = Prefix + "TopicNamePrefix";

    /// <summary>Setting name: queue name prefix.</summary>
    public const string QueueNamePrefixSettingName = Prefix + "QueueNamePrefix";

    /// <summary>Setting name: dead-letter queue suffix.</summary>
    public const string DeadLetterQueueSuffixSettingName = Prefix + "DeadLetterQueueSuffix";

    /// <summary>Setting name: auto-create topology switch.</summary>
    public const string AutoCreateTopologySettingName = Prefix + "AutoCreateTopology";

    /// <summary>Setting name: raw message delivery switch.</summary>
    public const string RawMessageDeliverySettingName = Prefix + "RawMessageDelivery";

    /// <summary>Setting name: FIFO switch.</summary>
    public const string FifoSettingName = Prefix + "Fifo";

    /// <summary>Setting name: FIFO group selection.</summary>
    public const string FifoGroupSelectionSettingName = Prefix + "FifoGroupSelection";

    /// <summary>Setting name: KMS master key id.</summary>
    public const string KmsMasterKeyIdSettingName = Prefix + "KmsMasterKeyId";

    /// <summary>Setting name: SQS redrive max receive count.</summary>
    public const string MaxReceiveCountSettingName = Prefix + "MaxReceiveCount";

    /// <summary>Setting name: queue visibility timeout in seconds.</summary>
    public const string VisibilityTimeoutSecondsSettingName = Prefix + "VisibilityTimeoutSeconds";

    /// <summary>Setting name: queue message retention in seconds.</summary>
    public const string MessageRetentionSecondsSettingName = Prefix + "MessageRetentionSeconds";

    /// <summary>Setting name: maximum message size in bytes.</summary>
    public const string MaxMessageSizeBytesSettingName = Prefix + "MaxMessageSizeBytes";

    /// <summary>Setting name: receive long-poll wait in seconds.</summary>
    public const string WaitTimeSecondsSettingName = Prefix + "WaitTimeSeconds";

    /// <summary>Setting name: maximum messages per receive call.</summary>
    public const string MaxMessagesPerReceiveSettingName = Prefix + "MaxMessagesPerReceive";

    /// <summary>Setting name: concurrent message processing limit.</summary>
    public const string ConcurrencyLimitSettingName = Prefix + "ConcurrencyLimit";

    /// <summary>Default dead-letter queue suffix.</summary>
    public const string DefaultDeadLetterQueueSuffix = "_error";

    /// <summary>Default <see cref="MaxReceiveCount"/>.</summary>
    public const int DefaultMaxReceiveCount = 5;

    /// <summary>Default <see cref="VisibilityTimeoutSeconds"/>.</summary>
    public const int DefaultVisibilityTimeoutSeconds = 30;

    /// <summary>Default <see cref="MessageRetentionSeconds"/> (4 days).</summary>
    public const int DefaultMessageRetentionSeconds = 345600;

    /// <summary>Default and maximum <see cref="MaxMessageSizeBytes"/> (256 KiB).</summary>
    public const int DefaultMaxMessageSizeBytes = 262144;

    /// <summary>Default <see cref="WaitTimeSeconds"/>.</summary>
    public const int DefaultWaitTimeSeconds = 20;

    /// <summary>Default <see cref="MaxMessagesPerReceive"/>.</summary>
    public const int DefaultMaxMessagesPerReceive = 10;

    /// <summary>Default <see cref="ConcurrencyLimit"/>.</summary>
    public const int DefaultConcurrencyLimit = 10;

    /// <summary>AWS region identifier (required; checked by the validator).</summary>
    public string Region { get; init; } = string.Empty;

    /// <summary>Logical endpoint name that derives the consumer queue (required).</summary>
    public string EndpointName { get; init; } = string.Empty;

    /// <summary>Optional service URL override (secret-bearing; never logged).</summary>
    public string? ServiceUrl { get; init; }

    /// <summary>Optional access key id (secret-bearing).</summary>
    public string? AccessKeyId { get; init; }

    /// <summary>Optional secret access key (secret-bearing).</summary>
    public string? SecretAccessKey { get; init; }

    /// <summary>Optional session token (secret-bearing).</summary>
    public string? SessionToken { get; init; }

    /// <summary>Prefix added to derived topic names.</summary>
    public string TopicNamePrefix { get; init; } = string.Empty;

    /// <summary>Prefix added to derived queue names.</summary>
    public string QueueNamePrefix { get; init; } = string.Empty;

    /// <summary>Suffix that derives the dead-letter queue name.</summary>
    public string DeadLetterQueueSuffix { get; init; } = DefaultDeadLetterQueueSuffix;

    /// <summary>Whether missing topology is created (<c>true</c>) or only verified (<c>false</c>).</summary>
    public bool AutoCreateTopology { get; init; } = true;

    /// <summary>Whether SNS-to-SQS subscriptions use raw message delivery.</summary>
    public bool RawMessageDelivery { get; init; } = true;

    /// <summary>Whether topics and queues are FIFO.</summary>
    public bool Fifo { get; init; }

    /// <summary>How the FIFO message group id is chosen.</summary>
    public FifoGroupSelection FifoGroupSelection { get; init; } = FifoGroupSelection.CorrelationId;

    /// <summary>Optional KMS master key id for server-side encryption.</summary>
    public string? KmsMasterKeyId { get; init; }

    /// <summary>SQS redrive <c>maxReceiveCount</c>.</summary>
    public int MaxReceiveCount { get; init; } = DefaultMaxReceiveCount;

    /// <summary>Queue visibility timeout in seconds.</summary>
    public int VisibilityTimeoutSeconds { get; init; } = DefaultVisibilityTimeoutSeconds;

    /// <summary>Queue message retention in seconds.</summary>
    public int MessageRetentionSeconds { get; init; } = DefaultMessageRetentionSeconds;

    /// <summary>Maximum message size in bytes.</summary>
    public int MaxMessageSizeBytes { get; init; } = DefaultMaxMessageSizeBytes;

    /// <summary>Receive long-poll wait in seconds.</summary>
    public int WaitTimeSeconds { get; init; } = DefaultWaitTimeSeconds;

    /// <summary>Maximum messages fetched per receive call.</summary>
    public int MaxMessagesPerReceive { get; init; } = DefaultMaxMessagesPerReceive;

    /// <summary>Concurrent message processing limit.</summary>
    public int ConcurrencyLimit { get; init; } = DefaultConcurrencyLimit;

    /// <summary>
    /// Leniently binds the settings from the provider-settings bag. Absent, blank, or uninterpretable
    /// values take the documented default; the startup validator performs the strict checks.
    /// </summary>
    /// <param name="providerSettings">The open provider-settings bag.</param>
    /// <returns>The bound settings.</returns>
    public static AmazonSqsProviderSettings FromProviderSettings(
        IReadOnlyDictionary<string, string> providerSettings)
    {
        ArgumentNullException.ThrowIfNull(providerSettings);

        var defaults = new AmazonSqsProviderSettings();

        return new AmazonSqsProviderSettings
        {
            Region = Text(providerSettings, RegionSettingName) ?? string.Empty,
            EndpointName = Text(providerSettings, EndpointNameSettingName) ?? string.Empty,
            ServiceUrl = Text(providerSettings, ServiceUrlSettingName),
            AccessKeyId = Text(providerSettings, AccessKeyIdSettingName),
            SecretAccessKey = Text(providerSettings, SecretAccessKeySettingName),
            SessionToken = Text(providerSettings, SessionTokenSettingName),
            TopicNamePrefix = Text(providerSettings, TopicNamePrefixSettingName) ?? string.Empty,
            QueueNamePrefix = Text(providerSettings, QueueNamePrefixSettingName) ?? string.Empty,
            DeadLetterQueueSuffix = Text(providerSettings, DeadLetterQueueSuffixSettingName)
                ?? DefaultDeadLetterQueueSuffix,
            AutoCreateTopology = Bool(providerSettings, AutoCreateTopologySettingName, defaults.AutoCreateTopology),
            RawMessageDelivery = Bool(providerSettings, RawMessageDeliverySettingName, defaults.RawMessageDelivery),
            Fifo = Bool(providerSettings, FifoSettingName, defaults.Fifo),
            FifoGroupSelection = Enum.TryParse<FifoGroupSelection>(
                Text(providerSettings, FifoGroupSelectionSettingName), ignoreCase: false, out var group)
                && Enum.IsDefined(group)
                    ? group
                    : defaults.FifoGroupSelection,
            KmsMasterKeyId = Text(providerSettings, KmsMasterKeyIdSettingName),
            MaxReceiveCount = Int(providerSettings, MaxReceiveCountSettingName, defaults.MaxReceiveCount),
            VisibilityTimeoutSeconds = Int(providerSettings, VisibilityTimeoutSecondsSettingName, defaults.VisibilityTimeoutSeconds),
            MessageRetentionSeconds = Int(providerSettings, MessageRetentionSecondsSettingName, defaults.MessageRetentionSeconds),
            MaxMessageSizeBytes = Int(providerSettings, MaxMessageSizeBytesSettingName, defaults.MaxMessageSizeBytes),
            WaitTimeSeconds = Int(providerSettings, WaitTimeSecondsSettingName, defaults.WaitTimeSeconds),
            MaxMessagesPerReceive = Int(providerSettings, MaxMessagesPerReceiveSettingName, defaults.MaxMessagesPerReceive),
            ConcurrencyLimit = Int(providerSettings, ConcurrencyLimitSettingName, defaults.ConcurrencyLimit),
        };
    }

    /// <summary>
    /// Declares every Amazon SQS setting so the configuration validator can check presence and
    /// permitted values. Secret-bearing settings (and free-text settings) declare no permitted range, so
    /// the validator never echoes their value.
    /// </summary>
    /// <returns>The setting descriptors.</returns>
    public static IReadOnlyCollection<ProviderSettingDescriptor> Describe() => new[]
    {
        new ProviderSettingDescriptor(RegionSettingName, Required: true, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(EndpointNameSettingName, Required: true, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(ServiceUrlSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(AccessKeyIdSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(SecretAccessKeySettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(SessionTokenSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(TopicNamePrefixSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(QueueNamePrefixSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(DeadLetterQueueSuffixSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(AutoCreateTopologySettingName, Required: false, PermittedValuesOrRange: "true, false"),
        new ProviderSettingDescriptor(RawMessageDeliverySettingName, Required: false, PermittedValuesOrRange: "true, false"),
        new ProviderSettingDescriptor(FifoSettingName, Required: false, PermittedValuesOrRange: "true, false"),
        new ProviderSettingDescriptor(FifoGroupSelectionSettingName, Required: false, PermittedValuesOrRange: "CorrelationId, MessageType"),
        new ProviderSettingDescriptor(KmsMasterKeyIdSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(MaxReceiveCountSettingName, Required: false, PermittedValuesOrRange: "1..1000"),
        new ProviderSettingDescriptor(VisibilityTimeoutSecondsSettingName, Required: false, PermittedValuesOrRange: "1..43200"),
        new ProviderSettingDescriptor(MessageRetentionSecondsSettingName, Required: false, PermittedValuesOrRange: "60..1209600"),
        new ProviderSettingDescriptor(MaxMessageSizeBytesSettingName, Required: false, PermittedValuesOrRange: "1..262144"),
        new ProviderSettingDescriptor(WaitTimeSecondsSettingName, Required: false, PermittedValuesOrRange: "0..20"),
        new ProviderSettingDescriptor(MaxMessagesPerReceiveSettingName, Required: false, PermittedValuesOrRange: "1..10"),
        new ProviderSettingDescriptor(ConcurrencyLimitSettingName, Required: false, PermittedValuesOrRange: "1..1000"),
    };

    private static string? Text(IReadOnlyDictionary<string, string> bag, string name) =>
        bag.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;

    private static bool Bool(IReadOnlyDictionary<string, string> bag, string name, bool fallback) =>
        bool.TryParse(Text(bag, name), out var value) ? value : fallback;

    private static int Int(IReadOnlyDictionary<string, string> bag, string name, int fallback) =>
        int.TryParse(Text(bag, name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : fallback;
}
