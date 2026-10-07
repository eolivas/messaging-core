using Messaging.Abstractions;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The provider-specific settings the Batch Processing adapter needs: where it pulls from, where the
/// publisher writes, the bounded window size, and how pull cycles are triggered. These are declared to
/// the configuration validator through <see cref="Describe"/> so missing or out-of-range values fail
/// fast at startup (Req 2.5).
/// </summary>
/// <remarks>
/// Setting names are the configuration keys the validator reports by (Req 2.5). None of these values
/// are secrets; connection strings/credentials for the concrete batch store are injected into the
/// transport ports, not held here, and are never logged (code-conventions security).
/// </remarks>
public sealed record BatchProviderSettings
{
    /// <summary>The configuration key for the batch source.</summary>
    public const string SourceSettingName = "Batch:Source";

    /// <summary>The configuration key for the batch destination.</summary>
    public const string DestinationSettingName = "Batch:Destination";

    /// <summary>The configuration key for the bounded window size.</summary>
    public const string WindowSizeSettingName = "Batch:WindowSize";

    /// <summary>The configuration key for the trigger mode.</summary>
    public const string TriggerModeSettingName = "Batch:TriggerMode";

    /// <summary>The configuration key for the schedule interval (used when scheduled).</summary>
    public const string ScheduleSettingName = "Batch:Schedule";

    /// <summary>Minimum permitted <see cref="WindowSize"/>.</summary>
    public const int MinWindowSize = 1;

    /// <summary>Maximum permitted <see cref="WindowSize"/>.</summary>
    public const int MaxWindowSize = 10_000;

    /// <summary>The batch source the consumer host pulls bounded windows from.</summary>
    public required string Source { get; init; }

    /// <summary>The batch destination the publisher writes to.</summary>
    public required string Destination { get; init; }

    /// <summary>The bounded window size: items pulled per cycle, 1..10000 inclusive.</summary>
    public required int WindowSize { get; init; }

    /// <summary>How pull cycles are triggered (scheduled vs externally triggered).</summary>
    public BatchTriggerMode TriggerMode { get; init; } = BatchTriggerMode.Scheduled;

    /// <summary>The interval between scheduled pull cycles; honored only when scheduled.</summary>
    public TimeSpan Schedule { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Declares the provider-specific required settings so the configuration validator can check
    /// their presence and permitted values during startup validation (Req 2.5). This set is what the
    /// provider's <see cref="IMessagingProvider.RequiredSettings"/> exposes and does not depend on any
    /// instance values.
    /// </summary>
    public static IReadOnlyCollection<ProviderSettingDescriptor> Describe() => new[]
    {
        new ProviderSettingDescriptor(SourceSettingName, Required: true, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(DestinationSettingName, Required: true, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(
            WindowSizeSettingName,
            Required: true,
            PermittedValuesOrRange: $"{MinWindowSize}..{MaxWindowSize}"),
        new ProviderSettingDescriptor(
            TriggerModeSettingName,
            Required: true,
            PermittedValuesOrRange: $"{nameof(BatchTriggerMode.Scheduled)}|{nameof(BatchTriggerMode.Triggered)}"),
        new ProviderSettingDescriptor(
            ScheduleSettingName,
            Required: false,
            PermittedValuesOrRange: "ISO-8601 duration"),
    };

    /// <summary>
    /// Validates the window-size bound fail-fast (Req 2.5). The validator checks presence and ranges
    /// from <see cref="Describe"/>; this guard protects the shell's own invariant so a misconfigured
    /// window never reaches the pull loop.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">If <see cref="WindowSize"/> is out of range.</exception>
    public void Validate()
    {
        if (WindowSize is < MinWindowSize or > MaxWindowSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WindowSize),
                WindowSize,
                $"WindowSize must be between {MinWindowSize} and {MaxWindowSize} inclusive.");
        }
    }
}
