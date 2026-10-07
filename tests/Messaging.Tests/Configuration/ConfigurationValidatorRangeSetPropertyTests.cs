using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;

namespace Messaging.Tests.Configuration;

// Feature: messaging-core, Property 3: Range and set validation is complete and sound.
// Validates: Requirements 2.3, 2.6.
//
// Encodes both directions of the property against ConfigurationValidator:
//   SOUND    — for any configuration whose five bounded numeric settings are all within their
//              permitted ranges and whose MessagingMode is a recognized value, the validator
//              flags no range/set error and (with no missing values and no provider settings)
//              reports IsValid true (Req 2.6). The validator never rejects an in-range value.
//   COMPLETE — for any configuration where one or more numeric settings are generated outside
//              their permitted range and/or the mode is an out-of-set value, the validator
//              reports an error for every offending setting, by its configuration name, carrying
//              the permitted range/set, and IsValid is false (Req 2.3).
//
// A single generator produces both in-range and out-of-range values for each setting (and both
// recognized and out-of-set modes) so one property exercises both directions.
public class ConfigurationValidatorRangeSetPropertyTests
{
    // The configuration name the validator reports for an out-of-set Mode (Req 2.4).
    private const string ModeSettingName = nameof(MessagingConfiguration.Mode);

    /// <summary>
    /// A generated candidate configuration together with the exact set of setting names the
    /// validator is expected to flag for range/set violations.
    /// </summary>
    public sealed record Candidate(
        MessagingConfiguration Configuration,
        IReadOnlySet<string> ExpectedFlaggedSettings);

    [Property(MaxTest = 300, QuietOnSuccess = true, Arbitrary = new[] { typeof(ConfigurationValidatorRangeSetPropertyTests) })]
    public Property RangeAndSetValidation_IsCompleteAndSound(Candidate candidate)
    {
        var validator = new ConfigurationValidator();

        var result = validator.Validate(candidate.Configuration);

        var expected = candidate.ExpectedFlaggedSettings;
        var reportedSettings = result.Errors.Select(e => e.SettingName).ToList();

        // COMPLETE: the reported setting names are exactly the offending ones — every offending
        // setting reported once, no spurious extra, no in-range/in-set setting flagged.
        var reportedSetMatchesExpected =
            reportedSettings.Count == expected.Count
            && expected.All(reportedSettings.Contains)
            && reportedSettings.All(expected.Contains);

        // Every reported range/set error carries a permitted set/range (Req 2.3) matching the
        // single source of truth.
        var permittedTextIsCorrect = result.Errors.All(PermittedTextIsCorrect);

        if (expected.Count == 0)
        {
            // SOUND + Req 2.6: an all-in-range, recognized-mode configuration with no missing
            // values is valid and flags nothing.
            return (result.IsValid && reportedSettings.Count == 0)
                .ToProperty()
                .Label($"expected valid but reported: [{string.Join(", ", reportedSettings)}]");
        }

        // Req 2.3: any out-of-range/out-of-set value prevents a processing-ready state and is
        // reported by name with its permitted range/set.
        return (!result.IsValid && reportedSetMatchesExpected && permittedTextIsCorrect)
            .ToProperty()
            .Label(
                $"expected flagged [{string.Join(", ", expected)}] but reported " +
                $"[{string.Join(", ", reportedSettings)}] (IsValid={result.IsValid})")
            .Classify(expected.Contains(ModeSettingName), "mode out-of-set")
            .Classify(expected.Count > 1, "multiple out-of-range");
    }

    /// <summary>
    /// Confirms a reported error carries the permitted range/set matching the single source of
    /// truth: <see cref="ConfigurationBounds"/> for numeric settings and the full recognized
    /// <see cref="MessagingMode"/> set for the mode.
    /// </summary>
    private static bool PermittedTextIsCorrect(ConfigurationError error)
    {
        if (error.SettingName == ModeSettingName)
        {
            return error.PermittedValuesOrRange == string.Join(", ", Enum.GetNames<MessagingMode>());
        }

        return BoundsBySettingName.TryGetValue(error.SettingName, out var bounds)
            && error.PermittedValuesOrRange == bounds.PermittedRange;
    }

    /// <summary>Every bounded numeric setting keyed by the configuration name the validator reports.</summary>
    private static readonly IReadOnlyDictionary<string, ConfigurationBounds.Bounds> BoundsBySettingName =
        new Dictionary<string, ConfigurationBounds.Bounds>
        {
            [ConfigurationBounds.MaxAttempts.SettingName] = ConfigurationBounds.MaxAttempts,
            [ConfigurationBounds.PerAttemptDelaySeconds.SettingName] = ConfigurationBounds.PerAttemptDelaySeconds,
            [ConfigurationBounds.MaxDispatchAttempts.SettingName] = ConfigurationBounds.MaxDispatchAttempts,
            [ConfigurationBounds.RetentionSeconds.SettingName] = ConfigurationBounds.RetentionSeconds,
            [ConfigurationBounds.ShutdownTimeoutSeconds.SettingName] = ConfigurationBounds.ShutdownTimeoutSeconds,
        };

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<Candidate> CandidateArbitrary() => Arb.From(CandidateGen());

    /// <summary>
    /// Builds candidate configurations: for each bounded numeric setting it generates either an
    /// in-range value or an out-of-range value (below min or above max), and for the mode either a
    /// recognized value or an out-of-set value. The expected-flagged set is assembled alongside so
    /// the assertion knows precisely which settings must be reported.
    /// </summary>
    private static Gen<Candidate> CandidateGen() =>
        from maxAttempts in NumericChoiceGen(ConfigurationBounds.MaxAttempts)
        from perAttemptDelay in NumericChoiceGen(ConfigurationBounds.PerAttemptDelaySeconds)
        from maxDispatch in NumericChoiceGen(ConfigurationBounds.MaxDispatchAttempts)
        from retention in NumericChoiceGen(ConfigurationBounds.RetentionSeconds)
        from shutdown in NumericChoiceGen(ConfigurationBounds.ShutdownTimeoutSeconds)
        from mode in ModeChoiceGen()
        select BuildCandidate(maxAttempts, perAttemptDelay, maxDispatch, retention, shutdown, mode);

    private static Candidate BuildCandidate(
        (int Value, bool OutOfRange) maxAttempts,
        (int Value, bool OutOfRange) perAttemptDelay,
        (int Value, bool OutOfRange) maxDispatch,
        (int Value, bool OutOfRange) retention,
        (int Value, bool OutOfRange) shutdown,
        (MessagingMode Mode, bool OutOfSet) mode)
    {
        var configuration = new MessagingConfiguration
        {
            Mode = mode.Mode,
            Retry = new RetryPolicyConfig
            {
                MaxAttempts = maxAttempts.Value,
                PerAttemptDelaySeconds = perAttemptDelay.Value,
            },
            Outbox = new OutboxConfig
            {
                MaxDispatchAttempts = maxDispatch.Value,
            },
            Idempotency = new IdempotencyConfig
            {
                RetentionSeconds = retention.Value,
            },
            Lifecycle = new LifecycleConfig
            {
                ShutdownTimeoutSeconds = shutdown.Value,
            },
        };

        var expected = new HashSet<string>();
        if (maxAttempts.OutOfRange) expected.Add(ConfigurationBounds.MaxAttempts.SettingName);
        if (perAttemptDelay.OutOfRange) expected.Add(ConfigurationBounds.PerAttemptDelaySeconds.SettingName);
        if (maxDispatch.OutOfRange) expected.Add(ConfigurationBounds.MaxDispatchAttempts.SettingName);
        if (retention.OutOfRange) expected.Add(ConfigurationBounds.RetentionSeconds.SettingName);
        if (shutdown.OutOfRange) expected.Add(ConfigurationBounds.ShutdownTimeoutSeconds.SettingName);
        if (mode.OutOfSet) expected.Add(ModeSettingName);

        return new Candidate(configuration, expected);
    }

    /// <summary>
    /// Generates either an in-range value (within [Min, Max]) or an out-of-range value (below Min
    /// or above Max), tagging which so the expected-flagged set is computed deterministically.
    /// </summary>
    private static Gen<(int Value, bool OutOfRange)> NumericChoiceGen(ConfigurationBounds.Bounds bounds)
    {
        var inRange =
            from v in Gen.Choose(bounds.Min, bounds.Max)
            select (v, false);

        var belowMin = Gen.Choose(int.MinValue, bounds.Min - 1);
        var aboveMax = Gen.Choose(bounds.Max + 1, int.MaxValue);
        var outOfRange =
            from v in Gen.OneOf(belowMin, aboveMax)
            select (v, true);

        return Gen.Frequency(
            (1, inRange),
            (1, outOfRange));
    }

    /// <summary>
    /// Generates either a recognized <see cref="MessagingMode"/> or an out-of-set enum value (an
    /// integer not defined on the enum), tagging which.
    /// </summary>
    private static Gen<(MessagingMode Mode, bool OutOfSet)> ModeChoiceGen()
    {
        var recognizedValues = Enum.GetValues<MessagingMode>();
        var maxDefined = recognizedValues.Cast<int>().Max();

        var recognized =
            from idx in Gen.Choose(0, recognizedValues.Length - 1)
            select (recognizedValues[idx], false);

        // Out-of-set: an integer strictly greater than any defined value cast to the enum type so
        // Enum.IsDefined reports false (mirrors a bad configured mode).
        var outOfSet =
            from offset in Gen.Choose(1, 1000)
            select ((MessagingMode)(maxDefined + offset), true);

        return Gen.Frequency(
            (2, recognized),
            (1, outOfSet));
    }
}
