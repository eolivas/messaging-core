using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;

namespace Messaging.Tests.Configuration;

// Feature: messaging-core, Property 2: Configuration validation reports every missing value.
// For all configurations missing an arbitrary non-empty subset of required settings (including
// provider-specific required settings), the validation result's errors name every missing
// setting in that subset — never only the first.
// Validates: Requirements 2.2, 2.5.
public class ConfigurationValidatorMissingValueProperties
{
    // The configuration name the validator reports for an absent Mode (Req 1.2, 2.2).
    private const string ModeSettingName = nameof(MessagingConfiguration.Mode);

    /// <summary>
    /// A generated validation scenario: the provider-declared required settings the validator is
    /// constructed with, plus the exact set of required setting names deliberately left missing
    /// from the configuration under test. <paramref name="ExpectedMissingNames"/> is the complete
    /// set the result MUST report — including <c>Mode</c> when the mode is also omitted.
    /// </summary>
    public sealed record MissingValueScenario(
        IReadOnlyList<ProviderSettingDescriptor> Descriptors,
        MessagingConfiguration Configuration,
        IReadOnlySet<string> ExpectedMissingNames);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(ConfigurationValidatorMissingValueProperties) })]
    public Property Validation_reports_every_missing_required_value(MissingValueScenario scenario)
    {
        var validator = new ConfigurationValidator(scenario.Descriptors.ToArray());

        var result = validator.Validate(scenario.Configuration);

        var reportedNames = result.Errors.Select(e => e.SettingName).ToHashSet();

        // Every deliberately-missing required setting is reported (never only the first), and a
        // non-empty missing set means the configuration is rejected.
        var reportsEveryMissing = scenario.ExpectedMissingNames.All(reportedNames.Contains);

        return (reportsEveryMissing && !result.IsValid)
            .ToProperty()
            .Classify(scenario.ExpectedMissingNames.Contains(ModeSettingName), "mode missing")
            .Classify(scenario.ExpectedMissingNames.Count > 1, "multiple missing");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MissingValueScenario> ScenarioArbitrary() =>
        Arb.From(ScenarioGen());

    private static Gen<MissingValueScenario> ScenarioGen() =>
        from descriptors in RequiredDescriptorsGen()
        // Choose a non-empty subset of required settings to leave missing. The candidate pool is
        // the required provider settings plus the Mode pseudo-setting.
        from missing in NonEmptyMissingSubsetGen(descriptors)
        select BuildScenario(descriptors, missing);

    /// <summary>
    /// Generates a set of provider-declared settings, all required and unconstrained
    /// (<c>PermittedValuesOrRange = null</c>) with distinct names. Unconstrained values keep the
    /// scenario focused on missing-value reporting: any value supplied for a present setting is
    /// accepted, so the only errors produced are missing-value errors (Req 2.5).
    /// </summary>
    private static Gen<IReadOnlyList<ProviderSettingDescriptor>> RequiredDescriptorsGen() =>
        from count in Gen.Choose(0, 6)
        select (IReadOnlyList<ProviderSettingDescriptor>)Enumerable
            .Range(0, count)
            .Select(i => new ProviderSettingDescriptor($"Provider.Setting{i}", Required: true, PermittedValuesOrRange: null))
            .ToList();

    /// <summary>
    /// From the candidate required settings (Mode plus each required provider setting), picks a
    /// non-empty subset to be missing. Each candidate is independently included with a coin flip;
    /// if the flips produce the empty set, the first candidate is forced in so the subset is
    /// always non-empty.
    /// </summary>
    private static Gen<IReadOnlySet<string>> NonEmptyMissingSubsetGen(
        IReadOnlyList<ProviderSettingDescriptor> descriptors)
    {
        var candidates = new List<string> { ModeSettingName };
        candidates.AddRange(descriptors.Select(d => d.Name));

        return from flips in Gen.ArrayOf(Gen.Elements(true, false), candidates.Count)
               let chosen = candidates.Where((_, i) => flips[i]).ToHashSet()
               select EnsureNonEmpty(chosen, candidates);
    }

    private static IReadOnlySet<string> EnsureNonEmpty(HashSet<string> chosen, IReadOnlyList<string> candidates)
    {
        if (chosen.Count == 0)
        {
            chosen.Add(candidates[0]);
        }

        return chosen;
    }

    /// <summary>
    /// Builds the configuration for a scenario: Mode is present (a valid value) unless it is in
    /// the missing set; each required provider setting is included in <c>ProviderSettings</c> with
    /// a valid value unless it is in the missing set. All numeric settings stay at their in-range
    /// defaults so the only errors the validator can raise are missing-value errors.
    /// </summary>
    private static MissingValueScenario BuildScenario(
        IReadOnlyList<ProviderSettingDescriptor> descriptors,
        IReadOnlySet<string> missing)
    {
        var providerSettings = new Dictionary<string, string>();
        foreach (var descriptor in descriptors)
        {
            if (!missing.Contains(descriptor.Name))
            {
                providerSettings[descriptor.Name] = "present-value";
            }
        }

        var configuration = new MessagingConfiguration
        {
            Mode = missing.Contains(ModeSettingName) ? null : MessagingMode.PublishSubscribe,
            ProviderSettings = providerSettings,
        };

        return new MissingValueScenario(descriptors, configuration, missing);
    }
}
