using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Providers;

namespace Messaging.Tests.Providers;

// Feature: messaging-core, Property 1: Provider resolution is exact, fail-fast, and stable.
// Validates: Requirements 1.4, 1.5, 1.6, 3.4, 3.5, 3.7.
//
// For any assignment of a provider count (0, 1, or many) to each MessagingMode, a registry built
// from the corresponding provider instances resolves every mode by its exact rule:
//   EXACT + STABLE  (count == 1) — Resolve returns the single registered provider for that mode,
//                                  and repeated calls return the identical instance (reference
//                                  stable for the registry's lifetime). Req 1.4, 1.5, 3.4.
//   FAIL-FAST unresolved (count == 0) — Resolve throws ProviderResolutionException with
//                                  Failure == Unresolved. Req 1.6, 3.7.
//   FAIL-FAST ambiguous  (count >= 2) — Resolve throws ProviderResolutionException with
//                                  Failure == Ambiguous. Req 3.5.
// RegisteredModes reports exactly the modes with at least one provider (Req 1.5).
[Trait("Feature", "messaging-core")]
[Trait("Property", "1")]
public class ProviderRegistryResolutionPropertyTests
{
    private static readonly MessagingMode[] AllModes = Enum.GetValues<MessagingMode>();

    /// <summary>
    /// A generated registry layout: how many providers are registered for each mode. A count of
    /// 0 means the mode is unresolved, 1 means an exact match, 2+ means an ambiguous mapping.
    /// </summary>
    public sealed record RegistryLayout(IReadOnlyDictionary<MessagingMode, int> ProviderCountByMode);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(ProviderRegistryResolutionPropertyTests) })]
    public Property Resolution_IsExact_FailFast_AndStable(RegistryLayout layout)
    {
        // Arrange: materialize the providers for the layout, remembering each mode's instances so
        // the exact-match assertion can check reference identity.
        var providersByMode = BuildProviders(layout);
        var allProviders = providersByMode.Values.SelectMany(list => list).ToList();
        var registry = new ProviderRegistry(allProviders);

        // RegisteredModes is exactly the set of modes with at least one provider (Req 1.5).
        var expectedRegistered = providersByMode
            .Where(kvp => kvp.Value.Count > 0)
            .Select(kvp => kvp.Key)
            .ToHashSet();
        var registeredMatches = registry.RegisteredModes.ToHashSet().SetEquals(expectedRegistered);

        // Every mode obeys its resolution rule.
        var everyModeResolvesCorrectly = AllModes.All(mode => ModeResolvesCorrectly(registry, mode, providersByMode[mode]));

        return (registeredMatches && everyModeResolvesCorrectly)
            .ToProperty()
            .Classify(expectedRegistered.Count == AllModes.Length, "all modes registered")
            .Classify(providersByMode.Any(kvp => kvp.Value.Count >= 2), "has ambiguous mode")
            .Classify(providersByMode.Any(kvp => kvp.Value.Count == 0), "has unresolved mode");
    }

    private static bool ModeResolvesCorrectly(
        ProviderRegistry registry,
        MessagingMode mode,
        IReadOnlyList<IMessagingProvider> registeredForMode)
    {
        return registeredForMode.Count switch
        {
            // EXACT + STABLE: the single registered instance is returned, and twice is the same ref.
            1 => ResolvesToExactStableInstance(registry, mode, registeredForMode[0]),
            // FAIL-FAST unresolved.
            0 => FailsFastWith(registry, mode, ProviderResolutionFailure.Unresolved),
            // FAIL-FAST ambiguous.
            _ => FailsFastWith(registry, mode, ProviderResolutionFailure.Ambiguous),
        };
    }

    private static bool ResolvesToExactStableInstance(
        ProviderRegistry registry,
        MessagingMode mode,
        IMessagingProvider expected)
    {
        var first = registry.Resolve(mode);
        var second = registry.Resolve(mode);

        return ReferenceEquals(first, expected)
            && ReferenceEquals(second, expected)
            && ReferenceEquals(first, second);
    }

    private static bool FailsFastWith(
        ProviderRegistry registry,
        MessagingMode mode,
        ProviderResolutionFailure expectedFailure)
    {
        try
        {
            registry.Resolve(mode);
            return false;
        }
        catch (ProviderResolutionException ex)
        {
            return ex.Failure == expectedFailure && ex.Mode == mode;
        }
    }

    private static IReadOnlyDictionary<MessagingMode, IReadOnlyList<IMessagingProvider>> BuildProviders(
        RegistryLayout layout)
    {
        return AllModes.ToDictionary(
            mode => mode,
            mode => (IReadOnlyList<IMessagingProvider>)Enumerable
                .Range(0, layout.ProviderCountByMode[mode])
                .Select(_ => (IMessagingProvider)new StubMessagingProvider(mode))
                .ToList());
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<RegistryLayout> LayoutArbitrary() => Arb.From(LayoutGen());

    /// <summary>
    /// For each mode, pick a provider count biased to cover all three resolution outcomes: 0
    /// (unresolved), 1 (exact), and 2-4 (ambiguous). Counts above 1 are kept small so generated
    /// registries stay cheap while still exercising ambiguity.
    /// </summary>
    private static Gen<RegistryLayout> LayoutGen()
    {
        var countGen = Gen.Frequency(
            (1, Gen.Constant(0)),
            (2, Gen.Constant(1)),
            (1, Gen.Choose(2, 4)));

        return from counts in Gen.ArrayOf(countGen, AllModes.Length)
               select new RegistryLayout(
                   AllModes
                       .Select((mode, i) => (mode, count: counts[i]))
                       .ToDictionary(pair => pair.mode, pair => pair.count));
    }
}
