using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Lifecycle;

namespace Messaging.Tests.Lifecycle;

// Feature: messaging-core, Property 24: Readiness reflects transport reachability.
// Validates: Requirements 10.3, 10.4 (and 1.7 active-mode carry-through).
//
// For ANY transport-health result and ANY active mode, MessagingHealthChecks.MapToReadiness is a
// total projection where:
//   * the readiness status is ready WHEN AND ONLY WHEN the transport is reachable (Req 10.3/10.4),
//   * when NOT reachable the readiness response carries the source's named unavailable dependency
//     (Req 10.4), and
//   * the active mode is reported on every response (Req 1.7).
// Driving MapToReadiness directly lets the property exercise arbitrary TransportHealth values
// without standing up a provider.
[Trait("Feature", "messaging-core")]
[Trait("Property", "24")]
public class ReadinessReachabilityPropertyTests
{
    /// <summary>A generated input: an arbitrary transport-health result paired with an active mode.</summary>
    public sealed record ReadinessInput(TransportHealth Health, MessagingMode ActiveMode);

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(ReadinessReachabilityPropertyTests) })]
    public Property Readiness_ReflectsTransportReachability(ReadinessInput input)
    {
        var result = MessagingHealthChecks.MapToReadiness(input.Health, input.ActiveMode);

        // Ready iff reachable (Req 10.3/10.4).
        var readyMatchesReachable = result.IsReady == input.Health.Reachable;

        // Active mode is always carried through (Req 1.7).
        var modeCarried = result.ActiveMode == input.ActiveMode;

        // When not reachable, the named unavailable dependency is carried through (Req 10.4);
        // when reachable, a ready result carries no dependency.
        var dependencyCarried = input.Health.Reachable
            ? result.UnavailableDependency is null
            : result.UnavailableDependency == input.Health.UnavailableDependency;

        return (readyMatchesReachable && modeCarried && dependencyCarried)
            .ToProperty()
            .Label(
                $"reachable={input.Health.Reachable}, dependency={input.Health.UnavailableDependency ?? "<null>"}, " +
                $"mode={input.ActiveMode} => isReady={result.IsReady}, " +
                $"reportedDependency={result.UnavailableDependency ?? "<null>"}, reportedMode={result.ActiveMode}")
            .Classify(input.Health.Reachable, "reachable")
            .Classify(!input.Health.Reachable, "unreachable");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<ReadinessInput> ReadinessInputArbitrary() => Arb.From(ReadinessInputGen());

    /// <summary>
    /// Generates arbitrary readiness inputs across the whole transport-health input space: reachable
    /// results (no unavailable dependency, matching the production contract) and unreachable results
    /// carrying a non-empty named dependency, each paired with an arbitrary <see cref="MessagingMode"/>.
    /// </summary>
    private static Gen<ReadinessInput> ReadinessInputGen() =>
        from mode in Gen.Elements(
            MessagingMode.PublishSubscribe,
            MessagingMode.MessageQueue,
            MessagingMode.Streaming,
            MessagingMode.BatchProcessing)
        from health in Gen.OneOf(ReachableHealthGen(), UnreachableHealthGen())
        select new ReadinessInput(health, mode);

    /// <summary>A reachable transport result; by contract it names no unavailable dependency.</summary>
    private static Gen<TransportHealth> ReachableHealthGen() =>
        Gen.Constant(new TransportHealth(Reachable: true, UnavailableDependency: null));

    /// <summary>An unreachable transport result naming a non-empty dependency (Req 10.4).</summary>
    private static Gen<TransportHealth> UnreachableHealthGen() =>
        from dependency in DependencyNameGen()
        select new TransportHealth(Reachable: false, UnavailableDependency: dependency);

    /// <summary>Generates a non-empty, non-whitespace dependency name from visible token characters.</summary>
    private static Gen<string> DependencyNameGen() =>
        from chars in Gen.NonEmptyListOf(Gen.Elements(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray()))
        select new string(chars.ToArray());
}
