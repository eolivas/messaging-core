using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>Property tests for <see cref="LocalStackImageTag.Resolve"/>.</summary>
public class LocalStackImageTagPropertyTests
{
    /// <summary>A valid <c>major.minor.patch</c> tag.</summary>
    public sealed record PatchTag(string Value);

    /// <summary>A non-empty tag that is not <c>major.minor.patch</c>.</summary>
    public sealed record InvalidTag(string Value);

    public static Arbitrary<PatchTag> PatchTags() =>
        Gen.Three(Gen.Choose(0, 999))
            .Select(t => new PatchTag($"{t.Item1}.{t.Item2}.{t.Item3}"))
            .ToArbitrary();

    public static Arbitrary<InvalidTag> InvalidTags()
    {
        var number = Gen.Choose(0, 999).Select(n => n.ToString());
        var partial = number.Select(a => a);
        var twoParts = Gen.Two(number).Select(t => $"{t.Item1}.{t.Item2}");
        var fourParts = Gen.Four(number).Select(t => $"{t.Item1}.{t.Item2}.{t.Item3}.{t.Item4}");
        var withSuffix = Gen.Three(number).Select(t => $"{t.Item1}.{t.Item2}.{t.Item3}-rc1");
        var withPrefix = Gen.Three(number).Select(t => $"v{t.Item1}.{t.Item2}.{t.Item3}");
        var padded = Gen.Three(number).Select(t => $" {t.Item1}.{t.Item2}.{t.Item3} ");
        var trailingNewline = Gen.Three(number).Select(t => $"{t.Item1}.{t.Item2}.{t.Item3}\n");
        var floating = Gen.Elements("latest", "stable", "community", "LATEST", " ", "\t", "4.x", "4.14.x");

        return Gen.OneOf(partial, twoParts, fourParts, withSuffix, withPrefix, padded, trailingNewline, floating)
            .Select(v => new InvalidTag(v))
            .ToArbitrary();
    }

    // Feature: sns-sqs-provider, Property 15: LocalStack image tag resolution
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(LocalStackImageTagPropertyTests) })]
    public bool Resolve_ForAnyPatchLevelVersion_ReturnsItUnchanged(PatchTag tag) =>
        LocalStackImageTag.Resolve(tag.Value) == tag.Value;

    // Feature: sns-sqs-provider, Property 15: LocalStack image tag resolution
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(LocalStackImageTagPropertyTests) })]
    public bool Resolve_ForAnyNonPatchLevelOverride_Throws(InvalidTag tag)
    {
        try
        {
            LocalStackImageTag.Resolve(tag.Value);
            return false;
        }
        catch (ArgumentException)
        {
            return true;
        }
    }

    // Feature: sns-sqs-provider, Property 15: LocalStack image tag resolution
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public bool Resolve_ForNullOrEmptyOverride_ReturnsTheDefault(bool useNull) =>
        LocalStackImageTag.Resolve(useNull ? null : string.Empty) == "4.14.0";
}
