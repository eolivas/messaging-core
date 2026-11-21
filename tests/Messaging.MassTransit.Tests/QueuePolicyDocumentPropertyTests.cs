using System;
using System.Linq;
using System.Text.Json.Nodes;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 5.3, 14.9.
//
// QueuePolicyDocument.Merge keeps statements it does not own, emits exactly one scoped
// Allow sqs:SendMessage statement per distinct topic ARN (service principal, ArnEquals on
// aws:SourceArn, never a wildcard principal) and is idempotent.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "8")]
public class QueuePolicyDocumentPropertyTests
{
    private const string QueueArn = "arn:aws:sqs:us-east-1:123456789012:orders";

    // Feature: sns-sqs-provider, Property 8: Queue policy merge is scoped and idempotent
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(QueuePolicyDocumentPropertyTests) })]
    public Property Merge_ForAnyExistingPolicyAndTopics_IsScopedAndPreservesUnrelatedStatements(Scenario s)
    {
        var merged = QueuePolicyDocument.Merge(s.ExistingJson, QueueArn, s.TopicArns);
        var statements = (JsonNode.Parse(merged)!["Statement"] as JsonArray)!;
        var distinct = s.TopicArns.Distinct(StringComparer.Ordinal).ToList();

        var unrelatedPreserved = s.UnrelatedSids.All(sid =>
            statements.Count(st => SidOf(st) == sid) == 1);

        var onePerTopic = distinct.All(topic =>
        {
            var matches = statements.Where(st => SidOf(st) == QueuePolicyDocument.SidFor(topic)).ToList();
            return matches.Count == 1 && IsScoped(matches[0]!, topic);
        });

        // Statements for topics not requested are left as found; every requested one is non-wildcard.
        var ownedSids = distinct.Select(QueuePolicyDocument.SidFor).ToHashSet(StringComparer.Ordinal);
        var noWildcard = !statements.Any(st => IsWildcardPrincipal(st) && ownedSids.Contains(SidOf(st) ?? string.Empty));
        var expectedCount = s.UnrelatedSids.Count + distinct.Count;

        return (unrelatedPreserved && onePerTopic && noWildcard && statements.Count == expectedCount)
            .ToProperty()
            .Classify(s.ExistingJson is null, "no existing policy")
            .Classify(s.TopicArns.Count != distinct.Count, "duplicate topics")
            .Classify(s.UnrelatedSids.Count > 0, "unrelated statements");
    }

    // Feature: sns-sqs-provider, Property 8: Queue policy merge is scoped and idempotent
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(QueuePolicyDocumentPropertyTests) })]
    public Property Merge_AppliedTwice_EqualsMergedOnce(Scenario s)
    {
        var once = QueuePolicyDocument.Merge(s.ExistingJson, QueueArn, s.TopicArns);
        var twice = QueuePolicyDocument.Merge(once, QueueArn, s.TopicArns);

        return (once == twice).ToProperty();
    }

    private static string? SidOf(JsonNode? statement) => statement?["Sid"]?.GetValue<string>();

    private static bool IsWildcardPrincipal(JsonNode? statement) =>
        statement?["Principal"] is JsonValue v && v.GetValue<string>() == "*"
        || statement?["Principal"]?["AWS"]?.ToString() == "*";

    private static bool IsScoped(JsonNode statement, string topicArn) =>
        statement["Effect"]?.GetValue<string>() == "Allow"
        && statement["Action"]?.GetValue<string>() == "sqs:SendMessage"
        && statement["Resource"]?.GetValue<string>() == QueueArn
        && statement["Principal"] is JsonObject principal
        && principal.Count == 1
        && principal["Service"]?.GetValue<string>() == "sns.amazonaws.com"
        && statement["Condition"] is JsonObject condition
        && condition.Count == 1
        && condition["ArnEquals"] is JsonObject arnEquals
        && arnEquals.Count == 1
        && arnEquals["aws:SourceArn"]?.GetValue<string>() == topicArn;

    /// <summary>A generated existing policy, the unrelated statement ids it carries, and requested topics.</summary>
    public sealed record Scenario(string? ExistingJson, System.Collections.Generic.IReadOnlyList<string> UnrelatedSids, System.Collections.Generic.IReadOnlyList<string> TopicArns);

    public static Arbitrary<Scenario> ScenarioArbitrary() => Arb.From(
        from topicCount in Gen.Choose(0, 6)
        from topicIdx in Gen.ArrayOf(Gen.Choose(0, 4), topicCount)
        from unrelatedCount in Gen.Choose(0, 3)
        from kind in Gen.Choose(0, 3)
        from stale in Gen.ArrayOf(Gen.Choose(0, 2), 5)
        select Build(topicIdx, unrelatedCount, kind, stale));

    private static Scenario Build(int[] topicIdx, int unrelatedCount, int kind, int[] stale)
    {
        var topics = topicIdx.Select(TopicArn).ToList();
        if (kind == 0)
        {
            return new Scenario(null, Array.Empty<string>(), topics);
        }

        var statements = new JsonArray();
        var unrelated = Enumerable.Range(0, unrelatedCount).Select(i => $"Other{i}").ToList();
        foreach (var sid in unrelated)
        {
            statements.Add(new JsonObject
            {
                ["Sid"] = sid,
                ["Effect"] = "Allow",
                ["Principal"] = new JsonObject { ["AWS"] = "arn:aws:iam::123456789012:root" },
                ["Action"] = "sqs:ReceiveMessage",
                ["Resource"] = QueueArn,
            });
        }

        if (kind >= 2)
        {
            // Pre-existing statements for some topics: 1 = stale wildcard, 2 = already correct.
            for (var i = 0; i < 5; i++)
            {
                if (stale[i] == 0)
                {
                    continue;
                }

                statements.Add(new JsonObject
                {
                    ["Sid"] = QueuePolicyDocument.SidFor(TopicArn(i)),
                    ["Effect"] = "Allow",
                    ["Principal"] = stale[i] == 1 ? JsonValue.Create("*") : new JsonObject { ["Service"] = "sns.amazonaws.com" },
                    ["Action"] = "sqs:SendMessage",
                    ["Resource"] = QueueArn,
                });
            }
        }

        var existing = new JsonObject { ["Version"] = "2012-10-17", ["Statement"] = statements };
        var usedPreexisting = kind >= 2
            ? Enumerable.Range(0, 5).Where(i => stale[i] != 0).Select(TopicArn)
            : Enumerable.Empty<string>();

        // Pre-existing owned statements for topics not requested are unrelated to this merge and stay.
        var requested = topics.ToHashSet(StringComparer.Ordinal);
        var extraKept = usedPreexisting.Where(t => !requested.Contains(t)).Select(QueuePolicyDocument.SidFor);

        return new Scenario(existing.ToJsonString(), unrelated.Concat(extraKept).ToList(), topics);
    }

    private static string TopicArn(int i) => $"arn:aws:sns:us-east-1:123456789012:topic-{i}";
}
