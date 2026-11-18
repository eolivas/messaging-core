using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 10: Verify-only never mutates and names every missing resource
// Validates: Requirements 5.6, 5.7.
//
// For any plan and any subset of its resources already present, ApplyAsync(autoCreate: false) makes no
// mutating call. It succeeds when nothing is missing; otherwise it throws
// TopologyResourceMissingException whose name set equals exactly the set of missing resources.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "10")]
public class SnsSqsTopologyVerifyOnlyPropertyTests
{
    // Feature: sns-sqs-provider, Property 10: Verify-only never mutates and names every missing resource
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsTopologyVerifyOnlyPropertyTests) })]
    public Property VerifyOnly_ForAnyPlanAndExistingSubset_NeverMutatesAndNamesEveryMissingResource(VerifyCase c)
    {
        var settings = new AmazonSqsProviderSettings
        {
            Region = "us-east-1",
            EndpointName = "orders",
            Fifo = c.Fifo,
        };
        var contracts = Enumerable.Range(0, c.TopicPreset.Length)
            .Select(i => new ContractRegistration($"a.b.Type{i}", typeof(object)))
            .ToArray();
        var plan = SnsSqsTopologyPlanner.Plan(
            settings, MessagingMode.PublishSubscribe, MassTransitRole.Consumer, contracts);
        var dlqSpec = plan.Queues.Single(q => q.IsDeadLetter);
        var queueSpec = plan.Queues.Single(q => !q.IsDeadLetter);

        var client = new FakeSnsSqsTopologyClient();
        var expected = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (spec, preset) in new[] { (dlqSpec, c.DlqPreset), (queueSpec, c.QueuePreset) })
        {
            if (preset)
            {
                var attrs = new Dictionary<string, string>();
                if (c.Fifo)
                {
                    attrs["FifoQueue"] = "true";
                }

                client.PresetQueue(spec.Name, attrs);
            }
            else
            {
                expected.Add(spec.Name);
            }
        }

        for (var i = 0; i < plan.Topics.Count; i++)
        {
            var topicName = plan.Topics[i].Name;
            var subscribed = false;
            if (c.TopicPreset[i])
            {
                var arn = client.PresetTopic(topicName);
                if (c.SubscriptionPreset[i] && c.QueuePreset)
                {
                    client.PresetSubscription(arn, FakeSnsSqsTopologyClient.QueueArnFor(queueSpec.Name));
                    subscribed = true;
                }
            }
            else
            {
                expected.Add(topicName);
            }

            if (!subscribed)
            {
                expected.Add($"{topicName}->{queueSpec.Name}");
            }
        }

        var manager = new SnsSqsTopologyManager(client, settings, NullLogger<SnsSqsTopologyManager>.Instance);
        TopologyResourceMissingException? thrown = null;
        try
        {
            manager.ApplyAsync(plan, autoCreate: false, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (TopologyResourceMissingException ex)
        {
            thrown = ex;
        }

        var noMutation = client.MutationCount == 0;
        var named = expected.Count == 0
            ? thrown is null
            : thrown is not null
                && thrown.MissingResources.Count == expected.Count
                && thrown.MissingResources.ToHashSet(StringComparer.Ordinal).SetEquals(expected)
                && expected.All(n => thrown.Message.Contains(n, StringComparison.Ordinal));

        return (noMutation && named).ToProperty().Label(
            $"noMutation={noMutation} named={named} expected=[{string.Join(",", expected)}] " +
            $"actual=[{string.Join(",", thrown?.MissingResources ?? Array.Empty<string>())}] case={c}");
    }

    // ----- Generators -------------------------------------------------------------------------

    /// <summary>A generated FIFO switch and preset existing state for each planned resource.</summary>
    public sealed record VerifyCase(
        bool Fifo, bool DlqPreset, bool QueuePreset, bool[] TopicPreset, bool[] SubscriptionPreset)
    {
        public override string ToString() =>
            $"Fifo={Fifo} Dlq={DlqPreset} Queue={QueuePreset} " +
            $"Topics=[{string.Join(",", TopicPreset)}] Subs=[{string.Join(",", SubscriptionPreset)}]";
    }

    public static Arbitrary<VerifyCase> VerifyCaseArbitrary() => Arb.From(VerifyCaseGen());

    private static Gen<VerifyCase> VerifyCaseGen() =>
        from fifo in Gen.Elements(true, false)
        from topicCount in Gen.Choose(0, 5)
        from dlq in Gen.Elements(true, false)
        from queue in Gen.Elements(true, false)
        from topics in Gen.ArrayOf(Gen.Elements(true, false), topicCount)
        from subs in Gen.ArrayOf(Gen.Elements(true, false), topicCount)
        select new VerifyCase(fifo, dlq, queue, topics, subs);
}
