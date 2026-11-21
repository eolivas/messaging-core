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

// Feature: sns-sqs-provider, Property 9: Topology reconciliation is complete, non-destructive, and idempotent
// Validates: Requirements 5.1, 5.2, 5.9, 11.6.
//
// For any plan (one queue subscribed to N topics, FIFO or standard) and any preset existing state,
// ApplyAsync(autoCreate: true) leaves every planned dead-letter queue, queue, topic, and subscription in
// place with exactly one subscription per (topic, queue) pair. Resources that existed before are not
// modified (a queue policy may gain a statement). A second ApplyAsync changes no state and makes no
// mutating call.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "9")]
public class SnsSqsTopologyReconciliationPropertyTests
{
    private const string Marker = "PreExistingMarker";

    // Feature: sns-sqs-provider, Property 9: Topology reconciliation is complete, non-destructive, and idempotent
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsTopologyReconciliationPropertyTests) })]
    public Property Reconciliation_ForAnyPlanAndExistingState_IsCompleteNonDestructiveAndIdempotent(ReconcileCase c)
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
        var presetQueues = new List<string>();
        if (c.DlqPreset)
        {
            presetQueues.Add(dlqSpec.Name);
        }

        if (c.QueuePreset)
        {
            presetQueues.Add(queueSpec.Name);
        }

        var presetTopics = new List<string>();
        foreach (var name in presetQueues)
        {
            var attrs = new Dictionary<string, string> { [Marker] = name };
            if (c.Fifo)
            {
                attrs["FifoQueue"] = "true";
            }

            client.PresetQueue(name, attrs);
        }

        var presetSubscriptionTopics = new List<string>();
        for (var i = 0; i < plan.Topics.Count; i++)
        {
            if (!c.TopicPreset[i])
            {
                continue;
            }

            var arn = client.PresetTopic(plan.Topics[i].Name, new Dictionary<string, string> { [Marker] = "t" });
            presetTopics.Add(plan.Topics[i].Name);
            if (c.SubscriptionPreset[i])
            {
                client.PresetSubscription(arn, FakeSnsSqsTopologyClient.QueueArnFor(queueSpec.Name));
                presetSubscriptionTopics.Add(plan.Topics[i].Name);
            }
        }

        var before = presetQueues.ToDictionary(
            n => n, n => client.SnapshotQueueAttributes(FakeSnsSqsTopologyClient.QueueUrlFor(n))!);
        var presetSubscriptionArns = presetSubscriptionTopics.ToDictionary(
            t => t, t => SubscriptionsOf(client, t).Select(s => s.SubscriptionArn).ToArray());

        var manager = new SnsSqsTopologyManager(client, settings, NullLogger<SnsSqsTopologyManager>.Instance);
        manager.ApplyAsync(plan, autoCreate: true, CancellationToken.None).GetAwaiter().GetResult();

        // (1) Completeness: every planned resource exists; exactly one subscription per pair.
        var queueArn = FakeSnsSqsTopologyClient.QueueArnFor(queueSpec.Name);
        var complete = plan.Queues.All(q =>
                client.SnapshotQueueAttributes(FakeSnsSqsTopologyClient.QueueUrlFor(q.Name)) is not null)
            && plan.Topics.All(t => client.GetTopicArnAsync(t.Name, CancellationToken.None)
                .GetAwaiter().GetResult() is not null)
            && plan.Subscriptions.All(s =>
                SubscriptionsOf(client, s.TopicName).Count(x => x.Protocol == "sqs" && x.Endpoint == queueArn) == 1);

        // (2) Non-destructive: pre-existing queue attributes keep their values (Policy may be extended),
        // and pre-existing subscriptions keep their ARNs.
        var preserved = before.All(pair =>
        {
            var after = client.SnapshotQueueAttributes(FakeSnsSqsTopologyClient.QueueUrlFor(pair.Key))!;
            return pair.Value.All(kv => kv.Key == "Policy" || (after.TryGetValue(kv.Key, out var v) && v == kv.Value));
        }) && presetSubscriptionArns.All(pair =>
        {
            var now = SubscriptionsOf(client, pair.Key).Select(s => s.SubscriptionArn).ToArray();
            return pair.Value.All(now.Contains);
        });

        // (3) Idempotent: a second run changes no state and makes no mutating call.
        var stateBefore = Snapshot(client, plan);
        var mutationsBefore = client.MutationCount;
        manager.ApplyAsync(plan, autoCreate: true, CancellationToken.None).GetAwaiter().GetResult();
        var idempotent = client.MutationCount == mutationsBefore && Snapshot(client, plan) == stateBefore;

        return (complete && preserved && idempotent).ToProperty().Label(
            $"complete={complete} preserved={preserved} idempotent={idempotent} case={c}");
    }

    private static IReadOnlyList<SnsSubscriptionInfo> SubscriptionsOf(FakeSnsSqsTopologyClient client, string topicName) =>
        client.ListSubscriptionsByTopicAsync(FakeSnsSqsTopologyClient.TopicArnFor(topicName), CancellationToken.None)
            .GetAwaiter().GetResult();

    // A canonical text form of all planned state, for equality comparison.
    private static string Snapshot(FakeSnsSqsTopologyClient client, TopologyPlan plan)
    {
        var queues = plan.Queues.OrderBy(q => q.Name, StringComparer.Ordinal).Select(q =>
        {
            var a = client.SnapshotQueueAttributes(FakeSnsSqsTopologyClient.QueueUrlFor(q.Name));
            return q.Name + "{" + string.Join(
                ",", (a ?? new Dictionary<string, string>()).OrderBy(x => x.Key, StringComparer.Ordinal)
                    .Select(x => x.Key + "=" + x.Value)) + "}";
        });
        var subs = plan.Topics.OrderBy(t => t.Name, StringComparer.Ordinal).Select(t =>
            t.Name + "[" + string.Join(
                ",", SubscriptionsOf(client, t.Name).Select(s => s.SubscriptionArn + "@" + s.Endpoint)
                    .OrderBy(x => x, StringComparer.Ordinal)) + "]");
        return string.Join(";", queues.Concat(subs));
    }

    // ----- Generators -------------------------------------------------------------------------

    /// <summary>A generated FIFO switch and preset existing state for each planned resource.</summary>
    public sealed record ReconcileCase(
        bool Fifo, bool DlqPreset, bool QueuePreset, bool[] TopicPreset, bool[] SubscriptionPreset)
    {
        public override string ToString() =>
            $"Fifo={Fifo} Dlq={DlqPreset} Queue={QueuePreset} " +
            $"Topics=[{string.Join(",", TopicPreset)}] Subs=[{string.Join(",", SubscriptionPreset)}]";
    }

    public static Arbitrary<ReconcileCase> ReconcileCaseArbitrary() => Arb.From(ReconcileCaseGen());

    private static Gen<ReconcileCase> ReconcileCaseGen() =>
        from fifo in Gen.Elements(true, false)
        from topicCount in Gen.Choose(0, 5)
        from dlq in Gen.Elements(true, false)
        from queue in Gen.Elements(true, false)
        from topics in Gen.ArrayOf(Gen.Elements(true, false), topicCount)
        from subs in Gen.ArrayOf(Gen.Elements(true, false), topicCount)
        select new ReconcileCase(fifo, dlq, queue, topics, subs);
}
