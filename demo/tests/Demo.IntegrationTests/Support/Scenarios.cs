using Demo.Application.Sessions;
using Demo.Contracts;

namespace Demo.IntegrationTests.Support;

/// <summary>The provider-independent scenarios; each provider test class runs them against its own broker.</summary>
public static class Scenarios
{
    /// <summary>One event: exactly one Mock_Customer and one Mock_Email for the LeadId within 30 s.</summary>
    public static async Task EventReachesBothConsumersExactlyOnceAsync(BrokerFixture broker)
    {
        await using var session = await Session.StartAsync(broker);

        var outcome = await session.TriggerAsync();
        var lead = outcome.LeadId!;

        await Session.WaitUntilAsync(
            () => session.Flow.CountForLead(MicroserviceId.Customer, FlowStepKind.CustomerCreated, lead) >= 1
                  && session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead) >= 1,
            $"Customer_Created and Email_Sent for lead {lead}");
        await Session.SettleAsync();

        Assert.Equal(1, session.Flow.CountForLead(MicroserviceId.Customer, FlowStepKind.CustomerCreated, lead));
        Assert.Equal(1, session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead));
    }

    /// <summary>Each consumer receives its own copy of the one published event.</summary>
    public static async Task EachConsumerReceivesItsOwnCopyAsync(BrokerFixture broker)
    {
        await using var session = await Session.StartAsync(broker);

        var outcome = await session.TriggerAsync();
        var correlation = outcome.CorrelationId!;

        await Session.WaitUntilAsync(
            () => session.Flow.Count(MicroserviceId.Customer, FlowStepKind.Delivered, correlation) >= 1
                  && session.Flow.Count(MicroserviceId.Communications, FlowStepKind.Delivered, correlation) >= 1,
            $"a delivery to each consumer for correlation id {correlation}");
        await Session.SettleAsync();

        Assert.Equal(1, session.Flow.Count(MicroserviceId.SalesLeads, FlowStepKind.Published, correlation));
        Assert.Equal(1, session.Flow.Count(MicroserviceId.Customer, FlowStepKind.Delivered, correlation));
        Assert.Equal(1, session.Flow.Count(MicroserviceId.Communications, FlowStepKind.Delivered, correlation));
    }

    /// <summary>Fail once on Customer: it retries, then succeeds. Communications is unaffected.</summary>
    public static async Task FailOnceRetriesThenSucceedsAsync(BrokerFixture broker)
    {
        await using var session = await Session.StartAsync(broker);
        session.SetFailureMode(ConsumerId.Customer, FailureMode.FailOnce);

        var outcome = await session.TriggerAsync();
        var lead = outcome.LeadId!;
        var correlation = outcome.CorrelationId!;

        await Session.WaitUntilAsync(
            () => session.Flow.CountForLead(MicroserviceId.Customer, FlowStepKind.CustomerCreated, lead) >= 1
                  && session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead) >= 1,
            $"Customer_Created after a retry and Email_Sent for lead {lead}");
        await Session.SettleAsync();

        Assert.True(session.Flow.Count(MicroserviceId.Customer, FlowStepKind.Retrying, correlation) >= 1);
        Assert.Equal(1, session.Flow.CountForLead(MicroserviceId.Customer, FlowStepKind.CustomerCreated, lead));
        Assert.Equal(0, session.Flow.Count(MicroserviceId.Customer, FlowStepKind.DeadLettered, correlation));
        Assert.Equal(1, session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead));
        Assert.Equal(0, session.Flow.Count(MicroserviceId.Communications, FlowStepKind.Retrying, correlation));
    }

    /// <summary>Always fail on Customer: the message is dead-lettered and no Mock_Customer exists. Communications is unaffected.</summary>
    public static async Task AlwaysFailDeadLettersWithoutAffectingOtherConsumerAsync(BrokerFixture broker)
    {
        await using var session = await Session.StartAsync(broker);
        session.SetFailureMode(ConsumerId.Customer, FailureMode.AlwaysFail);

        var outcome = await session.TriggerAsync();
        var lead = outcome.LeadId!;
        var correlation = outcome.CorrelationId!;

        await Session.WaitUntilAsync(
            () => session.Flow.Count(MicroserviceId.Customer, FlowStepKind.DeadLettered, correlation) >= 1
                  && session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead) >= 1,
            $"Dead_Lettered on Customer and Email_Sent for lead {lead}");
        await Session.SettleAsync();

        Assert.Equal(1, session.Flow.Count(MicroserviceId.Customer, FlowStepKind.DeadLettered, correlation));
        Assert.Equal(0, session.Flow.CountForLead(MicroserviceId.Customer, FlowStepKind.CustomerCreated, lead));
        Assert.Equal(1, session.Flow.CountForLead(MicroserviceId.Communications, FlowStepKind.EmailSent, lead));
    }
}
