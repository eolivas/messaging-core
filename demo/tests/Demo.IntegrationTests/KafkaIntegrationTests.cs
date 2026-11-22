using Demo.IntegrationTests.Support;

namespace Demo.IntegrationTests;

[Trait("Category", "Integration")]
public sealed class KafkaIntegrationTests : IClassFixture<KafkaFixture>
{
    private readonly KafkaFixture _broker;

    public KafkaIntegrationTests(KafkaFixture broker) => _broker = broker;

    [Fact]
    public Task QuoteAccepted_OneEvent_CreatesOneCustomerAndOneEmail() =>
        Scenarios.EventReachesBothConsumersExactlyOnceAsync(_broker);

    [Fact]
    public Task QuoteAccepted_TwoConsumers_EachReceivesOwnCopy() =>
        Scenarios.EachConsumerReceivesItsOwnCopyAsync(_broker);

    [Fact]
    public Task FailOnce_CustomerConsumer_RetriesThenSucceeds() =>
        Scenarios.FailOnceRetriesThenSucceedsAsync(_broker);

    [Fact]
    public Task AlwaysFail_CustomerConsumer_DeadLettersAndOtherConsumerSucceeds() =>
        Scenarios.AlwaysFailDeadLettersWithoutAffectingOtherConsumerAsync(_broker);
}
