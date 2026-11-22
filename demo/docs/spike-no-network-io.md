# Spike: does building a transport client perform network I/O?

Task 1.2. Requirements 11.1 and 11.2. Gates the scope of `ProviderCompositionSelfCheck` (task 8.1, design D4).

## Method

`demo/tests/Demo.Infrastructure.Tests/Spike/NoNetworkIoSpikeTests.cs` points each client at a local loopback
`TcpListener` and builds it without starting, subscribing or producing. After the build the test waits 2 s
and counts accepted connections. It also asserts that the build itself returns in under 5 s.

Run: `dotnet test demo/tests/Demo.Infrastructure.Tests --filter "Category!=Integration"`.
The 2 s window is short, so a client that connects later than that would not be caught.

## Results

| Provider | What was built | Connections accepted | Build time |
|---|---|---|---|
| RabbitMQ | `RabbitMqBusAdapter.Create` (role `Both`, consumer endpoint registered, bus not started) | 0 | under 5 s |
| Amazon SQS | `AmazonSqsBusAdapter.Create` (role `Both`, `ServiceUrl` on the listener, bus not started; topology client stub throws if called) | 0 | under 5 s |
| Kafka config | `KafkaClientConfigFactory.CreateProducerConfig`, `CreateConsumerConfig`, `CreateAdminClientConfig` | 0 | under 5 s |
| Kafka consumer | `new ConfluentKafkaConsumer(...)`, no subscribe | more than 0 (2 on the first run) | under 5 s |
| Kafka producer | `new ConfluentKafkaProducer(...)`, no produce | more than 0 (3 on the first run) | under 5 s |

## Conclusions

- RabbitMQ and Amazon SQS: building the bus performs no network I/O. The provider composition self-check
  may build these adapters.
- Kafka: the library factory (`KafkaClientConfigFactory`) is pure and performs no I/O. Constructing the
  Confluent producer or consumer does connect, because librdkafka opens connections to the bootstrap servers
  from background threads as soon as the client handle exists. The build call returns quickly, but the
  connection attempts follow it.

## Impact on the startup self-check (design D4)

For Kafka, the self-check is limited to the validators, descriptor resolution and `KafkaClientConfigFactory`.
It must not construct `ConfluentKafkaProducer`, `ConfluentKafkaConsumer` or `ConfluentKafkaTransportProbe`.
For RabbitMQ and Amazon SQS, the self-check may also build the bus adapter without starting it.

Consequence for the demo: the Kafka clients are built only when the Kafka provider is selected and started,
never during a self-check or for an unselected provider.

The two Kafka client tests assert the observed behavior (connections greater than 0). If a librdkafka upgrade
stops connecting at build time, those tests fail and this conclusion should be revisited.
