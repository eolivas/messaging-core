// One broker container at a time. Starting RabbitMQ, LocalStack and Kafka together on a small
// Docker Desktop VM (about 4 GB) kept all three from becoming ready within the 60 s budget.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
