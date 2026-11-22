# Messaging Demo App

A local, single-user demo of the Messaging Core library. A React Frontend
talks to a .NET Backend_For_Frontend (BFF). You pick a provider, review its
defaults, start it, and click "Quote Accepted". Three mock microservices
(Sales Leads, Customer, Communications) exchange that event through the
selected provider and the Frontend animates the flow.

Everything lives under `demo/`. The library under `src/`, `tests/`,
`MessagingCore.sln`, `global.json` and the root `docker-compose.yml` are not
modified by the demo.

| Component | URL |
|---|---|
| Frontend | http://localhost:5173 |
| BFF (Demo.Api) | http://localhost:5080 |

## Prerequisites

| Tool | Minimum version | Used for |
|---|---|---|
| .NET SDK | 8.0.100 (`global.json` at the repo root pins it, `rollForward: latestFeature`) | BFF, microservices, backend tests |
| Docker with Compose v2 (`docker compose`) | Docker Engine 24+, Compose 2.20+ (the broker script uses `up --wait`) | RabbitMQ, Kafka, LocalStack, integration tests |
| Node.js and npm | Node 18+ (Node 20 LTS recommended), npm 9+ | Vite Frontend in `demo/src/frontend` |
| Bash | any | `demo/scripts/start-broker.sh` |

Docker must be running before you start a broker or run integration tests.

## Layout

```
demo/
  MessagingDemo.sln
  docker-compose.yml          RabbitMQ only
  scripts/start-broker.sh     one command per provider
  src/Demo.Api                BFF (Minimal API, SSE endpoint)
  src/Demo.Application        handlers, session state, ports
  src/Demo.Infrastructure     provider compositions, microservice hosts
  src/Demo.Contracts          shared contract and telemetry port
  src/Demo.SalesLeads         mock publisher microservice
  src/Demo.Customer           mock consumer microservice
  src/Demo.Communications     mock consumer microservice
  src/frontend                React + TypeScript + Vite
  tests/                      unit, architecture and integration tests
```

## Build

Run from the repository root.

```bash
dotnet build demo/MessagingDemo.sln
cd demo/src/frontend && npm install
```

## Start

Use three steps: broker, BFF, Frontend. Each runs in its own terminal.

### 1. Start one broker (pick the provider you want to demo)

```bash
demo/scripts/start-broker.sh rabbitmq   # demo/docker-compose.yml, ports 5672, 15672
demo/scripts/start-broker.sh kafka      # root docker-compose.yml, port 9092
demo/scripts/start-broker.sh sns-sqs    # root docker-compose.yml (LocalStack), port 4566
```

The script checks that Docker runs and that the ports are free, starts the
container, and waits up to 60 s for it to become healthy. On failure it
prints the container logs, removes the container and exits non-zero.

Note on RabbitMQ: the container can report healthy (the `ping` health check)
a few seconds before the broker accepts logins. If the first Start in the
Frontend fails with a connection or authentication error, wait 30 to 60
seconds and retry.

Azure Service Bus is listed in the Frontend as unavailable. Messaging Core
has no Azure Service Bus transport yet, so it cannot be started.

### 2. Start the BFF

```bash
dotnet run --project demo/src/Demo.Api
```

The BFF listens on http://localhost:5080 (`launchSettings.json`). Local
connection settings come from `demo/src/Demo.Api/appsettings.Development.json`
(throwaway local-only values: `guest/guest` for RabbitMQ, `test/test` for
LocalStack). Environment variables override them, for example
`Demo__RabbitMq__ConnectionString`. The BFF exits non-zero at startup if a
required setting is missing or empty.

### 3. Start the Frontend

```bash
cd demo/src/frontend
npm run dev
```

Open http://localhost:5173. The Frontend reads the BFF address from
`VITE_BFF_BASE_URL` (use `http://localhost:5080`). The BFF only accepts
cross-origin calls from `Demo:FrontendOrigin`, which defaults to
`http://localhost:5173`.

## Stop

Stop the Frontend and the BFF with Ctrl+C. Then stop the broker you started:

```bash
# RabbitMQ
docker compose -f demo/docker-compose.yml down

# Kafka
docker compose -f docker-compose.yml down kafka

# SNS/SQS (LocalStack)
docker compose -f docker-compose.yml down localstack
```

Run the Kafka and LocalStack commands from the repository root. Use
`docker compose -f docker-compose.yml down` without a service name to stop
both.

## Tests

Unit tests (no Docker needed), from the repository root:

```bash
dotnet test demo/MessagingDemo.sln --filter "Category!=Integration"
cd demo/src/frontend && npm run test -- --run
```

Integration tests (Testcontainers, Docker must be running):

```bash
dotnet test demo/MessagingDemo.sln --filter "Category=Integration"
```

Do not run a plain `dotnet test` on the solution: it mixes unit and container
tests. Check `docker ps` afterwards for leftover containers.

## Library follow-ups

The demo works around the following behaviors of Messaging Core without
changing `src/`. Each workaround sits in one composition class per provider in
`Demo.Infrastructure`, so removing it after a library change is a small edit.
Details are in the spec's `design.md`.

| ID | Follow-up for the library |
|---|---|
| F1 | Wire the Amazon SQS seams in `AddMassTransitMessagingProvider` as the `sns-sqs-provider` design describes. Today it only wires RabbitMQ. |
| F2 | Let the RabbitMQ DI path accept a receive-queue name per consumer. Today every consumer uses `messaging-core`. |
| F3 | Add a hosted service that starts and stops the RabbitMQ bus. |
| F4 | Call `ObservabilityContext.OnRetry` from the retry path so `IMessagingLog` receives `Retry` events. |
| F5 | Provide `IDeadLetterSink` implementations for RabbitMQ and Kafka. Only `SqsDeadLetterSink` exists. |
| F6 | Add the message type to `IMessagingLog.MessageEvent` and the failure kind to `IDeadLetterLog.DeadLettered`. |
| F7 | `JsonContractBinder` accepts missing or empty fields. The demo adds a validating `IHandlerInvoker`. A stricter binder (or opt-in validation) would remove that need. |
| F8 | Make `AddKafkaMessagingProvider` skip the consumer hosted service for a publisher-only role, and make `AutoOffsetReset` configurable (it is fixed to `Earliest`). |

The design also records F12 (a publisher-only `AddMassTransitMessagingProvider`
cannot resolve `IMessagingProvider` without a consume processor).
