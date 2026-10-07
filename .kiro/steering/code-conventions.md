---
inclusion: fileMatch
fileMatchPattern: '**/*.cs'
---

# Code Conventions & Architecture Rules

Project-specific C# rules. These override general defaults. Where a rule
conflicts with a library's idiom, follow the rule and flag the conflict.

## Layering (Clean Architecture)
- Dependencies point inward: `Domain` ← `Application` ← `Infrastructure` / `Api`.
- `Domain` has no dependencies on other layers or external packages (no EF, no framework types).
- Interfaces (ports) are declared in `Domain`/`Application`; implementations (adapters) live in `Infrastructure`.
- No layer skips a layer: `Api` reaches `Infrastructure` only through `Application` abstractions.

## SOLID — concrete bars
- **SRP:** one public responsibility per class. Handlers do one thing; no "manager"/"helper" grab-bags.
- **DIP:** depend on interfaces for anything that crosses a layer boundary or does I/O. Constructor injection only — no service locator, no static singletons for dependencies.
- **OCP/LSP:** prefer composition over inheritance. Abstract base classes only for genuine shared behavior, not shared fields.
- **ISP:** small, consumer-driven interfaces. No fat interfaces that force empty implementations.

## CQRS & Application layer
- Commands mutate and return void / an id / a result object — not domain entities.
- Queries never mutate and return DTOs/read models, not domain entities.
- One handler per command/query: `<Action><Entity>CommandHandler` / `...QueryHandler`.
- Validation lives in the Application layer (e.g. FluentValidation), not in controllers.

## Persistence
- Writes go through EF via a repository/unit-of-work abstraction.
- Read-heavy or hot-path queries may use Dapper behind a query interface — document why in a comment.
- Parameterized queries only (EF or Dapper with parameters). No string-concatenated SQL, ever — including dynamic query builders.
- No business logic in the database. No raw SQL in `Api`/`Application`; keep it in `Infrastructure`.
- Migrations are reviewed and never auto-applied to production from app startup.

## APIs
- Endpoints return typed result/DTO objects and a consistent error contract (Problem Details).
- Version the API; breaking changes get a new version, never a mutated existing one.
- No domain entities serialized over the wire — map to DTOs.

## Security (code-level)
- Secrets never in source, config, or logs. Load from the environment / secret store; connection strings and keys are injected, not committed.
- Treat all input as untrusted at the boundary: validate and bind to typed models in the Application layer before anything reaches the domain. Reject unknown fields.
- Authenticate at the API edge; authorize per endpoint (authenticated ≠ allowed). Authorization decisions live in the Application layer.
- Treat message payloads as untrusted on consume: bind to known contracts, never deserialize into arbitrary types.
- Never log secrets, tokens, or PII-bearing raw payloads; reference sensitive values by key name, redact before the log call.
- Don't leak internals (stack traces, SQL, infra detail) in API responses — generic Problem Details to the client, detail to logs.

## Error handling & observability
- Fail fast on invalid state; throw domain-specific exceptions, not bare `Exception`.
- Structured logging only. Include a correlation id on every request.
- Log where the error is handled, not at every layer it passes through (no double-logging).

## Testing
- Every command/query handler has unit tests; domain logic is covered at the unit level (xUnit).
- Integration tests cover persistence and messaging against real containerized infrastructure (see `infrastructure.md`).
- Arrange-Act-Assert; one logical assertion focus per test. Name as `Method_Scenario_ExpectedResult`.
- No test depends on another test's state or on execution order.

## Naming & structure
- Async methods end in `Async` and take a `CancellationToken` where they do I/O.
- One public type per file; file name matches the type.
- Feature-first folders under each layer (group by feature, not by technical kind).
