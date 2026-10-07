---
inclusion: fileMatch
fileMatchPattern: 'Dockerfile*|**/*.yml|**/*.yaml|docker-compose*'
---

# Infrastructure, Containers & Service Boundaries

Rules for containerization, deployment, and inter-service design.

## Database engine consistency
- One engine — **PostgreSQL** — across every environment: local, integration tests, staging, prod.
- Same major version everywhere. No lite/embedded/in-memory substitute standing in for the real database. Behavior must never diverge by environment.

## Docker across the lifecycle

### Local development
- `docker-compose` runs the dependencies: PostgreSQL and the broker (RabbitMQ/Kafka).
- The service runs on the host for fast debugging and connects to the containerized dependencies.
- Compose owns local config and seed data; `docker compose up` gives a new dev a working environment in one command.
- Volumes persist local DB data between runs; this data is disposable, never authoritative.

### Integration tests
- Spin up ephemeral, isolated containers of the same DB and broker, run, then tear down (Testcontainers preferred; compose acceptable in CI).
- Same engine and major version as production — no in-memory or lite substitute.
- Each run starts from a known clean state (fresh container or migrated + truncated); no run depends on another's leftovers.
- Migrations are applied the same way as in prod, so migration bugs surface here.

### Staging & production
- Docker packages the *service* as the deployable artifact: multi-stage build, runtime-only final image.
- The same built image is promoted from staging to prod unchanged; only injected configuration differs.
- Config and secrets come from the environment / secret store at runtime — never baked into the image, never environment-specific images.
- Containers are stateless with CPU/memory limits and liveness/readiness probes; persistent state lives only in the DB, broker, or external volumes.
- Base image tags are pinned (digest or explicit version — never `latest`); containers run as a non-root user.
- Keep images small: `.dockerignore` source, build artifacts, SDK, and test tooling.

## Microservices & service boundaries
- A service owns its data. No service touches another service's database directly — cross-service data goes through APIs or events.
- Prefer async messaging (events) over synchronous calls; use sync HTTP only when a request/response answer is genuinely needed now.
- Consumers are idempotent; assume at-least-once delivery and possible reordering. Dedupe on a message/business key.
- Use the outbox pattern for "update state + publish event" so the two can't diverge.
- Message contracts are versioned and backward-compatible: add fields, don't repurpose or remove without a new version.
- No shared library couples services at the domain level; shared code is limited to cross-cutting concerns (logging, contracts).
- Every service exposes health/readiness endpoints and propagates correlation ids across calls and messages.
- Define failure behavior explicitly: timeouts, retries with backoff, and dead-letter handling for poison messages.
