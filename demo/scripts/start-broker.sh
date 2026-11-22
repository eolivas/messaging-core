#!/usr/bin/env bash
# Starts the broker container for a demo provider and waits until healthy.
# Usage: demo/scripts/start-broker.sh <rabbitmq|kafka|sns-sqs>
set -u

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
demo_dir="$(dirname "$script_dir")"
root_dir="$(dirname "$demo_dir")"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  echo "Usage: $0 <rabbitmq|kafka|sns-sqs>" >&2
  exit 2
}

[ $# -eq 1 ] || usage

case "$1" in
  rabbitmq)
    compose_file="$demo_dir/docker-compose.yml"
    service="rabbitmq"
    container="messaging-demo-rabbitmq"
    ports="5672 15672"
    ;;
  kafka)
    compose_file="$root_dir/docker-compose.yml"
    service="kafka"
    container="messaging-core-kafka"
    ports="9092"
    ;;
  sns-sqs)
    compose_file="$root_dir/docker-compose.yml"
    service="localstack"
    container="messaging-core-localstack"
    ports="4566"
    ;;
  *) usage ;;
esac

command -v docker >/dev/null 2>&1 ||
  fail "docker is not installed (container: $container)."
docker info >/dev/null 2>&1 ||
  fail "Docker is not running; cannot start container $container."

# True when something listens on the given TCP port on this host.
port_in_use() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$1" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1
  fi
}

# A port held by our own already-running container is not a conflict.
already_running="$(docker ps --filter "name=^${container}$" \
  --format '{{.Names}}' 2>/dev/null)"
if [ -z "$already_running" ]; then
  for port in $ports; do
    if port_in_use "$port"; then
      fail "port $port is already in use; cannot start container $container."
    fi
  done
fi

if docker compose -f "$compose_file" up -d --wait --wait-timeout 60 \
  "$service"; then
  echo "Container $container is healthy (service: $service)."
  exit 0
fi

echo "ERROR: container $container (service: $service) failed to become" \
  "healthy within 60 seconds." >&2
docker compose -f "$compose_file" logs --tail 20 "$service" >&2 || true
docker compose -f "$compose_file" rm -sf "$service" >/dev/null 2>&1 || true
exit 1
