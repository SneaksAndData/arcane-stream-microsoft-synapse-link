set shell := ["bash", "-c"]

COMPOSE_FILE := "test-resources/docker-compose.yaml"
ENV_FILE := "test-resources/integration-tests.env"

# List available recipes
default:
    @just --list

build:
    mise exec -- sbt assembly

docker-build version="dev":
    docker build \
        -f .container/Dockerfile \
        --build-arg GITHUB_TOKEN="$GITHUB_TOKEN" \
        -t ghcr.io/sneaksanddata/arcane-stream-microsoft-synapse-link:{{ version }} \
        .

clean:
    mise exec -- sbt clean

# Start test services in background
up *args="":
    docker compose -f {{COMPOSE_FILE}} up -d {{args}}

# Stop test services
stop *args="":
    docker compose -f {{COMPOSE_FILE}} down {{args}}

# Restart test services
fresh *args="":
    docker compose -f {{COMPOSE_FILE}} restart {{args}}

# View logs for test services
logs *args="":
    docker compose -f {{COMPOSE_FILE}} logs {{args}}

# View test services status
ps:
    docker compose -f {{COMPOSE_FILE}} ps

# NOTE: defaults to Test/testFull because under sbt 2 a bare `test` delegates to
# `testQuick`, which silently selects no tests and still exits successfully,
# hiding integration test failures.
# Run tests with integration-tests.env loaded
test *args="Test/testFull":
    export $(cat {{ENV_FILE}} | xargs) && sbt "{{args}}"

# Accepts a bare class name (StreamRunner), an explicit glob (*StreamRunner*),
# or a direct fully-qualified test address (com.sneaksanddata...StreamRunner).
# Run specific tests with integration-tests.env loaded
test-only *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export $(cat {{ENV_FILE}} | xargs)
    ARGS="{{args}}"
    if [[ "$ARGS" == *"*"* ]] || [[ "$ARGS" == *" "* ]] || [[ "$ARGS" == *"."* ]]; then
        # explicit glob, multiple selectors, or direct test address: pass through as-is
        sbt "Test/testOnly ${ARGS}"
    else
        # bare class name: match it anywhere in the fully-qualified name
        sbt "Test/testOnly *${ARGS}*"
    fi

# Validate formatting
fmt-check:
    sbt scalafmtCheckAll

# Format code
fmt:
    sbt scalafmtAll

check: fmt-check

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'just stop' EXIT

    just up
    docker compose -f {{COMPOSE_FILE}} wait lakekeeper_migrate lakekeeper_prepare
    just test

stream debug="":
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f dev.env ]]; then
        echo "Missing dev.env, create it locally before running this command (see dev.env.example)." >&2
        exit 1
    fi

    log_level="INFO"
    if [[ "{{ debug }}" == "--debug" ]]; then
        log_level="DEBUG"
    elif [[ -n "{{ debug }}" ]]; then
        echo "Unknown stream option: {{ debug }}. Did you mean '--debug'?" >&2
        exit 1
    fi

    just build
    mise exec --env dev -- env STREAMCONTEXT__BACKFILL=${STREAMCONTEXT__BACKFILL:-false} java -DLOG_LEVEL="${log_level}" -Dlogback.configurationFile=src/main/resources/logback.xml -Dscala.concurrent.context.numThreads=2 -Dscala.concurrent.context.maxThreads=2 -jar target/com.sneaksanddata.arcane.microsoft-synapse-link.assembly.jar

backfill debug="":
    STREAMCONTEXT__BACKFILL=true just stream "{{ debug }}"
