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

check:
    mise exec -- sbt scalafmtCheckAll

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'docker compose down' EXIT

    docker compose up -d
    docker compose wait prepare_buckets lakekeeper_migrate lakekeeper_prepare
    mise exec --env it -- sbt test

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
