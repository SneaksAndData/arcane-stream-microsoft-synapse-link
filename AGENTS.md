# AGENTS.md

Scala 3 (`3.9.0`) plugin for the Arcane data platform: a single streaming app (`com.sneaksanddata.arcane.microsoft_synapse_link.main`)
that reads Microsoft Synapse Link (Dataverse/D365 CDM) exports from Azure Blob storage and merges them into an Iceberg table. Most logic lives in the
`arcane-framework-scala` dependency; this repo mainly wires up the plugin's DI layers (`src/main/scala/main.scala`),
`MicrosoftSynapseLinkPluginStreamContext`/`MicrosoftSynapseLinkSourceSettings`, and integration tests.

## Prerequisites

- `GITHUB_TOKEN` env var (or a `.ghtoken` file in repo root) must be set/exist — it authenticates against the
  GitHub Maven registry for the `arcane-framework-scala` dependency. **Nothing builds without it.**
- Use `mise` + `just`; do not call `sbt`/`docker` directly unless replicating what a `just` recipe does, since they
  set required env/tool versions (JDK Temurin 25 — see `mise.toml`; sbt 2.0.12 is pinned in `project/build.properties`).

## Commands (via `justfile`)

- `just build` — `sbt assembly`, produces `target/com.sneaksanddata.arcane.microsoft-synapse-link.assembly.jar`.
- `just check` / `just fmt-check` — `sbt scalafmtCheckAll` (run before committing; CI fails the build on formatting drift).
- `just fmt` — `sbt scalafmtAll`, auto-formats.
- `just clean` — `sbt clean`.
- `just up` / `just stop` / `just fresh` / `just logs` / `just ps` — manage the `test-resources/docker-compose.yaml`
  stack directly (MinIO/S2, Azurite, Lakekeeper + Postgres, Trino).
- `just test [sbt-args]` — exports `test-resources/integration-tests.env` then runs `sbt` with the given args (defaults to `Test/testFull`; under sbt 2 a bare `test` selects no tests).
- `just test-only <pattern>` — runs `sbt Test/testOnly` scoped to a class/pattern match, with the same env loaded.
- `just it` — full integration run: `just build`, `just up`, waits for `lakekeeper_migrate`/`lakekeeper_prepare`
  to exit, then `just test`. Tears down compose on exit via `trap`.
- `just stream [--debug]` / `just backfill [--debug]` — runs the assembled JAR locally against `dev.env`
  (copy from `dev.env.example`; the file is required and the recipe aborts if it's missing). `backfill` just sets
  `STREAMCONTEXT__BACKFILL=true` and calls `stream`.
- `just docker-build [tag]` — builds the container image via `.container/Dockerfile`, passing `GITHUB_TOKEN` as a
  build arg.

To scope a single test, use `just test-only StreamRunner` (matches by class name) or
`just test-only "com.sneaksanddata.arcane.microsoft_synapse_link.integration.StreamRunner"` for an exact match
(tests are ZIO-test/ScalaTest specs under `src/test/scala/integration`, and effectively all are integration
tests requiring the Docker Compose stack — there is no separate fast unit-test suite).

## Test suite notes

- Integration tests assume live services at fixed localhost ports from `test-resources/docker-compose.yaml`: Azurite blob `10001`,
  Lakekeeper (Iceberg REST catalog) `20001`, Trino `8080`, Postgres `5432`. Credentials/URIs are hardcoded in
  `test-resources/integration-tests.env` / `test-resources/docker-compose.yaml`.
- CI (`.github/workflows/build.yaml`) uses `just up`, `just logs`, `just test "clean; coverage; Test/testFull; coverageReport"`,
  then `just fmt-check` — reproduce this order locally when validating changes (tests first, formatting check after).

## Formatting

- `.scalafmt.conf`: scalafmt `3.9.6`, `scala3` dialect, `maxColumn = 120`. Run `just check` before pushing.

## Release / CI flow (informational, don't invoke unless asked)

- Container images, Helm charts, and releases are all manual `workflow_dispatch` GitHub Actions
  (`build-container-image.yaml`, `release.yaml`, `publish-ecr-public.yaml`) —
  nothing auto-publishes on push to `main` except the test/coverage workflow.
- `release.yaml` only runs from `main` and uses semver bump inputs `major_v: 2`, `minor_v: 4` — keep these in sync
  with the actual next version if asked to cut a release.

## Helm / Kind

- `./.helm` is the chart for deploying this stream's CRDs/StreamClass on top of the separately-installed
  `arcane-crd` and `arcane-operator` charts. README has the full Kind walkthrough if you need to stand up a local
  cluster for manual verification.
