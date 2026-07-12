# connector-k8s

The Henosis Kubernetes reconciler.

The service implements the generated `henosis.v1.ConnectorService` contract. It durably accepts
complete graph slices, provisions the real platform renderer through the D24 `prepare-runner.sh`
recipe, renders and validates the whole Kubernetes world, atomically publishes the result to the
deploy repository's `env/<environment>` branch, and reports one complete observation to core.

The connector-owned bytes that authoring and collector integrations place in
`Component.context` are specified in [docs/component-context-v1.md](docs/component-context-v1.md).
The format is strict and versioned; it carries the environment identity, source repository and
immutable revision, and image digest needed by the current renderer.

Per-environment direct and pull-request-gated publication, including the reusable human/machine
review projection, are specified in
[docs/review-projection-v1.md](docs/review-projection-v1.md).

## Service configuration

| Variable | Default | Purpose |
|---|---|---|
| `HENOSIS_BIND` | `0.0.0.0:8081` | ConnectRPC listen address |
| `HENOSIS_CORE_URL` | `http://core:8080` | Core callback service origin |
| `HENOSIS_STATE_DIR` | `/var/lib/henosis-connector-k8s/state` | Durable accepted levels and publication checkpoints |
| `HENOSIS_PREPARE_RUNNER` | `/opt/henosis/platform/scripts/prepare-runner.sh` | Platform D24 recipe |
| `HENOSIS_PLATFORM_REF` | `origin/main` | Platform ref resolved to an immutable SHA per invocation |
| `HENOSIS_PLATFORM_CHECKOUT` | `/var/lib/henosis-connector-k8s/platform` | Recipe-managed platform checkout |
| `HENOSIS_RUNNER_CACHE_DIR` | `/var/lib/henosis-connector-k8s/runner-cache` | Recipe-managed SHA cache |
| `HENOSIS_DEPLOY_REMOTE` | `https://github.com/henosis-playground/deploy.git` | Desired-state repository; other GitHub orgs are rejected |
| `HENOSIS_GITHUB_TOKEN_FILE` | `/run/secrets/github-pat` | PAT file read by Git askpass |
| `HENOSIS_PUBLICATION_POLICIES` | `{"default":"direct","environments":{}}` | Strict JSON default and per-environment `direct`/`pr-gated` policies |

## Layout

<!-- LINT.IfChange(layout_rules) -->
- `crates/` — library crates and reusable supporting crates
- `tests/` — workspace member crates that build integration and end-to-end test binaries
- `crates/workspace-hack/` — cargo-hakari dependency unification (auto-generated, do not edit)
- Reusable test harnesses, fixtures, and helpers belong in `crates/`, not `tests/`
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`
<!-- LINT.ThenChange(//AGENTS.md:layout_rules) -->

## Commands

Use `just` to discover and run common tasks:

<!-- LINT.IfChange(command_recipes) -->
- `just lint` — run all lints (fmt, clippy, deny, pre-commit). Always run after making changes.
- `just test` — run all tests with optimized third-party dependencies
- `just doc` — build docs
<!-- LINT.ThenChange(//AGENTS.md:command_recipes) -->
