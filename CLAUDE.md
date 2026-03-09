# PROJECT

DESCRIPTION

## Layout

- `crates/` — workspace member crates
- `tests/` — test crates (test suites, harnesses, fixtures)
- `crates/workspace-hack/` — cargo-hakari dependency unification (auto-generated, do not edit)

## Commands

Use `just` — run `just` to list all recipes. Prefer just recipes over raw cargo commands.

- `just lint` — run all lints (fmt, clippy, deny, pre-commit). Always run after making changes.
- `just test` — run all tests
- `just doc` — build docs

All recipes accept passthrough flags: `just test -p some-crate`, `just clippy -- -W clippy::pedantic`.

## Version control

If a `.jj/` directory exists, the project uses [jj](https://martinvonz.github.io/jj/)
(Jujutsu) — use jj commands exclusively. Otherwise, use git.

## Conventions

- Edition 2024, resolver 3, nightly rustfmt (pinned in `rust-toolchain.toml`)
- Conventional commits (enforced by `committed` in CI)
- Workspace-level lints in root `Cargo.toml` — do not add crate-level lint attributes
- New crates must set `[lints] workspace = true` and inherit shared `[package]` fields
  with `.workspace = true`

## Claude Code

The `/plugin-dev` skill is available for creating project-specific Claude Code
commands, agents, and skills.
