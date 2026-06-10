# {{PROJECT}}

{{DESCRIPTION}}

## Layout

<!-- LINT.IfChange(layout_rules) -->
- `crates/` — library crates and reusable supporting crates
- `tests/` — workspace member crates that build integration and end-to-end test binaries
- `crates/workspace-hack/` — cargo-hakari dependency unification (auto-generated, do not edit)
- Reusable test harnesses, fixtures, and helpers belong in `crates/`, not `tests/`
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`
<!-- LINT.ThenChange(//README.md:layout_rules) -->

## Commands

Use `just` — run `just` to list all recipes. Prefer just recipes over raw cargo commands.

<!-- LINT.IfChange(command_recipes) -->
- `just lint` — run all lints (fmt, clippy, deny, pre-commit). Always run after making changes.
- `just test` — run all tests with optimized third-party dependencies
- `just doc` — build docs
<!-- LINT.ThenChange(//README.md:command_recipes) -->

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
- Use narrow `LINT.IfChange` / `LINT.ThenChange` directives when duplicated
  cross-file content must stay in sync and cannot be eliminated.
