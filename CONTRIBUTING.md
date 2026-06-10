# Contributing to {{PROJECT}}

## Getting Started

### Rust

Install [rustup](https://rustup.rs/), then clone the repo. The pinned nightly toolchain
will be installed automatically on first `cargo` invocation via `rust-toolchain.toml`.

### Development Tools

| Tool | Used by |
|---|---|
| [just](https://github.com/casey/just) | Command runner (`justfile`) |
| [cargo-deny](https://github.com/EmbarkStudios/cargo-deny) | `just check-deny` — license, ban, and advisory checks |
| [cargo-hakari](https://github.com/guppy-rs/guppy/tree/main/tools/cargo-hakari) | `just hakari` — workspace-hack dependency unification |
| [git-cliff](https://github.com/orhun/git-cliff) | `just changelog` — changelog generation |
| [cargo-nextest](https://github.com/nextest-rs/nextest) | `just test` — test runner |
| [prek](https://github.com/j178/prek) | `just check-pre-commit` — pre-commit hooks |
| [Taplo](https://github.com/tamasfe/taplo) | `just fmt` / `just check-fmt` — TOML formatting |
| [yamlfmt](https://github.com/google/yamlfmt) | `just fmt` / `just check-fmt` — YAML formatting |

Feel free to install these through your preferred package manager. If you don't want to
bother, all of these tools are also written in Rust. The downside is they won't
auto-update; rerun the command to update them.

```sh
cargo install --locked just cargo-deny cargo-hakari cargo-nextest git-cliff prek taplo-cli
go install github.com/google/yamlfmt/cmd/yamlfmt@latest
```

## Workspace Layout

- `crates/` contains library crates and reusable supporting crates.
- `tests/` contains workspace member crates that produce integration or end-to-end test binaries.
- Reusable test harnesses, fixtures, and helpers belong in `crates/`, not `tests/`.
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`.

## Feature Requests

Need some new functionality to help? You can let us know by opening an
[issue][new issue]. It's helpful to look through [all issues][all issues] in
case it's already being talked about.

## Bug Reports

Please let us know about what problems you run into, whether in behavior or
ergonomics of API. You can do this by opening an [issue][new issue]. It's
helpful to look through [all issues][all issues] in case it's already being
talked about.

## Pull Requests

Looking for an idea? Check our [issues][issues]. If the issue looks open ended,
it is probably best to post on the issue how you are thinking of resolving the
issue so you can get feedback early in the process.

Already have an idea? It might be good to first [create an issue][new issue]
to propose it so we can make sure we are aligned and lower the risk of needing
to re-work some of it.

### Process

As a heads up, CI runs the following checks:
- warnings turned to compile errors
- `cargo nextest run`
- `rustfmt`
- `taplo fmt --check`
- `yamlfmt -lint`
- `clippy`
- `rustdoc`
- `cargo deny`
- `prek`
- [`committed`](https://github.com/crate-ci/committed) for [Conventional Commits](https://www.conventionalcommits.org)
- [`typos`](https://github.com/crate-ci/typos) for spelling
- `zizmor`
- MSRV checks
- minimal-version checks
- coverage generation

Run `just lint` locally to catch most of these before pushing.

We request that the commit history gets cleaned up.

Commits should be atomic, meaning they are complete and have a single responsibility.
A complete commit should build, pass tests, update documentation and tests, and not have dead code.

PRs should tell a cohesive story, with refactor and test commits that keep the
fix or feature commits simple and clear.

Specifically, we would encourage:
- File renames be isolated into their own commit
- Add tests in a commit before their feature or fix, showing the current behavior

[issues]: {{REPOSITORY}}/issues
[new issue]: {{REPOSITORY}}/issues/new
[all issues]: {{REPOSITORY}}/issues?utf8=%E2%9C%93&q=is%3Aissue
