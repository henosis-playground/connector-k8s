# Contributing to PROJECT

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

Feel free to install these through your preferred package manager. If you don't want to
bother, all of these tools are also written in Rust — here's one command to install them
all through cargo. The downside is they won't auto-update; rerun the command to update.

```sh
cargo install --locked just cargo-deny cargo-hakari cargo-nextest git-cliff prek
```

## Pull Requests

We run the following checks on all PRs:
- `cargo nextest run`
- `cargo fmt` (nightly)
- `cargo clippy` — warnings are errors
- `cargo doc` — warnings are errors
- `cargo deny` — license, ban, and advisory checks
- [`committed`](https://github.com/crate-ci/committed) — [Conventional Commits](https://www.conventionalcommits.org) style
- [`typos`](https://github.com/crate-ci/typos) — spell checking

Run `just lint` locally to catch most of these before pushing.

We request that the commit history is clean:
- Commits should be atomic — complete, single responsibility, buildable, and passing tests.
- File renames should be isolated into their own commit.
- PRs should tell a cohesive story, with refactor and test commits that keep
  feature or fix commits simple and clear.

## Releasing

Pre-requisites:
- Running `cargo login`
- Push permission to the repo
- [`cargo-release`](https://github.com/crate-ci/cargo-release/)

When ready to release:
1. Update the changelog (`just changelog`)
2. Determine the next version according to semver
3. Run `cargo release -x <level>`

[issues]: https://github.com/skuld-systems/PROJECT/issues
[new issue]: https://github.com/skuld-systems/PROJECT/issues/new
