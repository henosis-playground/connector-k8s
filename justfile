export RUST_BACKTRACE := env_var_or_default("RUST_BACKTRACE", "short")

default:
  just --list

clean:
    cargo clean

fmt *flags:
    cargo fmt --all {{ flags }}

check-fmt:
    cargo fmt --all -- --check

clippy *flags:
    cargo clippy --all-targets {{ flags }} -- -D warnings

check-deny:
    cargo deny --all-features check

check-pre-commit:
    prek run --all-files

# Runs all lints (fmt, clippy, deny, pre-commit hooks)
lint: check-fmt clippy check-deny check-pre-commit

test *flags:
    cargo nextest run {{ flags }}

doc *flags:
    RUSTDOCFLAGS="--cfg docsrs" cargo doc --all-features --no-deps --document-private-items --keep-going {{ flags }}

[private]
_assert-clean:
    {{ if `test -z "$(git status --porcelain --untracked-files=no)" && echo clean || echo dirty` == "dirty" {
        error("working tree is dirty — commit or stash changes first")
    } else {
        ""
    } }}

# Auto-fix formatting and lint warnings (requires clean working tree)
fix: _assert-clean
    cargo fmt --all
    cargo fix --workspace --allow-dirty --allow-staged
    cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged

changelog:
    git cliff -o CHANGELOG.md

hakari:
    cargo hakari manage-deps
    cargo hakari generate
