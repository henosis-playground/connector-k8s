# PROJECT

DESCRIPTION

## Layout

- `crates/` contains library crates and reusable supporting crates.
- `tests/` contains workspace member crates that build integration and end-to-end test binaries.
- Reusable test harnesses, fixtures, and helpers belong in `crates/`.
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`.

## Commands

Use `just` to discover and run common tasks:

- `just lint`
- `just test`
- `just doc`
