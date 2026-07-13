# Kubernetes render cache v1

The connector memoizes only the deterministic renderer result. Runner resolution happens before
the lookup so a mutable configured platform ref cannot reuse output from an older platform commit.
On a hit the connector restores and validates the rendered tree, then performs generation-receipt
embedding, Git publication or proposal handling, and core reporting normally. Failures, receipts,
publication results, proposal state, and reports are never cached.

The `henosis.dev/k8s-render-action/v1` BLAKE3 recipe covers every input available to the connector
that can affect rendering:

- the resolved immutable platform Git SHA;
- BLAKE3 digests of the exact `prepare-runner` recipe and prepared runner entrypoint, which bind the
  pinned Node/pnpm provisioning contract and execution wrapper;
- the canonical desired-environment and `dev` TOML manifests, including environment identity,
  component names, source repositories and SHAs, and image digests;
- every complete registered component-spec hash;
- every resolved upstream component-spec hash and its canonical JSON output value; and
- the fixed render subcommand and a digest of the complete effective inherited process environment,
  with `GITHUB_ACTIONS=true` applied. Raw environment values are not stored in cache metadata or
  telemetry.

Graph ID, generation, and slice sequence are occurrence identity, not reusable computation input,
so they are deliberately excluded. Generation-specific receipt embedding remains outside the cache
boundary. Any change to the recipe vocabulary requires a new version prefix and cache directory.

Only a renderer tree that passes the manifest environment, complete component-set, output, and
reserved-receipt-slot checks is installed. Every entry retains a deterministic BLAKE3 digest over
its sorted paths, entry kinds, Unix modes, and file contents. Entries are copied into a staging
directory and renamed atomically, so readers never observe a partial entry. The digest and semantic
checks are verified on every hit; a corrupt entry is discarded and rendered again. Cache I/O
failure does not block a fresh render.

The cache lives under `HENOSIS_STATE_DIR/scratch/render-cache-v1` and is bounded by
`HENOSIS_RENDER_CACHE_MAX_ENTRIES` (default `64`). After each insertion, entries beyond the bound
are evicted oldest-insertion-first using directory modification time with path order as a stable
tie-breaker. Reads do not refresh age, making this a FIFO policy rather than LRU. Setting the bound
to `0` disables memoization. The reconcile wide event records cache status, bounded reason, recipe
digest, resolved platform SHA, and eviction count.
