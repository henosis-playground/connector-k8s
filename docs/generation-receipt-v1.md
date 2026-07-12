# Kubernetes generation receipt v1

Every non-empty tree published to `env/<environment.id>` contains a top-level `henosis` object in
`manifest.json`. This makes any resulting Git commit—including a GitHub-generated PR merge
commit—resolve to the Henosis graph generation that produced it.

```json
{
  "environment": "dev",
  "components": {},
  "henosis": {
    "apiVersion": "henosis.dev/generation-receipt/v1",
    "graphId": "1234567890abcdef1234567890abcdef",
    "generation": "42",
    "graphDigest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "componentSpecHashes": [
      "1111111111111111111111111111111111111111111111111111111111111111",
      "2222222222222222222222222222222222222222222222222222222222222222"
    ]
  }
}
```

The fields are strict connector output:

- `apiVersion` identifies this receipt vocabulary.
- `graphId` is the raw 16-byte core graph UUID rendered as 32 lowercase hexadecimal characters.
- `generation` is a decimal string so JavaScript consumers cannot lose `uint64` precision.
- `componentSpecHashes` is the complete Kubernetes slice's registered BLAKE3 spec hashes, sorted as
  raw 32-byte values and rendered as lowercase hexadecimal.
- `graphDigest` is `sha256:` plus SHA-256 over the byte sequence
  `henosis.dev/k8s-graph-generation/v1`, one zero byte, the raw graph UUID, the generation as an
  unsigned 64-bit big-endian integer, then each sorted raw component-spec hash.

The digest is stable across later slice sequences at the same generation, including sequences
created by publishing connector outputs. A new generation changes the receipt and therefore creates
an environment commit even when all rendered Kubernetes YAML is otherwise byte-identical.

This receipt is a discovery and resolution token, not a second graph store. It covers the complete
materialized Kubernetes slice; a graph may also contain specs owned by other connectors. A Warehouse
or promotion adapter MUST use `(graphId, generation, graphDigest)` to resolve and verify the
authoritative graph state through core. It MUST NOT reconstruct the whole graph or infer cross-
connector readiness from Git alone.

The `henosis` name is reserved. Publication fails closed if a renderer already returns a top-level
field with that name.
