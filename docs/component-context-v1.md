# Kubernetes component context v1

`henosis.v1.Component.context` is opaque to core. For components assigned to connector `k8s`, its
bytes MUST be UTF-8 JSON matching this document. Producers MUST emit `apiVersion` exactly as shown;
the connector rejects unknown versions, unknown fields, missing fields, and malformed pins.

```json
{
  "apiVersion": "henosis.dev/k8s-component-context/v1",
  "environment": {
    "id": "preview_3jhc7x633z88188fzqhcbbrf84"
  },
  "source": {
    "repository": "henosis-playground/service-a",
    "revision": "ca73c9ae5b6579ad0b6b77b80fb77b54fc5fd595"
  },
  "image": {
    "digest": "sha256:b808fd4ef39b8f18309b6e266f7ab84d466ee8713c20f832248ae35cc5b64586"
  }
}
```

The vocabulary is deliberately the smallest complete platform manifest pin:

- `apiVersion` is the literal `henosis.dev/k8s-component-context/v1`.
- `environment.id` is either a lowercase DNS label of at most 63 characters or a canonical TypeID
  with prefix `preview`. The stable token `preview`, legacy `preview-...` UUIDs, custom preview
  slugs, uppercase suffixes, and non-canonical TypeID suffixes are rejected.
- `source.repository` is a GitHub `owner/name`, without a URL or `.git` suffix.
- `source.revision` is a full, immutable, 40-character lowercase Git commit SHA. It MUST equal the
  enclosing `Component.revision.revision`; `source.repository` MUST equal
  `Component.revision.source`.
- `image.digest` is an immutable lowercase `sha256:` OCI digest with exactly 64 hex characters.

Every component in a complete slice MUST repeat the same `environment.id`. That identity is immutable
for a graph after the connector accepts its first generation. `Component.name` becomes the platform
manifest component key and MUST be a lowercase DNS label; names and component IDs MUST both be unique.

The connector translates the complete owned slice into the existing renderer input without adding
defaults:

```toml
[environment]
id = "preview_3jhc7x633z88188fzqhcbbrf84"

[components.service-a]
repo = "henosis-playground/service-a"
ref = "ca73c9ae5b6579ad0b6b77b80fb77b54fc5fd595"
digest = "sha256:b808fd4ef39b8f18309b6e266f7ab84d466ee8713c20f832248ae35cc5b64586"
```

No pnpm package name is carried: the current renderer discovers the component package and its
Henosis platform descriptor from the pinned repository, exactly as the deploy workflow does today.
No `follow` or `borrowForPreview` flag is carried either. `follow` is workflow-owned pin resolution;
the pushed slice contains the resulting immutable source pins. `borrowForPreview` is author-owned
metadata discovered inside the pinned component package by the renderer.

For an empty former-owner slice, core supplies `superseded_components`; their v1 contexts recover the
environment branch that must be removed. Retirement supplies the last complete slice, while the
connector's durable accepted level provides the same identity after a process restart.

## Publication and outputs

The applied environment exists only at `env/<environment.id>` in the configured deploy repository.
Rendering completes in a temporary directory and its `manifest.json` component set is compared with
the complete desired slice before publication. The connector's per-environment policy then either
pushes that complete tree directly with force-with-lease or proposes it through the PR-gated flow in
[review-projection-v1.md](review-projection-v1.md). A failed render or validation never changes the
applied tree.

Each renderer `manifest.json` component `outputs` object is serialized as deterministic JSON and
reported as that component's complete `ComponentOutputs.values_json`. The connector reports every
owned disposition, every output, and every diagnostic in one `ReportSlice` request. Structured
renderer validation issues retain their `code`, `message`, component, RFC 6901 record pointer, and
`help` verbatim; non-validator renderer failures retain the renderer message and full excerpt.
