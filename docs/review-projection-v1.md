# Review projection v1

A review projection is the connector-owned, side-effect-free view of one proposed native-state
transition. It has two representations generated from the same rendered diff:

- a human Markdown document with a component summary and the rendered Git patch;
- a machine JSON summary with created, changed, and destroyed artifacts for every component.

In `pr-gated` mode, `connector-k8s` attaches both representations to the pull-request body. The
projection is not committed to the proposal branch, so merging leaves the environment branch as the
exact renderer output tree. The GitHub **Files changed** view remains the untruncated authoritative
patch when the embedded human diff exceeds 32,000 UTF-8 bytes.

## Machine format

The JSON object uses this exact v1 shape:

```json
{
  "apiVersion": "henosis.dev/review-projection/v1",
  "connector": "k8s",
  "environment": "preview_3jhc7x633z88188fzqhcbbrf84",
  "targetBranch": "env/preview_3jhc7x633z88188fzqhcbbrf84",
  "proposalBranch": "henosis/proposals/preview_3jhc7x633z88188fzqhcbbrf84",
  "proposalCommit": "0123456789abcdef0123456789abcdef01234567",
  "components": [
    {
      "component": "service-a",
      "created": [],
      "changed": ["components/service-a/k8s.yaml"],
      "destroyed": []
    },
    {
      "component": "service-b",
      "created": ["components/service-b/k8s.yaml"],
      "changed": [],
      "destroyed": []
    }
  ],
  "environmentChanges": {
    "created": [],
    "changed": ["manifest.json"],
    "destroyed": []
  }
}
```

The reusable connector pattern is:

- `apiVersion`, `connector`, and `environment` identify the projection vocabulary and owner;
- target/proposal coordinates identify the exact review and proposed native-state revision;
- `components` is complete for the desired slice and sorted by native component name;
- each `created`, `changed`, and `destroyed` array contains stable connector-native artifact
  addresses, sorted by the native diff producer;
- `environmentChanges` accounts for artifacts that cannot be assigned to one component.

For Kubernetes v1, an artifact address is a rendered path. Git comparison uses
`--no-renames`, so a rename is deliberately a destroy plus a create. Paths under
`components/<name>/` belong to that component; other paths, including `manifest.json`, are
environment changes. This is complete and deterministic, but file-granular: a changed multi-object
`k8s.yaml` does not reveal which Kubernetes resources changed. That is the first known place to
consider a v2 native-object projection if this experiment survives.

The projection intentionally has no graph generation or contract slice-sequence field. Its exact
proposal commit is the publication-side identity. This keeps the format stable while the shared
contract adds independent slice-sequence and publication identities.

## Publication policies

`HENOSIS_PUBLICATION_POLICIES` is strict JSON connector configuration:

```json
{
  "default": "direct",
  "environments": {
    "preview_3jhc7x633z88188fzqhcbbrf84": "pr-gated"
  }
}
```

The default is `direct`; valid policy values are `direct` and `pr-gated`. Exact environment entries
override the default. Unknown JSON fields or policy values fail service startup.

`direct` retains the existing force-with-lease update of `env/<environment>`. `pr-gated` uses the
stable `henosis/proposals/<environment>` branch and opens or updates one PR against
`env/<environment>`. A newer slice force-with-lease replaces the proposal branch and updates the
same open PR. The connector persists the PR number, URL, head commit, complete renderer outputs, and
stable pending report before reporting `RECONCILING` plus INFO diagnostic `k8s.awaiting-review`.
It polls the GitHub API every 15 seconds and reports `READY` only after observing the merge.

An environment branch must exist before GitHub can use it as a PR base. For a new pr-gated
environment, the connector therefore creates an empty root commit on that branch before opening the
first proposal. This bootstrap is not rendered state, but it is visible to branch observers and is a
significant semantic creak in the experiment.

An empty superseding slice closes an unmerged PR and deletes its proposal branch. If rendered state
was already merged, GitHub cannot represent deletion of the PR's own base branch; the connector
fails that empty slice with `k8s.review.branch-deletion-unsupported` rather than bypassing review.
Terminal `RetireSlice` remains explicit cleanup and removes the environment branch after closing an
unmerged proposal.
