//! Registry for connector-specific tracing attributes.

/// Raw graph UUID rendered as lowercase hexadecimal.
pub const GRAPH_ID: &str = "soter.henosis.graph.id";
/// Desired graph generation rendered as a string ID.
pub const GRAPH_GENERATION: &str = "soter.henosis.graph.generation";
/// Durable graph-slice sequence rendered as a string ID.
pub const SLICE_SEQUENCE: &str = "soter.henosis.slice.sequence";
/// Connector-owned environment identity.
pub const ENVIRONMENT_ID: &str = "soter.henosis.environment.id";
/// Number of components in the complete owned slice.
pub const COMPONENT_COUNT: &str = "soter.henosis.slice.component_count";
/// Bounded reconcile outcome.
pub const RECONCILE_OUTCOME: &str = "soter.henosis.reconcile.outcome";
/// Commit published to the desired-state branch.
pub const PUBLISHED_COMMIT: &str = "soter.henosis.publication.commit";
/// Bounded environment publication policy.
pub const PUBLICATION_POLICY: &str = "soter.henosis.publication.policy";
/// GitHub pull-request number rendered as a string ID.
pub const PROPOSAL_NUMBER: &str = "soter.henosis.proposal.number";
/// Bounded deterministic-render cache outcome (`hit`, `miss`, or
/// `uncacheable`).
pub const RENDER_CACHE_STATUS: &str = "soter.henosis.render.cache_status";
/// Bounded explanation when a render could not use the cache.
pub const RENDER_CACHE_REASON: &str = "soter.henosis.render.cache_reason";
/// Complete semantic render-recipe BLAKE3 digest.
pub const RENDER_RECIPE: &str = "soter.henosis.render.recipe";
/// Immutable platform commit used by the prepared renderer.
pub const RENDER_PLATFORM_SHA: &str = "soter.henosis.render.platform_sha";
/// Number of oldest cache entries evicted by this render.
pub const RENDER_CACHE_EVICTED_COUNT: &str = "soter.henosis.render.cache_evicted_count";
