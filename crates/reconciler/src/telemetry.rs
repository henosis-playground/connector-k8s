//! Registry for connector-specific tracing attributes.

/// Raw graph UUID rendered as lowercase hexadecimal.
pub const GRAPH_ID: &str = "soter.henosis.graph.id";
/// Desired graph generation rendered as a string ID.
pub const GRAPH_GENERATION: &str = "soter.henosis.graph.generation";
/// Connector-owned environment identity.
pub const ENVIRONMENT_ID: &str = "soter.henosis.environment.id";
/// Number of components in the complete owned slice.
pub const COMPONENT_COUNT: &str = "soter.henosis.slice.component_count";
/// Bounded reconcile outcome.
pub const RECONCILE_OUTCOME: &str = "soter.henosis.reconcile.outcome";
/// Commit published to the desired-state branch.
pub const PUBLISHED_COMMIT: &str = "soter.henosis.publication.commit";
