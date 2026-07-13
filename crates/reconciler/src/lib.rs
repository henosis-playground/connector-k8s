//! Slice-holistic Kubernetes reconciliation, renderer execution, and
//! desired-state publication.

pub mod context;
pub mod engine;
pub mod reconciler;
mod render_cache;
pub mod review;
pub mod slice;
pub mod telemetry;

pub use reconciler::ConnectorConfig;
pub use reconciler::KubernetesConnector;

/// Registry key served by this connector.
pub const CONNECTOR_NAME: &str = "k8s";
