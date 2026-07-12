//! Connector-owned review projection for proposed native state.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Stable review-projection schema discriminator.
pub const REVIEW_PROJECTION_API_VERSION: &str = "henosis.dev/review-projection/v1";

/// Machine-readable summary attached to a proposal's human review document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewProjection {
    /// Versioned schema discriminator.
    pub api_version: String,
    /// Connector that produced this projection.
    pub connector: String,
    /// Connector-owned environment identity.
    pub environment: String,
    /// Branch that becomes applied state after merge.
    pub target_branch: String,
    /// Stable level-triggered proposal branch.
    pub proposal_branch: String,
    /// Exact proposed Git commit.
    pub proposal_commit: String,
    /// Complete summaries sorted by component name.
    pub components: Vec<ComponentReviewSummary>,
    /// Rendered artifacts not owned by one component.
    pub environment_changes: ArtifactChanges,
}

/// Native-artifact changes for one connector-owned component.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentReviewSummary {
    /// Component name in the connector's native namespace.
    pub component: String,
    /// Created, changed, and destroyed rendered artifacts.
    #[serde(flatten)]
    pub changes: ArtifactChanges,
}

/// Exhaustive change sets for rendered artifact addresses.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactChanges {
    /// Artifacts absent from applied state and present in the proposal.
    pub created: Vec<String>,
    /// Artifacts present in both states with different bytes.
    pub changed: Vec<String>,
    /// Artifacts present in applied state and absent from the proposal.
    pub destroyed: Vec<String>,
}

/// Malformed Git diff input while constructing a projection.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReviewError {
    /// The diff contained a status this projection does not support.
    #[error("unsupported Git diff status {0:?}")]
    Status(String),
    /// The NUL-delimited status stream was truncated or not UTF-8.
    #[error("malformed Git name-status stream")]
    Malformed,
}

impl ReviewProjection {
    /// Build the complete machine projection from a NUL-delimited
    /// `git diff --name-status --no-renames` stream.
    pub fn from_name_status(
        environment: &str,
        target_branch: String,
        proposal_branch: String,
        proposal_commit: String,
        component_names: &[String],
        name_status: &[u8],
    ) -> Result<Self, ReviewError> {
        let mut components = component_names
            .iter()
            .cloned()
            .map(|name| (name, ArtifactChanges::default()))
            .collect::<BTreeMap<_, _>>();
        let mut environment_changes = ArtifactChanges::default();
        let mut fields = name_status
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty());
        while let Some(status) = fields.next() {
            let path = fields.next().ok_or(ReviewError::Malformed)?;
            let status = std::str::from_utf8(status).map_err(|_| ReviewError::Malformed)?;
            let path = std::str::from_utf8(path).map_err(|_| ReviewError::Malformed)?;
            let changes = component_path(path)
                .map(|component| components.entry(component.into()).or_default())
                .unwrap_or(&mut environment_changes);
            match status {
                "A" => changes.created.push(path.into()),
                "M" => changes.changed.push(path.into()),
                "D" => changes.destroyed.push(path.into()),
                other => return Err(ReviewError::Status(other.into())),
            }
        }
        Ok(Self {
            api_version: REVIEW_PROJECTION_API_VERSION.into(),
            connector: crate::CONNECTOR_NAME.into(),
            environment: environment.into(),
            target_branch,
            proposal_branch,
            proposal_commit,
            components: components
                .into_iter()
                .map(|(component, changes)| ComponentReviewSummary { component, changes })
                .collect(),
            environment_changes,
        })
    }

    /// Render the human review document and embed this machine projection.
    pub fn document(&self, patch: &str) -> Result<String, serde_json::Error> {
        let mut document = format!(
            "# Henosis Kubernetes review\n\nMerge applies the proposed complete rendered tree to \
             `{}`. The proposal branch is level-triggered: a newer desired slice replaces this \
             document and commit.\n\n## Component summary\n\n| Component | Created | Changed | \
             Destroyed |\n|---|---:|---:|---:|\n",
            self.target_branch
        );
        if self.components.is_empty() {
            document.push_str("| _none_ | 0 | 0 | 0 |\n");
        }
        for component in &self.components {
            document.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                component.component,
                component.changes.created.len(),
                component.changes.changed.len(),
                component.changes.destroyed.len()
            ));
        }
        document.push_str(&format!(
            "| _environment_ | {} | {} | {} |\n\n",
            self.environment_changes.created.len(),
            self.environment_changes.changed.len(),
            self.environment_changes.destroyed.len()
        ));
        document.push_str("## Rendered tree diff\n\n```diff\n");
        let truncated = patch.len() > 32_000;
        let patch = truncate_utf8(patch, 32_000).replace("```", "` ` `");
        document.push_str(&patch);
        if truncated {
            document.push_str(
                "\n# Diff truncated; use GitHub's Files changed view for the complete patch.\n",
            );
        }
        document.push_str("\n```\n\n## Machine summary\n\n```json\n");
        document.push_str(&serde_json::to_string_pretty(self)?);
        document.push_str("\n```\n\n<!-- henosis-review-projection:v1 -->\n");
        Ok(document)
    }
}

fn component_path(path: &str) -> Option<&str> {
    let path = path.strip_prefix("components/")?;
    let (component, _) = path.split_once('/')?;
    (!component.is_empty()).then_some(component)
}

fn truncate_utf8(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_sorted_component_and_environment_changes() {
        let projection = ReviewProjection::from_name_status(
            "preview_3jhc7x633z88188fzqhcbbrf84",
            "env/preview_3jhc7x633z88188fzqhcbbrf84".into(),
            "henosis/proposals/preview_3jhc7x633z88188fzqhcbbrf84".into(),
            "a".repeat(40),
            &["service-c".into()],
            b"M\0components/service-b/k8s.yaml\0A\0manifest.json\0D\0components/service-a/k8s.yaml\0",
        )
        .unwrap();
        assert_eq!(projection.components[0].component, "service-a");
        assert_eq!(
            projection.components[0].changes.destroyed,
            ["components/service-a/k8s.yaml"]
        );
        assert_eq!(projection.components[1].changes.changed.len(), 1);
        assert_eq!(projection.components[2].component, "service-c");
        assert_eq!(projection.environment_changes.created, ["manifest.json"]);
        assert!(
            projection
                .document("diff --git a/a b/a\n")
                .unwrap()
                .contains("\"apiVersion\": \"henosis.dev/review-projection/v1\"")
        );
    }
}
