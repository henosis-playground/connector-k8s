//! Parse-don't-validate boundary from the shared protobuf contract.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use henosis_proto::proto::henosis::v1::Component;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
use serde::Serialize;
use thiserror::Error;

use crate::context::ComponentContext;
use crate::context::ContextError;
use crate::context::validate_dns_label;

/// Validated desired slice used by rendering and publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredSlice {
    /// Raw graph UUID bytes.
    pub graph_id: [u8; 16],
    /// Per-graph desired generation.
    pub generation: u64,
    /// Immutable environment identity and deploy branch suffix.
    pub environment: String,
    /// Components sorted by manifest name.
    pub components: BTreeMap<String, ComponentPin>,
}

/// One validated component pin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentPin {
    /// Raw component UUID bytes.
    pub id: [u8; 16],
    /// Connector-owned context.
    pub context: ComponentContext,
}

/// Strict TOML shape consumed by `henosis-render`.
#[derive(Debug, Serialize)]
pub struct RenderManifest<'a> {
    environment: ManifestEnvironment<'a>,
    components: BTreeMap<&'a str, ManifestComponent<'a>>,
}

#[derive(Debug, Serialize)]
struct ManifestEnvironment<'a> {
    id: &'a str,
}

#[derive(Debug, Serialize)]
struct ManifestComponent<'a> {
    repo: &'a str,
    #[serde(rename = "ref")]
    revision: &'a str,
    digest: &'a str,
}

/// Slice contract violation.
#[derive(Debug, Error)]
pub enum SliceError {
    /// A required contract field is absent or malformed.
    #[error("{0}")]
    Invalid(String),
    /// One component has invalid connector context.
    #[error("component {component}: {source}")]
    Context {
        /// Component name or UUID when the name is missing.
        component: String,
        /// Context-specific failure.
        source: ContextError,
    },
}

impl DesiredSlice {
    /// Validate an owned protobuf request, using retained identity for an empty
    /// former-owner slice.
    pub fn from_request(
        request: &ReconcileSliceRequest,
        retained_environment: Option<&str>,
    ) -> Result<Self, SliceError> {
        let slice = request
            .slice
            .as_option()
            .ok_or_else(|| SliceError::Invalid("slice is required".into()))?;
        let graph_id = uuid_bytes(slice.graph_id.as_deref(), "slice.graph_id")?;
        let generation = slice.generation.filter(|value| *value > 0).ok_or_else(|| {
            SliceError::Invalid("slice.generation must be greater than zero".into())
        })?;
        if slice.connector.as_deref() != Some(crate::CONNECTOR_NAME) {
            return Err(SliceError::Invalid(format!(
                "slice.connector must be {:?}",
                crate::CONNECTOR_NAME
            )));
        }

        let mut components = BTreeMap::new();
        let mut component_ids = BTreeSet::new();
        let mut environment = retained_environment.map(str::to_owned);
        for component in &slice.components {
            let (name, pin) = parse_component(component, &mut environment)?;
            if !component_ids.insert(pin.id) {
                return Err(SliceError::Invalid(format!(
                    "component {name:?} duplicates an owned component ID"
                )));
            }
            if components.insert(name.clone(), pin).is_some() {
                return Err(SliceError::Invalid(format!(
                    "component name {name:?} is duplicated"
                )));
            }
        }

        for component in &request.superseded_components {
            let _ = parse_component(component, &mut environment)?;
        }
        let environment = environment.ok_or_else(|| {
            SliceError::Invalid(
                "an empty first slice needs superseded component context to recover its \
                 environment"
                    .into(),
            )
        })?;

        Ok(Self {
            graph_id,
            generation,
            environment,
            components,
        })
    }

    /// Serialize the desired environment as deterministic platform TOML.
    pub fn manifest_toml(&self, environment: &str) -> Result<String, SliceError> {
        let manifest = RenderManifest {
            environment: ManifestEnvironment { id: environment },
            components: self
                .components
                .iter()
                .map(|(name, pin)| {
                    (
                        name.as_str(),
                        ManifestComponent {
                            repo: &pin.context.source.repository,
                            revision: &pin.context.source.revision,
                            digest: &pin.context.image.digest,
                        },
                    )
                })
                .collect(),
        };
        toml::to_string(&manifest).map_err(|error| {
            SliceError::Invalid(format!("could not encode render manifest: {error}"))
        })
    }
}

fn parse_component(
    component: &Component,
    environment: &mut Option<String>,
) -> Result<(String, ComponentPin), SliceError> {
    let id = uuid_bytes(component.id.as_deref(), "component.id")?;
    let fallback = hex::encode(id);
    let name = component
        .name
        .as_deref()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| SliceError::Invalid(format!("component {fallback} has no name")))?;
    validate_dns_label(name, "component.name").map_err(|source| SliceError::Context {
        component: name.into(),
        source,
    })?;
    if component.connector.as_deref() != Some(crate::CONNECTOR_NAME) {
        return Err(SliceError::Invalid(format!(
            "component {name:?} is not owned by connector {:?}",
            crate::CONNECTOR_NAME
        )));
    }
    let context = ComponentContext::from_bytes(component.context.as_deref().unwrap_or_default())
        .map_err(|source| SliceError::Context {
            component: name.into(),
            source,
        })?;
    match environment {
        Some(expected) if expected != &context.environment.id => {
            return Err(SliceError::Invalid(format!(
                "component {name:?} uses environment {:?}, but this graph is bound to {expected:?}",
                context.environment.id
            )));
        }
        None => *environment = Some(context.environment.id.clone()),
        _ => {}
    }
    let revision = component.revision.as_option().ok_or_else(|| {
        SliceError::Invalid(format!(
            "component {name:?} is missing its immutable revision"
        ))
    })?;
    if revision.source.as_deref() != Some(&context.source.repository)
        || revision.revision.as_deref() != Some(&context.source.revision)
    {
        return Err(SliceError::Invalid(format!(
            "component {name:?} revision does not match its connector context source pin"
        )));
    }
    Ok((name.into(), ComponentPin { id, context }))
}

fn uuid_bytes(value: Option<&[u8]>, field: &str) -> Result<[u8; 16], SliceError> {
    value
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SliceError::Invalid(format!("{field} must contain exactly 16 bytes")))
}

#[cfg(test)]
mod tests {
    use buffa::MessageField;
    use henosis_proto::proto::henosis::v1::ComponentRevision;
    use henosis_proto::proto::henosis::v1::GraphSlice;

    use super::*;
    use crate::context::API_VERSION;
    use crate::context::EnvironmentContext;
    use crate::context::ImageContext;
    use crate::context::SourceContext;

    fn request() -> ReconcileSliceRequest {
        let context = ComponentContext {
            api_version: API_VERSION.into(),
            environment: EnvironmentContext {
                id: "preview_3jhc7x633z88188fzqhcbbrf84".into(),
            },
            source: SourceContext {
                repository: "henosis-playground/service-a".into(),
                revision: "a".repeat(40),
            },
            image: ImageContext {
                digest: format!("sha256:{}", "b".repeat(64)),
            },
        };
        let component = Component {
            id: Some(vec![2; 16]),
            name: Some("service-a".into()),
            revision: MessageField::some(ComponentRevision {
                source: Some(context.source.repository.clone()),
                revision: Some(context.source.revision.clone()),
                ..Default::default()
            }),
            connector: Some(crate::CONNECTOR_NAME.into()),
            context: Some(serde_json::to_vec(&context).unwrap()),
            ..Default::default()
        };
        ReconcileSliceRequest {
            slice: MessageField::some(GraphSlice {
                graph_id: Some(vec![1; 16]),
                generation: Some(1),
                connector: Some(crate::CONNECTOR_NAME.into()),
                components: vec![component],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn builds_current_platform_manifest() {
        let desired = DesiredSlice::from_request(&request(), None).unwrap();
        let manifest = desired.manifest_toml(&desired.environment).unwrap();
        assert!(manifest.contains("id = \"preview_3jhc7x633z88188fzqhcbbrf84\""));
        assert!(manifest.contains("[components.service-a]"));
        assert!(manifest.contains("repo = \"henosis-playground/service-a\""));
    }

    #[test]
    fn rejects_context_revision_skew() {
        let mut request = request();
        request.slice.get_or_insert_default().components[0]
            .revision
            .get_or_insert_default()
            .revision = Some("c".repeat(40));
        assert!(DesiredSlice::from_request(&request, None).is_err());
    }
}
