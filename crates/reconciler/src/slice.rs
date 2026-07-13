//! Parse-don't-validate boundary from the shared protobuf contract.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use buffa::Message as _;
use buffa::MessageView as _;
use henosis_proto::proto::henosis::v1::GraphSlice;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequestView;
use henosis_proto::proto::henosis::v1::RegisteredComponentSpecView;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

use crate::context::ComponentContext;
use crate::context::ContextError;
use crate::context::validate_dns_label;

/// Validated desired slice used by rendering and publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredSlice {
    /// Raw graph UUID bytes.
    pub graph_id: [u8; 16],
    /// Per-graph desired generation provenance.
    pub generation: u64,
    /// Durable materialization identity.
    pub sequence: u64,
    /// Immutable environment identity and deploy branch suffix.
    pub environment: String,
    /// Registered component specs keyed and sorted by their content hash.
    pub components: IdOrdMap<ComponentPin>,
    /// Current-generation upstream publications keyed by their component spec
    /// hash.
    pub upstream_outputs: IdOrdMap<UpstreamOutput>,
}

/// One validated registered component pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentPin {
    /// Registered component-spec content hash.
    pub spec_hash: [u8; 32],
    /// Platform manifest component key.
    pub name: String,
    /// Connector-owned context.
    pub context: ComponentContext,
}

impl IdOrdItem for ComponentPin {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.spec_hash
    }
}

/// One validated current-generation upstream output level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamOutput {
    /// Component-spec identity that produced the output.
    pub component_spec_hash: [u8; 32],
    /// Canonical JSON output object.
    pub values_json: Vec<u8>,
}

impl IdOrdItem for UpstreamOutput {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
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
        /// Component name or spec hash when the name is missing.
        component: String,
        /// Context-specific failure.
        source: ContextError,
    },
}

impl DesiredSlice {
    /// Validate a borrowed request view, copying only accepted domain state.
    pub fn from_request(
        request: &ReconcileSliceRequestView<'_>,
        retained_environment: Option<&str>,
    ) -> Result<Self, SliceError> {
        let slice = request
            .slice
            .as_option()
            .ok_or_else(|| SliceError::Invalid("slice is required".into()))?;
        Self::from_parts(slice, &request.superseded_components, retained_environment)
    }

    /// Validate an exact slice recovered from core. Superseded specs are not
    /// needed because the durable environment binding is already retained.
    pub fn from_recovered(
        slice: &GraphSlice,
        retained_environment: &str,
    ) -> Result<Self, SliceError> {
        let bytes = slice.encode_to_vec();
        let view = henosis_proto::proto::henosis::v1::GraphSliceView::decode_view(&bytes)
            .map_err(|error| SliceError::Invalid(error.to_string()))?;
        Self::from_parts(&view, &[], Some(retained_environment))
    }

    fn from_parts<'a>(
        slice: &henosis_proto::proto::henosis::v1::GraphSliceView<'a>,
        superseded: &[RegisteredComponentSpecView<'a>],
        retained_environment: Option<&str>,
    ) -> Result<Self, SliceError> {
        let graph_id = uuid_bytes(slice.graph_id, "slice.graph_id")?;
        let generation = slice.generation.filter(|value| *value > 0).ok_or_else(|| {
            SliceError::Invalid("slice.generation must be greater than zero".into())
        })?;
        let sequence = slice
            .sequence
            .ok_or_else(|| SliceError::Invalid("slice.sequence is required".into()))?;
        if slice.connector != Some(crate::CONNECTOR_NAME) {
            return Err(SliceError::Invalid(format!(
                "slice.connector must be {:?}",
                crate::CONNECTOR_NAME
            )));
        }

        let mut components = IdOrdMap::with_capacity(slice.components.len());
        let mut component_names = BTreeSet::new();
        let mut environment = retained_environment.map(str::to_owned);
        for component in slice.components.iter() {
            let pin = parse_component(component, &mut environment)?;
            if !component_names.insert(pin.name.clone()) {
                return Err(SliceError::Invalid(format!(
                    "component name {:?} is duplicated",
                    pin.name
                )));
            }
            let hash = pin.spec_hash;
            components.insert_unique(pin).map_err(|_| {
                SliceError::Invalid(format!(
                    "component spec hash {} is duplicated",
                    hex::encode(hash)
                ))
            })?;
        }

        for component in superseded {
            let _ = parse_component(component, &mut environment)?;
        }
        let environment = environment.ok_or_else(|| {
            SliceError::Invalid(
                "an empty first slice needs superseded component context to recover its \
                 environment"
                    .into(),
            )
        })?;

        let mut upstream_outputs = IdOrdMap::with_capacity(slice.upstream_outputs.len());
        for output in slice.upstream_outputs.iter() {
            let component_spec_hash = hash_bytes(
                output.component_spec_hash,
                "slice.upstream_outputs.component_spec_hash",
            )?;
            let value: serde_json::Value = serde_json::from_slice(
                output.values_json.unwrap_or_default(),
            )
            .map_err(|error| {
                SliceError::Invalid(format!(
                    "upstream output {} is not JSON: {error}",
                    hex::encode(component_spec_hash)
                ))
            })?;
            let values_json = serde_json::to_vec(&value)
                .map_err(|error| SliceError::Invalid(error.to_string()))?;
            upstream_outputs
                .insert_unique(UpstreamOutput {
                    component_spec_hash,
                    values_json,
                })
                .map_err(|_| {
                    SliceError::Invalid(format!(
                        "upstream output spec hash {} is duplicated",
                        hex::encode(component_spec_hash)
                    ))
                })?;
        }

        Ok(Self {
            graph_id,
            generation,
            sequence,
            environment,
            components,
            upstream_outputs,
        })
    }

    /// Stable environment borrowed by every component in this slice.
    pub fn borrowed_environment(&self) -> Option<&str> {
        let mut borrowed = self.components.iter().map(|component| {
            component
                .context
                .borrow
                .as_ref()
                .map(|borrow| borrow.effective_environment.id.as_str())
        });
        let first = borrowed.next()??;
        borrowed
            .all(|environment| environment == Some(first))
            .then_some(first)
    }

    /// Serialize the desired environment as deterministic platform TOML.
    pub fn manifest_toml(&self, environment: &str) -> Result<String, SliceError> {
        let manifest = RenderManifest {
            environment: ManifestEnvironment { id: environment },
            components: self
                .components
                .iter()
                .map(|pin| {
                    (
                        pin.name.as_str(),
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

    /// Find a component by its platform manifest name.
    pub fn component_named(&self, name: &str) -> Option<&ComponentPin> {
        self.components
            .iter()
            .find(|component| component.name == name)
    }

    /// Sorted platform manifest component names.
    pub fn component_names(&self) -> Vec<String> {
        let mut names = self
            .components
            .iter()
            .map(|component| component.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Stable digest of every input that can affect this connector's output,
    /// excluding only the durable sequence cursor.
    pub fn materialization_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"henosis.dev/k8s-materialization/v1\0");
        hasher.update(&self.graph_id);
        hasher.update(&self.generation.to_be_bytes());
        for component in self.components.iter() {
            hasher.update(&component.spec_hash);
        }
        for output in self.upstream_outputs.iter() {
            hasher.update(&output.component_spec_hash);
            hasher.update(&(output.values_json.len() as u64).to_be_bytes());
            hasher.update(&output.values_json);
        }
        *hasher.finalize().as_bytes()
    }

    /// Versioned SHA-256 graph digest embedded in environment publications.
    pub fn graph_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"henosis.dev/k8s-graph-generation/v1\0");
        hasher.update(self.graph_id);
        hasher.update(self.generation.to_be_bytes());
        for component in self.components.iter() {
            hasher.update(component.spec_hash);
        }
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }
}

fn parse_component(
    component: &RegisteredComponentSpecView<'_>,
    environment: &mut Option<String>,
) -> Result<ComponentPin, SliceError> {
    let spec_hash = hash_bytes(component.hash, "component.hash")?;
    let fallback = hex::encode(spec_hash);
    let spec = component
        .spec
        .as_option()
        .ok_or_else(|| SliceError::Invalid(format!("component {fallback} has no spec body")))?;
    let encoded = spec
        .to_owned_message()
        .map_err(|error| SliceError::Invalid(error.to_string()))?
        .encode_to_vec();
    if blake3::hash(&encoded).as_bytes() != &spec_hash {
        return Err(SliceError::Invalid(format!(
            "component {fallback} hash does not match its canonical spec content"
        )));
    }
    let name = spec
        .name
        .filter(|name| !name.is_empty())
        .ok_or_else(|| SliceError::Invalid(format!("component {fallback} has no name")))?;
    validate_dns_label(name, "component.spec.name").map_err(|source| SliceError::Context {
        component: name.into(),
        source,
    })?;
    if spec.connector != Some(crate::CONNECTOR_NAME) {
        return Err(SliceError::Invalid(format!(
            "component {name:?} is not owned by connector {:?}",
            crate::CONNECTOR_NAME
        )));
    }
    let mut dependencies = BTreeSet::new();
    for dependency in spec.depends_on.iter() {
        let dependency = hash_bytes(Some(dependency), "component.spec.depends_on")?;
        if !dependencies.insert(dependency) {
            return Err(SliceError::Invalid(format!(
                "component {name:?} repeats a dependency spec hash"
            )));
        }
    }
    let context = ComponentContext::from_bytes(spec.connector_context.unwrap_or_default())
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
    Ok(ComponentPin {
        spec_hash,
        name: name.into(),
        context,
    })
}

fn uuid_bytes(value: Option<&[u8]>, field: &str) -> Result<[u8; 16], SliceError> {
    value
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SliceError::Invalid(format!("{field} must contain exactly 16 bytes")))
}

fn hash_bytes(value: Option<&[u8]>, field: &str) -> Result<[u8; 32], SliceError> {
    value
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SliceError::Invalid(format!("{field} must contain exactly 32 bytes")))
}

#[cfg(test)]
mod tests {
    use buffa::MessageField;
    use henosis_proto::proto::henosis::v1::ComponentSpec;
    use henosis_proto::proto::henosis::v1::GraphSlice;
    use henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
    use henosis_proto::proto::henosis::v1::RegisteredComponentSpec;

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
            borrow: None,
        };
        let spec = ComponentSpec {
            name: Some("service-a".into()),
            connector: Some(crate::CONNECTOR_NAME.into()),
            connector_context: Some(serde_json::to_vec(&context).unwrap()),
            ..Default::default()
        };
        let hash = blake3::hash(&spec.encode_to_vec()).as_bytes().to_vec();
        let component = RegisteredComponentSpec {
            hash: Some(hash),
            spec: MessageField::some(spec),
            ..Default::default()
        };
        ReconcileSliceRequest {
            slice: MessageField::some(GraphSlice {
                graph_id: Some(vec![1; 16]),
                generation: Some(1),
                connector: Some(crate::CONNECTOR_NAME.into()),
                components: vec![component],
                sequence: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn parse(request: &ReconcileSliceRequest) -> Result<DesiredSlice, SliceError> {
        let bytes = request.encode_to_vec();
        let view = ReconcileSliceRequestView::decode_view(&bytes).unwrap();
        DesiredSlice::from_request(&view, None)
    }

    #[test]
    fn builds_current_platform_manifest() {
        let desired = parse(&request()).unwrap();
        let manifest = desired.manifest_toml(&desired.environment).unwrap();
        assert!(manifest.contains("id = \"preview_3jhc7x633z88188fzqhcbbrf84\""));
        assert!(manifest.contains("[components.service-a]"));
        assert!(manifest.contains("repo = \"henosis-playground/service-a\""));
        assert_eq!(desired.sequence, 0);
    }

    #[test]
    fn rejects_registered_spec_hash_skew() {
        let mut request = request();
        request.slice.get_or_insert_default().components[0].hash = Some(vec![0; 32]);
        assert!(parse(&request).is_err());
    }

    #[test]
    fn graph_digest_is_sequence_independent() {
        let mut desired = parse(&request()).unwrap();
        let first = desired.graph_digest();
        desired.sequence = 9;
        assert_eq!(desired.graph_digest(), first);
    }
}
