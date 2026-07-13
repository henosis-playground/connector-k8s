//! Parse-don't-validate boundary from the shared connector SDK contract.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use connector_sdk::ContractError;
use connector_sdk::TargetSlice;
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
    /// A required target-specific field is absent or malformed.
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

impl From<SliceError> for ContractError {
    fn from(error: SliceError) -> Self {
        ContractError::target(error.to_string())
    }
}

impl DesiredSlice {
    /// Decode target-specific context and whole-slice invariants after the SDK
    /// has validated the shared wire contract.
    pub fn decode(slice: &TargetSlice) -> Result<Self, SliceError> {
        let mut components = IdOrdMap::with_capacity(slice.components.len());
        let mut component_names = BTreeSet::new();
        let mut environment = None;
        for component in &slice.components {
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
        for component in &slice.superseded_components {
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
        for output in &slice.upstream_outputs {
            upstream_outputs
                .insert_unique(UpstreamOutput {
                    component_spec_hash: output.component_spec_hash,
                    values_json: output.values_json.clone(),
                })
                .map_err(|_| {
                    SliceError::Invalid(format!(
                        "upstream output spec hash {} is duplicated",
                        hex::encode(output.component_spec_hash)
                    ))
                })?;
        }

        Ok(Self {
            graph_id: slice.graph_id,
            generation: slice.generation,
            sequence: slice.sequence,
            environment,
            components,
            upstream_outputs,
        })
    }

    /// Reject a target identity change after the SDK has retained a level.
    pub fn validate_transition(&self, next: &Self) -> Result<(), SliceError> {
        if self.environment != next.environment {
            return Err(SliceError::Invalid(format!(
                "graph environment changed from {:?} to {:?}",
                self.environment, next.environment
            )));
        }
        Ok(())
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
    component: &connector_sdk::Component,
    environment: &mut Option<String>,
) -> Result<ComponentPin, SliceError> {
    validate_dns_label(&component.name, "component.spec.name").map_err(|source| {
        SliceError::Context {
            component: component.name.clone(),
            source,
        }
    })?;
    let context = ComponentContext::from_bytes(&component.connector_context).map_err(|source| {
        SliceError::Context {
            component: component.name.clone(),
            source,
        }
    })?;
    match environment {
        Some(expected) if expected != &context.environment.id => {
            return Err(SliceError::Invalid(format!(
                "component {:?} uses environment {:?}, but this graph is bound to {expected:?}",
                component.name, context.environment.id
            )));
        }
        None => *environment = Some(context.environment.id.clone()),
        _ => {}
    }
    Ok(ComponentPin {
        spec_hash: component.spec_hash,
        name: component.name.clone(),
        context,
    })
}
