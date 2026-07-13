//! Kubernetes target lifecycle implemented against the shared connector SDK.

use std::fs;
use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use connector_sdk::ApplyOutcome;
use connector_sdk::Approved;
use connector_sdk::ConcurrencyScope;
use connector_sdk::Connector;
use connector_sdk::ContractError;
use connector_sdk::Diagnostic;
use connector_sdk::Output;
use connector_sdk::PassContext;
use connector_sdk::PlanOutcome;
use connector_sdk::PlanProposal;
use connector_sdk::RetireContext;
use connector_sdk::RetireOutcome;
use connector_sdk::Retry;
use connector_sdk::ReviewProjection;
use connector_sdk::TargetSlice;
use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::engine::Engine;
use crate::engine::EngineError;
use crate::engine::Proposal;
use crate::engine::ProposalPublication;
use crate::engine::ProposalStatus;
use crate::engine::PublicationPolicy;
use crate::slice::DesiredSlice;

const PUBLICATION_NAMESPACE: Uuid = Uuid::from_bytes([
    0xa7, 0x8d, 0x2e, 0xe3, 0x18, 0x8d, 0x5b, 0x4b, 0xaf, 0x21, 0x60, 0xc4, 0xd0, 0xf7, 0xd4, 0x3c,
]);

/// Connector-owned target state root. SDK checkpoints live separately.
#[derive(Clone, Debug)]
pub struct ConnectorConfig {
    /// Durable target observation state.
    pub target_state_dir: PathBuf,
}

/// Fresh target truth used for one plan.
pub struct Observation {
    state: TargetState,
    proposal_status: Option<ProposalStatus>,
}

/// One immutable Kubernetes effect. Rendered trees are deliberately not
/// persisted: apply deterministically re-renders and verifies this full-tree
/// digest before any Git mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecutablePlan {
    Publish {
        tree_digest: String,
        outputs: Vec<Output>,
    },
    Propose {
        tree_digest: String,
        outputs: Vec<Output>,
    },
    RecordMerged {
        commit: String,
    },
    CancelProposal,
    RemoveEnvironment,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct TargetState {
    #[serde(default)]
    input_digest: Option<[u8; 32]>,
    #[serde(default)]
    outputs: Vec<Output>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    published: bool,
    #[serde(default)]
    proposal: Option<PendingProposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PendingProposal {
    input_digest: [u8; 32],
    tree_digest: String,
    outputs: Vec<Output>,
    proposal: Proposal,
}

/// Kubernetes-specific decode/observe/plan/apply/retire implementation.
pub struct KubernetesConnector {
    config: ConnectorConfig,
    engine: Engine,
}

impl KubernetesConnector {
    /// Construct target lifecycle hooks around the native renderer/Git adapter.
    pub fn new(config: ConnectorConfig, engine: Engine) -> Result<Self, std::io::Error> {
        fs::create_dir_all(&config.target_state_dir)?;
        Ok(Self { config, engine })
    }

    fn state_path(&self, environment: &str) -> PathBuf {
        self.config
            .target_state_dir
            .join(format!("{environment}.json"))
    }

    fn load(&self, environment: &str) -> Result<TargetState, EngineError> {
        match fs::read(self.state_path(environment)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                target_state_error("decode Kubernetes target state", error.to_string())
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(TargetState::default())
            }
            Err(error) => Err(target_state_error(
                "read Kubernetes target state",
                error.to_string(),
            )),
        }
    }

    fn save(&self, environment: &str, state: &TargetState) -> Result<(), EngineError> {
        let path = self.state_path(environment);
        let bytes = serde_json::to_vec(state).map_err(|error| {
            target_state_error("encode Kubernetes target state", error.to_string())
        })?;
        let mut temporary =
            NamedTempFile::new_in(&self.config.target_state_dir).map_err(|error| {
                target_state_error(
                    "create Kubernetes target-state transaction",
                    error.to_string(),
                )
            })?;
        temporary
            .write_all(&bytes)
            .and_then(|()| temporary.as_file_mut().sync_all())
            .map_err(|error| {
                target_state_error("commit Kubernetes target state", error.to_string())
            })?;
        temporary.persist(path).map_err(|error| {
            target_state_error("publish Kubernetes target state", error.error.to_string())
        })?;
        fs::File::open(&self.config.target_state_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                target_state_error("sync Kubernetes target-state directory", error.to_string())
            })
    }

    async fn render_verified(
        &self,
        desired: &DesiredSlice,
        tree_digest: &str,
        outputs: &[Output],
    ) -> Result<crate::engine::RenderedWorld, ApplyOutcome> {
        let world = self
            .engine
            .render(desired)
            .await
            .map_err(engine_apply_failure)?;
        world.record_cache_telemetry();
        if world.tree_digest != tree_digest || world.outputs != outputs {
            return Err(ApplyOutcome::Stale(vec![Diagnostic::info(
                "k8s.review.proposal_changed",
                "deterministic re-render did not match the persisted full-tree plan digest",
            )]));
        }
        Ok(world)
    }
}

#[async_trait::async_trait]
impl Connector for KubernetesConnector {
    type Desired = DesiredSlice;
    type Observation = Observation;
    type Plan = ExecutablePlan;

    fn name(&self) -> &'static str {
        crate::CONNECTOR_NAME
    }

    fn decode(&self, slice: &TargetSlice) -> Result<Self::Desired, ContractError> {
        DesiredSlice::decode(slice).map_err(Into::into)
    }

    fn validate_transition(
        &self,
        _previous_slice: &TargetSlice,
        previous_desired: &Self::Desired,
        _next_slice: &TargetSlice,
        next_desired: &Self::Desired,
    ) -> Result<(), ContractError> {
        previous_desired
            .validate_transition(next_desired)
            .map_err(Into::into)
    }

    fn concurrency_scope(&self, _desired: &Self::Desired) -> ConcurrencyScope {
        // Runner provisioning still uses shared checkout/cache paths. Ship the
        // conservative connector-wide lock until that adapter is independently
        // proven safe, then narrow to Key(environment).
        ConcurrencyScope::Connector
    }

    fn publication_id(&self, slice: &TargetSlice, outputs: &[Output]) -> Option<[u8; 16]> {
        publication_identity(slice, outputs)
    }

    async fn observe(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
    ) -> Result<Self::Observation, PlanOutcome<Self::Plan>> {
        let state = self
            .load(&desired.environment)
            .map_err(engine_plan_failure)?;
        let proposal_status = match &state.proposal {
            Some(pending) => Some(
                self.engine
                    .proposal_status(&pending.proposal)
                    .await
                    .map_err(engine_plan_failure)?,
            ),
            None => None,
        };
        Ok(Observation {
            state,
            proposal_status,
        })
    }

    async fn plan(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        observed: &Self::Observation,
    ) -> PlanOutcome<Self::Plan> {
        let policy = self.engine.publication_policy(&desired.environment);
        let digest = desired.materialization_digest();

        if desired.components.is_empty() {
            if observed.state.proposal.is_some() {
                return proposal(
                    ExecutablePlan::CancelProposal,
                    desired,
                    "cancel pending review",
                );
            }
            if policy == PublicationPolicy::PrGated && observed.state.published {
                return PlanOutcome::Failed(vec![Diagnostic::error(
                    "k8s.review.branch-deletion-unsupported",
                    "GitHub pull requests cannot propose deleting their base branch",
                )]);
            }
            if policy == PublicationPolicy::Direct && observed.state.published {
                return proposal(
                    ExecutablePlan::RemoveEnvironment,
                    desired,
                    "delete environment branch",
                );
            }
            return PlanOutcome::Ready {
                outputs: Vec::new(),
                diagnostics: Vec::new(),
                publication: None,
            };
        }

        if let Some(pending) = &observed.state.proposal {
            if policy == PublicationPolicy::Direct {
                return proposal(
                    ExecutablePlan::CancelProposal,
                    desired,
                    "cancel review before direct publication",
                );
            }
            if pending.input_digest == digest {
                return match observed
                    .proposal_status
                    .as_ref()
                    .expect("proposal status accompanies a pending proposal")
                {
                    ProposalStatus::Open => PlanOutcome::Waiting {
                        diagnostics: vec![Diagnostic::info(
                            "k8s.awaiting-review",
                            pending.proposal.url.clone(),
                        )],
                        retry: Retry::after(Duration::from_secs(15)),
                    },
                    ProposalStatus::Merged(commit) => proposal(
                        ExecutablePlan::RecordMerged {
                            commit: commit.clone(),
                        },
                        desired,
                        "record merged review",
                    ),
                    ProposalStatus::Closed => PlanOutcome::Failed(vec![Diagnostic::error(
                        "k8s.review.closed",
                        "the publication proposal was closed without merging",
                    )]),
                };
            }
        }

        if observed.state.published && observed.state.input_digest == Some(digest) {
            let commit = observed
                .state
                .commit
                .as_deref()
                .expect("published target state has a commit");
            return PlanOutcome::Ready {
                outputs: observed.state.outputs.clone(),
                diagnostics: Vec::new(),
                publication: Some(self.engine.publication_evidence(commit)),
            };
        }

        let world = match self.engine.render(desired).await {
            Ok(world) => world,
            Err(error) => return engine_plan_failure(error),
        };
        world.record_cache_telemetry();
        let plan = match policy {
            PublicationPolicy::Direct => ExecutablePlan::Publish {
                tree_digest: world.tree_digest,
                outputs: world.outputs,
            },
            PublicationPolicy::PrGated => ExecutablePlan::Propose {
                tree_digest: world.tree_digest,
                outputs: world.outputs,
            },
        };
        proposal(plan, desired, "publish deterministic rendered tree")
    }

    async fn apply(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        approved: &Approved<Self::Plan>,
    ) -> ApplyOutcome {
        match approved.plan() {
            ExecutablePlan::Publish {
                tree_digest,
                outputs,
            } => {
                let world = match self.render_verified(desired, tree_digest, outputs).await {
                    Ok(world) => world,
                    Err(outcome) => return outcome,
                };
                let commit = match self.engine.publish(desired, &world).await {
                    Ok(commit) => commit,
                    Err(error) => return engine_apply_failure(error),
                };
                let state = TargetState {
                    input_digest: Some(desired.materialization_digest()),
                    outputs: outputs.clone(),
                    commit: Some(commit),
                    published: true,
                    proposal: None,
                };
                match self.save(&desired.environment, &state) {
                    Ok(()) => ApplyOutcome::Progress(Vec::new()),
                    Err(error) => engine_apply_failure(error),
                }
            }
            ExecutablePlan::Propose {
                tree_digest,
                outputs,
            } => {
                let world = match self.render_verified(desired, tree_digest, outputs).await {
                    Ok(world) => world,
                    Err(outcome) => return outcome,
                };
                match self.engine.propose(desired, &world).await {
                    Ok(ProposalPublication::Unchanged(commit)) => {
                        let state = TargetState {
                            input_digest: Some(desired.materialization_digest()),
                            outputs: outputs.clone(),
                            commit: Some(commit),
                            published: true,
                            proposal: None,
                        };
                        match self.save(&desired.environment, &state) {
                            Ok(()) => ApplyOutcome::Progress(Vec::new()),
                            Err(error) => engine_apply_failure(error),
                        }
                    }
                    Ok(ProposalPublication::Awaiting(proposal)) => {
                        let mut state = match self.load(&desired.environment) {
                            Ok(state) => state,
                            Err(error) => return engine_apply_failure(error),
                        };
                        state.proposal = Some(PendingProposal {
                            input_digest: desired.materialization_digest(),
                            tree_digest: tree_digest.clone(),
                            outputs: outputs.clone(),
                            proposal,
                        });
                        match self.save(&desired.environment, &state) {
                            Ok(()) => ApplyOutcome::Progress(Vec::new()),
                            Err(error) => engine_apply_failure(error),
                        }
                    }
                    Err(error) => engine_apply_failure(error),
                }
            }
            ExecutablePlan::RecordMerged { commit } => {
                let mut state = match self.load(&desired.environment) {
                    Ok(state) => state,
                    Err(error) => return engine_apply_failure(error),
                };
                let Some(pending) = state.proposal.as_ref() else {
                    return ApplyOutcome::Stale(vec![Diagnostic::info(
                        "k8s.review.proposal_changed",
                        "pending proposal disappeared before merge recording",
                    )]);
                };
                if let Err(error) = self.engine.remove_proposal_branch(&pending.proposal).await {
                    return ApplyOutcome::Waiting {
                        diagnostics: error.diagnostics().to_vec(),
                        retry: Retry::after(Duration::from_secs(15)),
                    };
                }
                let pending = state
                    .proposal
                    .take()
                    .expect("pending proposal was inspected");
                state.input_digest = Some(pending.input_digest);
                state.outputs = pending.outputs;
                state.commit = Some(commit.clone());
                state.published = true;
                match self.save(&desired.environment, &state) {
                    Ok(()) => ApplyOutcome::Progress(Vec::new()),
                    Err(error) => engine_apply_failure(error),
                }
            }
            ExecutablePlan::CancelProposal => {
                let mut state = match self.load(&desired.environment) {
                    Ok(state) => state,
                    Err(error) => return engine_apply_failure(error),
                };
                let Some(pending) = state.proposal.take() else {
                    return ApplyOutcome::Progress(Vec::new());
                };
                if let Err(error) = self.engine.cancel_proposal(&pending.proposal).await {
                    return engine_apply_failure(error);
                }
                match self.save(&desired.environment, &state) {
                    Ok(()) => ApplyOutcome::Progress(Vec::new()),
                    Err(error) => engine_apply_failure(error),
                }
            }
            ExecutablePlan::RemoveEnvironment => {
                if let Err(error) = self.engine.remove(&desired.environment).await {
                    return engine_apply_failure(error);
                }
                match self.save(&desired.environment, &TargetState::default()) {
                    Ok(()) => ApplyOutcome::Progress(Vec::new()),
                    Err(error) => engine_apply_failure(error),
                }
            }
        }
    }

    async fn retire(
        &self,
        _context: RetireContext<'_>,
        desired: Option<&Self::Desired>,
    ) -> RetireOutcome {
        let Some(desired) = desired else {
            return RetireOutcome::Absent;
        };
        let mut state = match self.load(&desired.environment) {
            Ok(state) => state,
            Err(error) => return RetireOutcome::Blocked(error.diagnostics().to_vec()),
        };
        if let Some(pending) = state.proposal.take()
            && let Err(error) = self.engine.cancel_proposal(&pending.proposal).await
        {
            return RetireOutcome::Blocked(error.diagnostics().to_vec());
        }
        if let Err(error) = self.engine.remove(&desired.environment).await {
            return RetireOutcome::Blocked(error.diagnostics().to_vec());
        }
        state = TargetState::default();
        match self.save(&desired.environment, &state) {
            Ok(()) => RetireOutcome::Absent,
            Err(error) => RetireOutcome::Blocked(error.diagnostics().to_vec()),
        }
    }
}

fn proposal(
    plan: ExecutablePlan,
    desired: &DesiredSlice,
    summary: &str,
) -> PlanOutcome<ExecutablePlan> {
    PlanOutcome::Apply(PlanProposal {
        review: ReviewProjection {
            json: serde_json::json!({
                "apiVersion": "henosis.dev/k8s-plan/v1",
                "environment": desired.environment,
                "graphId": hex::encode(desired.graph_id),
                "generation": desired.generation.to_string(),
                "summary": summary,
            }),
            markdown: format!(
                "# Kubernetes publication plan\n\n- Environment: `{}`\n- Graph: `{}`\n- \
                 Generation: `{}`\n- Action: {}\n",
                desired.environment,
                hex::encode(desired.graph_id),
                desired.generation,
                summary
            ),
        },
        plan,
    })
}

fn engine_plan_failure(error: EngineError) -> PlanOutcome<ExecutablePlan> {
    PlanOutcome::Failed(error.diagnostics().to_vec())
}

fn engine_apply_failure(error: EngineError) -> ApplyOutcome {
    ApplyOutcome::Failed(error.diagnostics().to_vec())
}

fn target_state_error(action: &str, detail: String) -> EngineError {
    EngineError::from_diagnostic(Diagnostic::error(
        "k8s.review.proposal",
        format!("{action}: {detail}"),
    ))
}

/// Build the fixed producer-contract identity used by compatibility tests.
pub fn publication_identity(slice: &TargetSlice, outputs: &[Output]) -> Option<[u8; 16]> {
    if outputs.is_empty() {
        return None;
    }
    let mut name = Vec::new();
    name.extend_from_slice(b"henosis.dev/k8s-publication/v1\0");
    name.extend_from_slice(&slice.graph_id);
    name.extend_from_slice(&slice.generation.to_be_bytes());
    name.extend_from_slice(slice.connector.as_bytes());
    let mut outputs = outputs.iter().collect::<Vec<_>>();
    outputs.sort_by_key(|output| output.component_spec_hash);
    for output in outputs {
        name.extend_from_slice(&output.component_spec_hash);
        let values = serde_json::to_vec(&output.values).ok()?;
        name.extend_from_slice(&(values.len() as u64).to_be_bytes());
        name.extend_from_slice(&values);
    }
    Some(*Uuid::new_v5(&PUBLICATION_NAMESPACE, &name).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(sequence: u64) -> TargetSlice {
        TargetSlice {
            graph_id: [1; 16],
            generation: 7,
            sequence,
            connector: crate::CONNECTOR_NAME.into(),
            components: Vec::new(),
            upstream_outputs: Vec::new(),
            superseded_components: Vec::new(),
        }
    }

    fn outputs(value: &str) -> Vec<Output> {
        vec![Output {
            component_spec_hash: [2; 32],
            values: serde_json::json!({"url": value}),
        }]
    }

    #[test]
    fn publication_identity_ignores_slice_sequence() {
        assert_eq!(
            publication_identity(&slice(4), &outputs("a")),
            publication_identity(&slice(5), &outputs("a"))
        );
    }

    #[test]
    fn publication_identity_v1_bytes_are_compatible() {
        assert_eq!(
            publication_identity(&slice(4), &outputs("a")),
            Some([
                0x20, 0xdc, 0x13, 0x1a, 0x80, 0xf0, 0x52, 0x1b, 0xb3, 0x41, 0x8c, 0x74, 0x0d, 0xf8,
                0x10, 0x53,
            ])
        );
    }

    #[test]
    fn publication_identity_changes_with_complete_outputs() {
        assert_ne!(
            publication_identity(&slice(4), &outputs("a")),
            publication_identity(&slice(4), &outputs("b"))
        );
    }

    #[test]
    fn non_publishing_report_has_no_publication_identity() {
        assert_eq!(publication_identity(&slice(4), &[]), None);
    }

    #[tokio::test]
    async fn sdk_conformance_push_ready_retire() {
        connector_sdk_conformance::assert_push_ready_retire().await;
    }
}
