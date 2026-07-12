//! Durable level-triggered reconciliation and atomic callback reporting.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use henosis_proto::connect::henosis::v1::ConnectorCallbackServiceClient;
use henosis_proto::proto::henosis::v1::ComponentDisposition;
use henosis_proto::proto::henosis::v1::ComponentDispositionKind;
use henosis_proto::proto::henosis::v1::Diagnostic;
use henosis_proto::proto::henosis::v1::DiagnosticSeverity;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
use henosis_proto::proto::henosis::v1::ReportSliceRequest;
use henosis_proto::proto::henosis::v1::SliceReport;
use http::Uri;
use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::Instrument as _;
use tracing::Level;
use tracing::Span;
use uuid::Uuid;

use crate::engine::Engine;
use crate::engine::EngineError;
use crate::engine::Proposal;
use crate::engine::ProposalPublication;
use crate::engine::ProposalStatus;
use crate::engine::PublicationPolicy;
use crate::slice::DesiredSlice;
use crate::slice::SliceError;

const REPORT_NAMESPACE: Uuid = Uuid::from_bytes([
    0xa7, 0x8d, 0x2e, 0xe3, 0x18, 0x8d, 0x5b, 0x4b, 0xaf, 0x21, 0x60, 0xc4, 0xd0, 0xf7, 0xd4, 0x3c,
]);

/// A complete-report callback boundary, replaceable by a faithful test harness.
pub trait Reporter: Send + Sync + 'static {
    /// Report one atomic level observation to core.
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>>;
}

/// Core callback transport failure.
#[derive(Debug, Error)]
#[error("core callback failed: {0}")]
pub struct ReportError(String);

/// `ConnectRPC` implementation of the callback boundary.
#[derive(Clone)]
pub struct CoreReporter {
    client: ConnectorCallbackServiceClient<HttpClient>,
}

impl CoreReporter {
    /// Build a plaintext `ConnectRPC` client for the compose network.
    pub fn new(uri: Uri) -> Self {
        let client =
            ConnectorCallbackServiceClient::new(HttpClient::plaintext(), ClientConfig::new(uri));
        Self { client }
    }
}

impl Reporter for CoreReporter {
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .report_slice(request)
                .await
                .map(|_| ())
                .map_err(|error| ReportError(error.to_string()))
        })
    }
}

/// Persistent state and scratch layout.
#[derive(Clone, Debug)]
pub struct ReconcilerConfig {
    /// Durable connector state root.
    pub state_dir: PathBuf,
}

/// Request acceptance or reconciliation failure.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// Caller supplied an invalid slice or context.
    #[error("invalid slice: {0}")]
    Invalid(#[from] SliceError),
    /// The graph has been terminally retired.
    #[error("graph is retired")]
    Retired,
    /// Equal generation was reused with a different desired level.
    #[error("generation {0} was already accepted with different contents")]
    GenerationConflict(u64),
    /// Durable local state could not be read or committed.
    #[error("connector state failure: {0}")]
    State(String),
    /// Rendering or desired-state Git publication failed.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// Core did not accept the final atomic report.
    #[error(transparent)]
    Report(#[from] ReportError),
}

/// Durable reconciler. One lock serializes effects for each graph.
pub struct Reconciler {
    config: ReconcilerConfig,
    engine: Engine,
    reporter: Arc<dyn Reporter>,
    graph_locks: RwLock<HashMap<[u8; 16], Arc<Mutex<()>>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GraphState {
    environment: String,
    desired: ReconcileSliceRequest,
    #[serde(default)]
    published: Option<PublishedState>,
    #[serde(default)]
    proposal: Option<PendingProposalState>,
    #[serde(default)]
    retired: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PublishedState {
    generation: u64,
    request_id: Vec<u8>,
    report: SliceReport,
    #[serde(default)]
    commit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingProposalState {
    generation: u64,
    request_id: Vec<u8>,
    report: SliceReport,
    outputs: Vec<henosis_proto::proto::henosis::v1::ComponentOutputs>,
    proposal: Proposal,
}

impl Reconciler {
    /// Create a reconciler with real external adapters.
    pub fn new(
        config: ReconcilerConfig,
        engine: Engine,
        reporter: Arc<dyn Reporter>,
    ) -> Result<Self, ReconcileError> {
        fs::create_dir_all(&config.state_dir)
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        Ok(Self {
            config,
            engine,
            reporter,
            graph_locks: RwLock::new(HashMap::new()),
        })
    }

    /// Durably accept a desired level and schedule one reconcile pass.
    pub async fn accept(
        self: &Arc<Self>,
        request: ReconcileSliceRequest,
    ) -> Result<u64, ReconcileError> {
        let graph_id = request_graph_id(&request)?;
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let current = self.load(graph_id)?;
        if current.as_ref().is_some_and(|state| state.retired) {
            return Err(ReconcileError::Retired);
        }
        let retained_environment = current.as_ref().map(|state| state.environment.as_str());
        let desired = DesiredSlice::from_request(&request, retained_environment)?;
        let generation = desired.generation;
        if let Some(state) = current {
            let retained_generation = request_generation(&state.desired)?;
            if generation < retained_generation {
                return Ok(retained_generation);
            }
            if generation == retained_generation && request != state.desired {
                return Err(ReconcileError::GenerationConflict(generation));
            }
            if generation > retained_generation {
                self.save(
                    graph_id,
                    &GraphState {
                        environment: desired.environment,
                        desired: request,
                        published: state.published,
                        proposal: state.proposal,
                        retired: false,
                    },
                )?;
            }
        } else {
            self.save(
                graph_id,
                &GraphState {
                    environment: desired.environment,
                    desired: request,
                    published: None,
                    proposal: None,
                    retired: false,
                },
            )?;
        }
        drop(_guard);
        let reconciler = Arc::clone(self);
        tokio::spawn(async move {
            let _ = reconciler.reconcile_once(graph_id, generation).await;
        });
        Ok(generation)
    }

    /// Resume every accepted non-retired desired level after process restart.
    pub async fn resume(self: &Arc<Self>) -> Result<usize, ReconcileError> {
        let mut resumed = 0;
        for entry in fs::read_dir(&self.config.state_dir)
            .map_err(|error| ReconcileError::State(error.to_string()))?
        {
            let entry = entry.map_err(|error| ReconcileError::State(error.to_string()))?;
            let Some(stem) = entry
                .path()
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_owned)
            else {
                continue;
            };
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes =
                hex::decode(stem).map_err(|error| ReconcileError::State(error.to_string()))?;
            let graph_id: [u8; 16] = bytes
                .try_into()
                .map_err(|_| ReconcileError::State("invalid graph state filename".into()))?;
            let state = self.load(graph_id)?.ok_or_else(|| {
                ReconcileError::State("graph state disappeared during startup".into())
            })?;
            if state.retired {
                continue;
            }
            let generation = request_generation(&state.desired)?;
            let reconciler = Arc::clone(self);
            tokio::spawn(async move {
                let _ = reconciler.reconcile_once(graph_id, generation).await;
            });
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Delete native desired state and terminally fence a graph.
    pub async fn retire(&self, graph_id: [u8; 16], generation: u64) -> Result<u64, ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let mut state = self
            .load(graph_id)?
            .ok_or_else(|| ReconcileError::State("cannot retire an unknown graph slice".into()))?;
        let retained_generation = request_generation(&state.desired)?;
        if generation != retained_generation {
            return Err(ReconcileError::State(format!(
                "retire generation {generation} does not match retained generation \
                 {retained_generation}"
            )));
        }
        if state.retired {
            return Ok(retained_generation);
        }
        if let Some(proposal) = &state.proposal {
            self.engine.cancel_proposal(&proposal.proposal).await?;
        }
        self.engine.remove(&state.environment).await?;
        state.retired = true;
        state.published = None;
        state.proposal = None;
        self.save(graph_id, &state)?;
        Ok(retained_generation)
    }

    async fn reconcile_once(
        self: Arc<Self>,
        graph_id: [u8; 16],
        expected_generation: u64,
    ) -> Result<(), ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let mut state = self
            .load(graph_id)?
            .ok_or_else(|| ReconcileError::State("accepted graph state is missing".into()))?;
        if state.retired || request_generation(&state.desired)? != expected_generation {
            return Ok(());
        }
        let desired = DesiredSlice::from_request(&state.desired, Some(&state.environment))?;
        let policy = self.engine.publication_policy(&desired.environment);
        let graph = hex::encode(graph_id);
        let generation = desired.generation.to_string();
        let span = tracing::span!(
            Level::INFO,
            "k8s.reconcile_slice",
            soter.henosis.graph.id = %graph,
            soter.henosis.graph.generation = %generation,
            soter.henosis.environment.id = %desired.environment,
            soter.henosis.slice.component_count = desired.components.len(),
            soter.henosis.publication.policy = publication_policy_name(policy),
            soter.henosis.reconcile.outcome = tracing::field::Empty,
            soter.henosis.publication.commit = tracing::field::Empty,
            soter.henosis.proposal.number = tracing::field::Empty,
        );
        async {
            if let Some(published) = &state.published
                && published.generation == desired.generation
            {
                self.report_snapshot(&published.request_id, &published.report)
                    .await?;
                Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "already_published");
                if let Some(commit) = &published.commit {
                    Span::current().record(crate::telemetry::PUBLISHED_COMMIT, commit);
                }
                return Ok(());
            }

            if policy == PublicationPolicy::PrGated
                && let Some(pending) = &state.proposal
                && pending.generation == desired.generation
            {
                Span::current().record(
                    crate::telemetry::PROPOSAL_NUMBER,
                    pending.proposal.number.to_string(),
                );
                match self.engine.proposal_status(&pending.proposal).await? {
                    ProposalStatus::Open => {
                        self.schedule_reconcile(graph_id, desired.generation);
                        self.report_snapshot(&pending.request_id, &pending.report)
                            .await?;
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "awaiting_review");
                        return Ok(());
                    }
                    ProposalStatus::Merged(commit) => {
                        let report = report_for(
                            &desired,
                            ComponentDispositionKind::Ready,
                            pending.outputs.clone(),
                            Vec::new(),
                        );
                        let published = PublishedState {
                            generation: desired.generation,
                            request_id: stable_report_id(&report),
                            report,
                            commit: Some(commit.clone()),
                        };
                        state.published = Some(published.clone());
                        let proposal = state.proposal.take().expect("proposal was inspected");
                        self.save(graph_id, &state)?;
                        self.report_snapshot(&published.request_id, &published.report)
                            .await?;
                        let _ = self.engine.remove_proposal_branch(&proposal.proposal).await;
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "proposal_merged");
                        Span::current().record(crate::telemetry::PUBLISHED_COMMIT, commit);
                        return Ok(());
                    }
                    ProposalStatus::Closed => {
                        let report = report_for(
                            &desired,
                            ComponentDispositionKind::Failed,
                            Vec::new(),
                            vec![diagnostic(
                                "k8s.review.closed",
                                "the publication proposal was closed without merging",
                                DiagnosticSeverity::Error,
                            )],
                        );
                        let _ = self.reporter.report(report_request(report, false)).await;
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "proposal_closed");
                        return Ok(());
                    }
                }
            }

            let reconciling = report_for(
                &desired,
                ComponentDispositionKind::Reconciling,
                Vec::new(),
                Vec::new(),
            );
            let _ = self
                .reporter
                .report(report_request(reconciling, false))
                .await;

            if let Some(pending) = state.proposal.take()
                && (desired.components.is_empty() || policy == PublicationPolicy::Direct)
            {
                if let Err(error) = self.engine.cancel_proposal(&pending.proposal).await {
                    self.report_failure(&desired, &error).await;
                    Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "failed");
                    return Err(error.into());
                }
                self.save(graph_id, &state)?;
            }

            if desired.components.is_empty()
                && policy == PublicationPolicy::PrGated
                && state.published.is_some()
            {
                let report = report_for(
                    &desired,
                    ComponentDispositionKind::Failed,
                    Vec::new(),
                    vec![diagnostic(
                        "k8s.review.branch-deletion-unsupported",
                        "GitHub pull requests cannot propose deleting their base branch",
                        DiagnosticSeverity::Error,
                    )],
                );
                let _ = self.reporter.report(report_request(report, false)).await;
                Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "failed");
                return Ok(());
            }

            let result = if desired.components.is_empty() {
                if policy == PublicationPolicy::Direct {
                    self.engine
                        .remove(&desired.environment)
                        .await
                        .map(|()| (Vec::new(), None))
                } else {
                    Ok((Vec::new(), None))
                }
            } else {
                match self.engine.render(&desired).await {
                    Ok(world) if policy == PublicationPolicy::Direct => self
                        .engine
                        .publish(&desired, &world)
                        .await
                        .map(|commit| (world.outputs, Some(commit))),
                    Ok(world) => match self.engine.propose(&desired, &world).await {
                        Ok(ProposalPublication::Unchanged(commit)) => {
                            Ok((world.outputs, Some(commit)))
                        }
                        Ok(ProposalPublication::Awaiting(proposal)) => {
                            let report = awaiting_review_report(&desired, &proposal.url);
                            let pending = PendingProposalState {
                                generation: desired.generation,
                                request_id: stable_report_id(&report),
                                report,
                                outputs: world.outputs,
                                proposal,
                            };
                            Span::current().record(
                                crate::telemetry::PROPOSAL_NUMBER,
                                pending.proposal.number.to_string(),
                            );
                            state.proposal = Some(pending.clone());
                            self.save(graph_id, &state)?;
                            self.schedule_reconcile(graph_id, desired.generation);
                            self.report_snapshot(&pending.request_id, &pending.report)
                                .await?;
                            Span::current()
                                .record(crate::telemetry::RECONCILE_OUTCOME, "awaiting_review");
                            return Ok(());
                        }
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            };
            let (outputs, commit) = match result {
                Ok(result) => result,
                Err(error) => {
                    self.report_failure(&desired, &error).await;
                    Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "failed");
                    return Err(error.into());
                }
            };
            state.proposal = None;
            let report = report_for(
                &desired,
                ComponentDispositionKind::Ready,
                outputs,
                Vec::new(),
            );
            let request_id = stable_report_id(&report);
            let published = PublishedState {
                generation: desired.generation,
                request_id,
                report,
                commit: commit.clone(),
            };
            state.published = Some(published.clone());
            self.save(graph_id, &state)?;
            self.report_snapshot(&published.request_id, &published.report)
                .await?;
            Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "published");
            if let Some(commit) = commit {
                Span::current().record(crate::telemetry::PUBLISHED_COMMIT, commit);
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn report_failure(&self, desired: &DesiredSlice, error: &EngineError) {
        let report = report_for(
            desired,
            ComponentDispositionKind::Failed,
            Vec::new(),
            error.diagnostics().to_vec(),
        );
        let _ = self.reporter.report(report_request(report, false)).await;
    }

    async fn report_snapshot(
        &self,
        request_id: &[u8],
        report: &SliceReport,
    ) -> Result<(), ReportError> {
        let request = ReportSliceRequest {
            request_id: Some(request_id.to_vec()),
            report: buffa::MessageField::some(report.clone()),
            ..Default::default()
        };
        let mut last = None;
        for delay in [0, 1, 2] {
            if delay > 0 {
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            match self.reporter.report(request.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| ReportError("report retry loop did not run".into())))
    }

    fn schedule_reconcile(self: &Arc<Self>, graph_id: [u8; 16], generation: u64) {
        let reconciler = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let _ = reconciler.reconcile_once(graph_id, generation).await;
        });
    }

    async fn graph_lock(&self, graph_id: [u8; 16]) -> Arc<Mutex<()>> {
        if let Some(lock) = self.graph_locks.read().await.get(&graph_id).cloned() {
            return lock;
        }
        self.graph_locks
            .write()
            .await
            .entry(graph_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn load(&self, graph_id: [u8; 16]) -> Result<Option<GraphState>, ReconcileError> {
        let path = self.state_path(graph_id);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReconcileError::State(error.to_string())),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| ReconcileError::State(error.to_string()))
    }

    fn save(&self, graph_id: [u8; 16], state: &GraphState) -> Result<(), ReconcileError> {
        let path = self.state_path(graph_id);
        let bytes =
            serde_json::to_vec(state).map_err(|error| ReconcileError::State(error.to_string()))?;
        let mut temporary = NamedTempFile::new_in(&self.config.state_dir)
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        temporary
            .write_all(&bytes)
            .and_then(|()| temporary.as_file().sync_all())
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        temporary
            .persist(path)
            .map_err(|error| ReconcileError::State(error.error.to_string()))?;
        sync_directory(&self.config.state_dir)?;
        Ok(())
    }

    fn state_path(&self, graph_id: [u8; 16]) -> PathBuf {
        self.config
            .state_dir
            .join(format!("{}.json", hex::encode(graph_id)))
    }
}

fn report_for(
    desired: &DesiredSlice,
    kind: ComponentDispositionKind,
    outputs: Vec<henosis_proto::proto::henosis::v1::ComponentOutputs>,
    diagnostics: Vec<Diagnostic>,
) -> SliceReport {
    let dispositions = desired
        .components
        .values()
        .map(|component| {
            ComponentDisposition::default()
                .with_component_id(component.id.to_vec())
                .with_kind(kind)
        })
        .collect();
    SliceReport {
        graph_id: Some(desired.graph_id.to_vec()),
        generation: Some(desired.generation),
        connector: Some(crate::CONNECTOR_NAME.into()),
        dispositions,
        outputs,
        diagnostics,
        ..Default::default()
    }
}

fn awaiting_review_report(desired: &DesiredSlice, url: &str) -> SliceReport {
    report_for(
        desired,
        ComponentDispositionKind::Reconciling,
        Vec::new(),
        vec![diagnostic(
            "k8s.awaiting-review",
            url,
            DiagnosticSeverity::Info,
        )],
    )
}

fn diagnostic(code: &str, message: &str, severity: DiagnosticSeverity) -> Diagnostic {
    Diagnostic::default()
        .with_code(code)
        .with_message(message)
        .with_severity(severity)
}

const fn publication_policy_name(policy: PublicationPolicy) -> &'static str {
    match policy {
        PublicationPolicy::Direct => "direct",
        PublicationPolicy::PrGated => "pr-gated",
    }
}

fn report_request(report: SliceReport, stable: bool) -> ReportSliceRequest {
    let request_id = if stable {
        stable_report_id(&report)
    } else {
        Uuid::now_v7().as_bytes().to_vec()
    };
    ReportSliceRequest {
        request_id: Some(request_id),
        report: buffa::MessageField::some(report),
        ..Default::default()
    }
}

fn stable_report_id(report: &SliceReport) -> Vec<u8> {
    let bytes = serde_json::to_vec(report).expect("generated report is JSON serializable");
    Uuid::new_v5(&REPORT_NAMESPACE, &bytes).as_bytes().to_vec()
}

fn request_graph_id(request: &ReconcileSliceRequest) -> Result<[u8; 16], ReconcileError> {
    request
        .slice
        .as_option()
        .and_then(|slice| slice.graph_id.as_deref())
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| ReconcileError::State("slice.graph_id must contain exactly 16 bytes".into()))
}

fn request_generation(request: &ReconcileSliceRequest) -> Result<u64, ReconcileError> {
    request
        .slice
        .as_option()
        .and_then(|slice| slice.generation)
        .filter(|generation| *generation > 0)
        .ok_or_else(|| ReconcileError::State("slice.generation is missing".into()))
}

fn sync_directory(directory: &Path) -> Result<(), ReconcileError> {
    fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| ReconcileError::State(error.to_string()))
}

/// Build a connector-owned diagnostic for request-boundary validation failures.
pub fn validation_diagnostic(error: &ReconcileError) -> Diagnostic {
    Diagnostic::default()
        .with_code("k8s.context.invalid")
        .with_message(error.to_string())
        .with_severity(DiagnosticSeverity::Error)
}
