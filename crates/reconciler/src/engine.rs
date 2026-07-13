//! Real platform-runner and desired-state Git adapters.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;

use connector_sdk::ContractFailureDetail;
use connector_sdk::ContractFailureKind;
use connector_sdk::Diagnostic;
use connector_sdk::Output as ComponentOutput;
use connector_sdk::Publication;
use reqwest::Method;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use thiserror::Error;
use tokio::process::Command;
use tracing::Span;

use crate::render_cache::CacheLookup;
use crate::render_cache::RenderCache;
use crate::render_cache::tree_digest;
use crate::review::ReviewProjection;
use crate::slice::DesiredSlice;

const STRUCTURED_FAILURE_PREFIX: &str = "HENOSIS_GATE_REPORT:";
const GITHUB_API_ROOT: &str = "https://api.github.com";
const GITHUB_API_VERSION: &str = "2026-03-10";

/// How one environment's rendered tree becomes applied state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublicationPolicy {
    /// Replace the environment branch immediately with force-with-lease.
    #[default]
    Direct,
    /// Update a stable proposal branch and require a pull-request merge.
    PrGated,
}

/// Connector-owned per-environment publication policy configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationPolicies {
    /// Policy used when an environment has no explicit entry.
    #[serde(default)]
    pub default: PublicationPolicy,
    /// Exact environment-ID overrides.
    #[serde(default)]
    pub environments: BTreeMap<String, PublicationPolicy>,
}

impl PublicationPolicies {
    /// Resolve the effective policy for an environment identity.
    pub fn for_environment(&self, environment: &str) -> PublicationPolicy {
        self.environments
            .get(environment)
            .copied()
            .unwrap_or(self.default)
    }
}

/// External execution and publication configuration.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Platform's D24 runner provisioning recipe.
    pub prepare_runner: PathBuf,
    /// Platform Git ref resolved by the provisioning recipe.
    pub platform_ref: String,
    /// Mutable checkout used by the provisioning recipe.
    pub platform_checkout: PathBuf,
    /// Persistent SHA-addressed runner cache.
    pub runner_cache: PathBuf,
    /// Allowed deploy repository remote.
    pub deploy_remote: String,
    /// File containing the GitHub PAT.
    pub github_token_file: PathBuf,
    /// Root for isolated render and publication staging.
    pub scratch_root: PathBuf,
    /// Maximum successful deterministic render recipes retained locally.
    /// Zero disables memoization.
    pub render_cache_max_entries: usize,
    /// Default and per-environment publication policies.
    pub publication_policies: PublicationPolicies,
}

/// Successful rendered world, before Git publication.
#[derive(Debug)]
pub struct RenderedWorld {
    /// Temporary directory retained through publication.
    _temporary: TempDir,
    /// Root containing only publishable renderer output.
    pub output_dir: PathBuf,
    /// Complete deterministic component outputs.
    pub outputs: Vec<ComponentOutput>,
    /// Digest of every path and byte in the exact rendered publication tree.
    pub tree_digest: String,
    cache: RenderCacheObservation,
}

#[derive(Debug)]
struct RenderCacheObservation {
    status: &'static str,
    reason: Option<&'static str>,
    recipe: String,
    platform_sha: String,
    evicted: usize,
}

impl RenderedWorld {
    /// Attach deterministic-render cache facts to the enclosing reconcile
    /// wide event after rendering succeeds.
    pub fn record_cache_telemetry(&self) {
        let span = Span::current();
        span.record(crate::telemetry::RENDER_CACHE_STATUS, self.cache.status);
        span.record(crate::telemetry::RENDER_RECIPE, self.cache.recipe.as_str());
        span.record(
            crate::telemetry::RENDER_PLATFORM_SHA,
            self.cache.platform_sha.as_str(),
        );
        span.record(
            crate::telemetry::RENDER_CACHE_EVICTED_COUNT,
            self.cache.evicted,
        );
        if let Some(reason) = self.cache.reason {
            span.record(crate::telemetry::RENDER_CACHE_REASON, reason);
        }
    }
}

/// Durable coordinates for one open GitHub review proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    /// GitHub pull-request number.
    pub number: u64,
    /// Browser URL attached to the pending slice report.
    pub url: String,
    /// Exact proposed head commit.
    pub commit: String,
    /// Applied-state branch used as the PR base.
    pub target_branch: String,
    /// Stable proposal branch used as the PR head.
    pub proposal_branch: String,
}

/// Result of attempting PR-gated publication.
#[derive(Debug)]
pub enum ProposalPublication {
    /// The rendered tree already equals the applied target branch.
    Unchanged(String),
    /// Review is required before this tree becomes applied.
    Awaiting(Proposal),
}

/// Current state of a durable review proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalStatus {
    /// The PR is still open and awaiting review.
    Open,
    /// The PR was merged; the target branch now contains this commit.
    Merged(String),
    /// The PR was closed without merging.
    Closed,
}

/// External adapter failure with reportable source diagnostics.
#[derive(Debug, Error)]
#[error("{summary}")]
pub struct EngineError {
    summary: String,
    diagnostics: Vec<Diagnostic>,
}

impl EngineError {
    /// Construct one target-adapter failure from a stable diagnostic.
    pub fn from_diagnostic(diagnostic: Diagnostic) -> Self {
        Self {
            summary: diagnostic.message.clone(),
            diagnostics: vec![diagnostic],
        }
    }

    /// Diagnostics suitable for a FAILED atomic slice report.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
}

/// Renderer and deploy Git implementation.
#[derive(Clone, Debug)]
pub struct Engine {
    config: EngineConfig,
    github: GitHubApi,
    render_cache: RenderCache,
}

#[derive(Clone, Debug)]
struct PreparedRunner {
    entrypoint: PathBuf,
    platform_sha: String,
    prepare_recipe_digest: [u8; 32],
    entrypoint_digest: [u8; 32],
}

impl Engine {
    /// Build an engine after enforcing the allowed GitHub organization
    /// boundary.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        let repository = parse_deploy_repository(&config.deploy_remote)?;
        let github = GitHubApi {
            client: reqwest::Client::builder()
                .user_agent("henosis-connector-k8s")
                .build()
                .map_err(|error| simple_error("k8s.github.client", &error.to_string()))?,
            repository,
            token_file: config.github_token_file.clone(),
        };
        let render_cache = RenderCache::new(
            config.scratch_root.join("render-cache-v1"),
            config.render_cache_max_entries,
        );
        Ok(Self {
            config,
            github,
            render_cache,
        })
    }

    /// Resolve the connector policy for one validated environment.
    pub fn publication_policy(&self, environment: &str) -> PublicationPolicy {
        self.config
            .publication_policies
            .for_environment(environment)
    }

    /// Build immutable browser evidence for one exact deploy revision.
    pub fn publication_evidence(&self, revision: &str) -> Publication {
        Publication {
            revision: revision.to_owned(),
            uri: format!(
                "https://github.com/{}/{}/commit/{revision}",
                self.github.repository.owner, self.github.repository.name
            ),
        }
    }

    /// Provision the real platform runner and render a complete world in
    /// isolation.
    pub async fn render(&self, desired: &DesiredSlice) -> Result<RenderedWorld, EngineError> {
        let effective_environment = desired
            .borrowed_environment()
            .unwrap_or(&desired.environment);
        let desired_manifest = desired
            .manifest_toml(effective_environment)
            .map_err(|error| simple_error("k8s.context.invalid", &error.to_string()))?;
        let dev_manifest = desired
            .manifest_toml("dev")
            .map_err(|error| simple_error("k8s.context.invalid", &error.to_string()))?;
        fs::create_dir_all(&self.config.scratch_root).map_err(|error| {
            io_error(
                "k8s.renderer.scratch",
                "create renderer scratch root",
                error,
            )
        })?;
        let temporary = tempfile::Builder::new()
            .prefix("reconcile-")
            .tempdir_in(&self.config.scratch_root)
            .map_err(|error| io_error("k8s.renderer.scratch", "create render staging", error))?;
        let manifest_dir = temporary.path().join("manifests");
        let output_dir = temporary.path().join("rendered");
        fs::create_dir_all(&manifest_dir)
            .map_err(|error| io_error("k8s.renderer.manifest", "create manifest staging", error))?;
        fs::write(manifest_dir.join("desired.toml"), &desired_manifest)
            .map_err(|error| io_error("k8s.renderer.manifest", "write desired manifest", error))?;
        fs::write(manifest_dir.join("dev.toml"), &dev_manifest)
            .map_err(|error| io_error("k8s.renderer.manifest", "write dev manifest", error))?;
        fs::create_dir_all(&output_dir)
            .map_err(|error| io_error("k8s.renderer.scratch", "create render output", error))?;

        let runner = self.prepare_runner().await?;
        let recipe = render_recipe_key(desired, &desired_manifest, &dev_manifest, &runner);
        let mut cache_status = "miss";
        let mut cache_reason = None;
        let mut evicted = 0;
        if self.render_cache.enabled() {
            match self.render_cache.restore(&recipe, &output_dir) {
                Ok(CacheLookup::Hit) => {
                    match read_render_outputs(&output_dir, desired, effective_environment).and_then(
                        |outputs| validate_generation_receipt_slot(&output_dir).map(|()| outputs),
                    ) {
                        Ok(outputs) => {
                            embed_generation_receipt(&output_dir, desired)?;
                            let tree_digest = tree_digest(&output_dir).map_err(|error| {
                                io_error("k8s.renderer.output", "digest rendered tree", error)
                            })?;
                            return Ok(RenderedWorld {
                                _temporary: temporary,
                                output_dir,
                                outputs,
                                tree_digest,
                                cache: RenderCacheObservation {
                                    status: "hit",
                                    reason: None,
                                    recipe,
                                    platform_sha: runner.platform_sha,
                                    evicted,
                                },
                            });
                        }
                        Err(_) => {
                            let _ = self.render_cache.invalidate(&recipe);
                            reset_render_output(&output_dir)?;
                            cache_reason = Some("invalid_entry");
                        }
                    }
                }
                Ok(CacheLookup::Miss) => {}
                Err(_) => {
                    let _ = self.render_cache.invalidate(&recipe);
                    reset_render_output(&output_dir)?;
                    cache_reason = Some("read_failed");
                }
            }
        } else {
            cache_status = "uncacheable";
            cache_reason = Some("disabled");
        }

        let rendered = Command::new(&runner.entrypoint)
            .arg("render")
            .arg(manifest_dir.join("desired.toml"))
            .arg("--output-dir")
            .arg(&output_dir)
            .env("GITHUB_ACTIONS", "true")
            .output()
            .await
            .map_err(|error| io_error("k8s.renderer.execute", "start platform renderer", error))?;
        if !rendered.status.success() {
            return Err(render_command_error(&rendered, desired));
        }
        let outputs = read_render_outputs(&output_dir, desired, effective_environment)?;
        validate_generation_receipt_slot(&output_dir)?;
        if self.render_cache.enabled() {
            match self.render_cache.store(&recipe, &output_dir) {
                Ok(stored) => evicted = stored.evicted,
                Err(_) => {
                    cache_status = "uncacheable";
                    cache_reason = Some("write_failed");
                }
            }
        }
        embed_generation_receipt(&output_dir, desired)?;
        let tree_digest = tree_digest(&output_dir)
            .map_err(|error| io_error("k8s.renderer.output", "digest rendered tree", error))?;
        Ok(RenderedWorld {
            _temporary: temporary,
            output_dir,
            outputs,
            tree_digest,
            cache: RenderCacheObservation {
                status: cache_status,
                reason: cache_reason,
                recipe,
                platform_sha: runner.platform_sha,
                evicted,
            },
        })
    }

    async fn prepare_runner(&self) -> Result<PreparedRunner, EngineError> {
        let prepared = Command::new(&self.config.prepare_runner)
            .arg(&self.config.platform_ref)
            .env("HENOSIS_PLATFORM_CHECKOUT", &self.config.platform_checkout)
            .env("HENOSIS_RUNNER_CACHE_DIR", &self.config.runner_cache)
            .output()
            .await
            .map_err(|error| io_error("k8s.runner.prepare", "start prepare-runner", error))?;
        if !prepared.status.success() {
            return Err(command_error(
                "k8s.runner.prepare",
                "prepare platform runner",
                &prepared,
            ));
        }
        let entrypoint = String::from_utf8(prepared.stdout)
            .map_err(|error| simple_error("k8s.runner.prepare", &error.to_string()))?;
        let entrypoint = PathBuf::from(entrypoint.trim());
        if entrypoint.as_os_str().is_empty() {
            return Err(simple_error(
                "k8s.runner.prepare",
                "prepare-runner returned an empty entrypoint",
            ));
        }
        let runner_root = entrypoint.parent().ok_or_else(|| {
            simple_error(
                "k8s.runner.prepare",
                "prepare-runner returned an entrypoint without a parent directory",
            )
        })?;
        let platform_sha =
            fs::read_to_string(runner_root.join(".henosis-platform-sha")).map_err(|error| {
                io_error(
                    "k8s.runner.identity",
                    "read prepared platform identity",
                    error,
                )
            })?;
        let platform_sha = platform_sha.trim().to_owned();
        if !is_commit_sha(&platform_sha) {
            return Err(simple_error(
                "k8s.runner.identity",
                "prepared platform identity is not a full lowercase Git commit SHA",
            ));
        }
        let prepare_recipe = fs::read(&self.config.prepare_runner).map_err(|error| {
            io_error("k8s.runner.identity", "read prepare-runner recipe", error)
        })?;
        let entrypoint_bytes = fs::read(&entrypoint)
            .map_err(|error| io_error("k8s.runner.identity", "read runner entrypoint", error))?;
        Ok(PreparedRunner {
            entrypoint,
            platform_sha,
            prepare_recipe_digest: *blake3::hash(&prepare_recipe).as_bytes(),
            entrypoint_digest: *blake3::hash(&entrypoint_bytes).as_bytes(),
        })
    }

    /// Replace the environment branch with one complete successful render.
    pub async fn publish(
        &self,
        desired: &DesiredSlice,
        world: &RenderedWorld,
    ) -> Result<String, EngineError> {
        let repository = self.git_repository(&desired.environment).await?;
        clear_worktree(repository.path())?;
        copy_tree(&world.output_dir, repository.path())?;
        git(repository.path(), &self.config, ["add", "--all"]).await?;
        let diff = git_status(
            repository.path(),
            &self.config,
            ["diff", "--cached", "--quiet"],
        )
        .await?;
        if diff.status.code() == Some(1) {
            git(
                repository.path(),
                &self.config,
                [
                    "commit",
                    "-m",
                    &format!(
                        "Render {} graph {} generation {}",
                        desired.environment,
                        hex::encode(desired.graph_id),
                        desired.generation
                    ),
                ],
            )
            .await?;
        } else if !diff.status.success() {
            return Err(command_error(
                "k8s.publisher.git",
                "inspect rendered diff",
                &diff,
            ));
        }
        let commit = git_stdout(repository.path(), &self.config, ["rev-parse", "HEAD"]).await?;
        let branch_ref = format!("refs/heads/env/{}", desired.environment);
        let lease = format!(
            "--force-with-lease={branch_ref}:{}",
            repository.expected_sha
        );
        git(
            repository.path(),
            &self.config,
            ["push", &lease, "origin", &format!("HEAD:{branch_ref}")],
        )
        .await?;
        Ok(commit.trim().into())
    }

    /// Publish a rendered tree to a stable proposal branch and open or update
    /// its pull request against the environment branch.
    pub async fn propose(
        &self,
        desired: &DesiredSlice,
        world: &RenderedWorld,
    ) -> Result<ProposalPublication, EngineError> {
        let mut repository = self.git_repository(&desired.environment).await?;
        let target_branch = environment_branch(&desired.environment);
        let target_ref = format!("refs/heads/{target_branch}");
        if repository.expected_sha.is_empty() {
            git(
                repository.path(),
                &self.config,
                [
                    "commit",
                    "--allow-empty",
                    "-m",
                    &format!("Initialize review target for {}", desired.environment),
                ],
            )
            .await?;
            let commit = git_stdout(repository.path(), &self.config, ["rev-parse", "HEAD"]).await?;
            let lease = format!("--force-with-lease={target_ref}:");
            git(
                repository.path(),
                &self.config,
                ["push", &lease, "origin", &format!("HEAD:{target_ref}")],
            )
            .await?;
            repository.expected_sha = commit.trim().into();
        }

        clear_worktree(repository.path())?;
        copy_tree(&world.output_dir, repository.path())?;
        git(repository.path(), &self.config, ["add", "--all"]).await?;
        let diff = git_status(
            repository.path(),
            &self.config,
            ["diff", "--cached", "--quiet"],
        )
        .await?;
        if diff.status.success() {
            return Ok(ProposalPublication::Unchanged(repository.expected_sha));
        }
        if diff.status.code() != Some(1) {
            return Err(command_error(
                "k8s.publisher.git",
                "inspect proposed rendered diff",
                &diff,
            ));
        }
        let name_status = git_output(
            repository.path(),
            &self.config,
            ["diff", "--cached", "--name-status", "--no-renames", "-z"],
        )
        .await?;
        let patch = git_stdout(
            repository.path(),
            &self.config,
            [
                "diff",
                "--cached",
                "--no-color",
                "--no-ext-diff",
                "--full-index",
            ],
        )
        .await?;
        git(
            repository.path(),
            &self.config,
            [
                "commit",
                "-m",
                &format!(
                    "Propose {} graph {} generation {}",
                    desired.environment,
                    hex::encode(desired.graph_id),
                    desired.generation
                ),
            ],
        )
        .await?;
        let commit = git_stdout(repository.path(), &self.config, ["rev-parse", "HEAD"]).await?;
        let commit = commit.trim().to_owned();
        let proposal_branch = proposal_branch(&desired.environment);
        let proposal_ref = format!("refs/heads/{proposal_branch}");
        let proposal_sha = remote_ref_sha(repository.path(), &self.config, &proposal_ref).await?;
        let lease = format!(
            "--force-with-lease={proposal_ref}:{}",
            proposal_sha.as_deref().unwrap_or_default()
        );
        git(
            repository.path(),
            &self.config,
            ["push", &lease, "origin", &format!("HEAD:{proposal_ref}")],
        )
        .await?;

        let component_names = desired.component_names();
        let projection = ReviewProjection::from_name_status(
            &desired.environment,
            target_branch.clone(),
            proposal_branch.clone(),
            commit.clone(),
            &component_names,
            &name_status.stdout,
        )
        .map_err(|error| simple_error("k8s.review.projection", &error.to_string()))?;
        let document = projection
            .document(&patch)
            .map_err(|error| simple_error("k8s.review.projection", &error.to_string()))?;
        let pull = self
            .github
            .upsert_pull(
                &target_branch,
                &proposal_branch,
                &format!("Apply rendered environment {}", desired.environment),
                &document,
                &commit,
            )
            .await?;
        Ok(ProposalPublication::Awaiting(Proposal {
            number: pull.number,
            url: pull.html_url,
            commit,
            target_branch,
            proposal_branch,
        }))
    }

    /// Inspect a previously persisted proposal without re-rendering it.
    pub async fn proposal_status(
        &self,
        proposal: &Proposal,
    ) -> Result<ProposalStatus, EngineError> {
        let pull = self.github.get_pull(proposal.number).await?;
        if pull.head.sha != proposal.commit {
            return Err(simple_error(
                "k8s.review.proposal_changed",
                "GitHub pull request head differs from the persisted proposed commit",
            ));
        }
        if pull.merged_at.is_some() {
            let Some(commit) = pull.merge_commit_sha else {
                // GitHub can expose merged_at before merge_commit_sha converges.
                // Keep polling rather than terminally failing an applied proposal.
                return Ok(ProposalStatus::Open);
            };
            if !is_commit_sha(&commit) {
                return Err(simple_error(
                    "k8s.review.merge",
                    "merged pull request returned an invalid merge commit identity",
                ));
            }
            return Ok(ProposalStatus::Merged(commit));
        }
        if pull.state == "open" {
            Ok(ProposalStatus::Open)
        } else {
            Ok(ProposalStatus::Closed)
        }
    }

    /// Close an unmerged proposal and remove its stable proposal branch.
    pub async fn cancel_proposal(&self, proposal: &Proposal) -> Result<(), EngineError> {
        let pull = self.github.get_pull(proposal.number).await?;
        if pull.merged_at.is_none() && pull.state == "open" {
            self.github.close_pull(proposal.number).await?;
        }
        self.remove_proposal_branch(proposal).await
    }

    /// Remove a merged proposal branch after observing application.
    pub async fn remove_proposal_branch(&self, proposal: &Proposal) -> Result<(), EngineError> {
        let repository = self
            .git_repository(&proposal_branch_environment(&proposal.proposal_branch)?)
            .await?;
        let proposal_ref = format!("refs/heads/{}", proposal.proposal_branch);
        let current = remote_ref_sha(repository.path(), &self.config, &proposal_ref).await?;
        let Some(current) = current else {
            return Ok(());
        };
        if current != proposal.commit {
            return Err(simple_error(
                "k8s.review.proposal_changed",
                "proposal branch changed outside the connector",
            ));
        }
        let lease = format!("--force-with-lease={proposal_ref}:{}", proposal.commit);
        git(
            repository.path(),
            &self.config,
            ["push", &lease, "origin", &format!(":{proposal_ref}")],
        )
        .await
    }

    /// Resolve the immutable deploy revision currently backing an environment.
    pub async fn applied_revision(&self, environment: &str) -> Result<String, EngineError> {
        let repository = self.git_repository(environment).await?;
        if repository.expected_sha.is_empty() {
            return Err(simple_error(
                "k8s.publisher.git",
                &format!("borrow target environment {environment:?} has no rendered branch"),
            ));
        }
        Ok(repository.expected_sha)
    }

    /// Delete an environment branch if it exists.
    pub async fn remove(&self, environment: &str) -> Result<(), EngineError> {
        let repository = self.git_repository(environment).await?;
        if repository.expected_sha.is_empty() {
            return Ok(());
        }
        let branch_ref = format!("refs/heads/env/{environment}");
        let lease = format!(
            "--force-with-lease={branch_ref}:{}",
            repository.expected_sha
        );
        git(
            repository.path(),
            &self.config,
            ["push", &lease, "origin", &format!(":{branch_ref}")],
        )
        .await
    }

    async fn git_repository(&self, environment: &str) -> Result<GitRepository, EngineError> {
        fs::create_dir_all(&self.config.scratch_root).map_err(|error| {
            io_error(
                "k8s.publisher.scratch",
                "create publisher scratch root",
                error,
            )
        })?;
        let temporary = tempfile::Builder::new()
            .prefix("publish-")
            .tempdir_in(&self.config.scratch_root)
            .map_err(|error| io_error("k8s.publisher.scratch", "create Git staging", error))?;
        git(temporary.path(), &self.config, ["init", "--quiet"]).await?;
        git(
            temporary.path(),
            &self.config,
            ["remote", "add", "origin", &self.config.deploy_remote],
        )
        .await?;
        git(
            temporary.path(),
            &self.config,
            ["config", "user.name", "Henosis Connector"],
        )
        .await?;
        git(
            temporary.path(),
            &self.config,
            [
                "config",
                "user.email",
                "henosis-agent@users.noreply.github.com",
            ],
        )
        .await?;
        let branch_ref = format!("refs/heads/env/{environment}");
        let existing = git_status(
            temporary.path(),
            &self.config,
            ["ls-remote", "--exit-code", "--heads", "origin", &branch_ref],
        )
        .await?;
        let expected_sha = if existing.status.success() {
            let line = String::from_utf8(existing.stdout)
                .map_err(|error| simple_error("k8s.publisher.git", &error.to_string()))?;
            let sha = line
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_owned();
            git(
                temporary.path(),
                &self.config,
                ["fetch", "--depth=1", "origin", &branch_ref],
            )
            .await?;
            git(
                temporary.path(),
                &self.config,
                ["checkout", "--detach", "FETCH_HEAD"],
            )
            .await?;
            sha
        } else if existing.status.code() == Some(2) {
            git(
                temporary.path(),
                &self.config,
                ["checkout", "--orphan", "render"],
            )
            .await?;
            String::new()
        } else {
            return Err(command_error(
                "k8s.publisher.git",
                "query environment branch",
                &existing,
            ));
        };
        Ok(GitRepository {
            temporary,
            expected_sha,
        })
    }
}

struct GitRepository {
    temporary: TempDir,
    expected_sha: String,
}

#[derive(Clone, Debug)]
struct GitHubApi {
    client: reqwest::Client,
    repository: DeployRepository,
    token_file: PathBuf,
}

#[derive(Clone, Debug)]
struct DeployRepository {
    owner: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct PullRequest {
    number: u64,
    html_url: String,
    state: String,
    merged_at: Option<String>,
    merge_commit_sha: Option<String>,
    head: PullHead,
}

#[derive(Debug, Deserialize)]
struct PullHead {
    sha: String,
}

impl GitHubApi {
    async fn upsert_pull(
        &self,
        base: &str,
        head: &str,
        title: &str,
        body: &str,
        expected_head: &str,
    ) -> Result<PullRequest, EngineError> {
        let qualified_head = format!("{}:{head}", self.repository.owner);
        let pulls = self
            .request::<Vec<PullRequest>>(
                Method::GET,
                "pulls",
                &[("state", "open"), ("base", base), ("head", &qualified_head)],
                None,
            )
            .await?;
        if pulls.len() > 1 {
            return Err(simple_error(
                "k8s.github.pr",
                "multiple open pull requests use the connector proposal branch",
            ));
        }
        let update = serde_json::json!({ "title": title, "body": body });
        let pull = if let Some(pull) = pulls.into_iter().next() {
            self.request(
                Method::PATCH,
                &format!("pulls/{}", pull.number),
                &[],
                Some(update),
            )
            .await?
        } else {
            let create = serde_json::json!({
                "title": title,
                "body": body,
                "head": head,
                "base": base,
            });
            self.request(Method::POST, "pulls", &[], Some(create))
                .await?
        };
        self.wait_for_pull_head(pull, expected_head).await
    }

    async fn wait_for_pull_head(
        &self,
        mut pull: PullRequest,
        expected_head: &str,
    ) -> Result<PullRequest, EngineError> {
        for delay in [0, 1, 2, 4] {
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                pull = self.get_pull(pull.number).await?;
            }
            if pull.head.sha == expected_head {
                return Ok(pull);
            }
        }
        Err(simple_error(
            "k8s.review.proposal_changed",
            "GitHub pull request head did not converge to the proposed commit",
        ))
    }

    async fn get_pull(&self, number: u64) -> Result<PullRequest, EngineError> {
        self.request(Method::GET, &format!("pulls/{number}"), &[], None)
            .await
    }

    async fn close_pull(&self, number: u64) -> Result<(), EngineError> {
        let _: PullRequest = self
            .request(
                Method::PATCH,
                &format!("pulls/{number}"),
                &[],
                Some(serde_json::json!({ "state": "closed" })),
            )
            .await?;
        Ok(())
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> Result<T, EngineError> {
        let token = fs::read_to_string(&self.token_file)
            .map_err(|error| io_error("k8s.publisher.auth", "read GitHub token", error))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(simple_error(
                "k8s.publisher.auth",
                "GitHub token file is empty",
            ));
        }
        let url = format!(
            "{GITHUB_API_ROOT}/repos/{}/{}/{}",
            self.repository.owner, self.repository.name, endpoint
        );
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .query(query);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| simple_error("k8s.github.api", &error.to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| simple_error("k8s.github.api", &error.to_string()))?;
        if !status.is_success() {
            let message = String::from_utf8_lossy(&bytes);
            return Err(simple_error(
                "k8s.github.api",
                &format!(
                    "GitHub API returned {status}: {}",
                    truncate(&message, 8_192)
                ),
            ));
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| simple_error("k8s.github.api", &error.to_string()))
    }
}

fn parse_deploy_repository(remote: &str) -> Result<DeployRepository, EngineError> {
    let repository = remote
        .strip_prefix("https://github.com/")
        .and_then(|value| value.strip_suffix(".git"))
        .ok_or_else(|| {
            simple_error(
                "k8s.publisher.remote_forbidden",
                "deploy remote must be an HTTPS GitHub .git URL",
            )
        })?;
    let (owner, name) = repository.split_once('/').ok_or_else(|| {
        simple_error(
            "k8s.publisher.remote_forbidden",
            "deploy remote must identify one GitHub owner and repository",
        )
    })?;
    if owner != "henosis-playground" || name.is_empty() || name.contains('/') {
        return Err(simple_error(
            "k8s.publisher.remote_forbidden",
            "deploy remote must belong to github.com/henosis-playground",
        ));
    }
    Ok(DeployRepository {
        owner: owner.into(),
        name: name.into(),
    })
}

fn environment_branch(environment: &str) -> String {
    format!("env/{environment}")
}

fn proposal_branch(environment: &str) -> String {
    format!("henosis/proposals/{environment}")
}

fn proposal_branch_environment(branch: &str) -> Result<String, EngineError> {
    branch
        .strip_prefix("henosis/proposals/")
        .filter(|environment| !environment.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| simple_error("k8s.review.proposal", "invalid proposal branch"))
}

impl GitRepository {
    fn path(&self) -> &Path {
        self.temporary.path()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RendererManifest {
    environment: String,
    components: BTreeMap<String, RendererComponent>,
}

#[derive(Deserialize)]
struct RendererComponent {
    outputs: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GenerationReceipt {
    api_version: &'static str,
    graph_id: String,
    generation: String,
    graph_digest: String,
    component_spec_hashes: Vec<String>,
}

#[derive(Deserialize)]
struct GateReport {
    failures: Vec<GateFailure>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GateFailure {
    consumer: String,
    producer: String,
    #[serde(default)]
    pinned_sha: Option<String>,
    #[serde(default)]
    resolved_sha: Option<String>,
    #[serde(default)]
    outputs_schema_at_pinned: Option<serde_json::Value>,
    #[serde(default)]
    outputs_schema_at_resolved: Option<serde_json::Value>,
    #[serde(default)]
    consumed_paths: Vec<String>,
    kind: String,
    message: String,
    excerpt: String,
}

#[derive(Deserialize)]
struct ValidationIssue {
    code: String,
    message: String,
    component: String,
    #[serde(default)]
    record: Option<RecordLocation>,
    #[serde(default)]
    help: Option<String>,
}

#[derive(Deserialize)]
struct RecordLocation {
    path: String,
}

fn render_recipe_key(
    desired: &DesiredSlice,
    desired_manifest: &str,
    dev_manifest: &str,
    runner: &PreparedRunner,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"henosis.dev/k8s-render-action/v1\0");
    hash_framed(&mut hasher, runner.platform_sha.as_bytes());
    hasher.update(&runner.prepare_recipe_digest);
    hasher.update(&runner.entrypoint_digest);
    hasher.update(&ambient_environment_digest());
    hash_framed(&mut hasher, desired.environment.as_bytes());
    hash_framed(&mut hasher, desired_manifest.as_bytes());
    hash_framed(&mut hasher, dev_manifest.as_bytes());
    hasher.update(b"render\0GITHUB_ACTIONS=true\0");
    for component in desired.components.iter() {
        hasher.update(&component.spec_hash);
    }
    for output in desired.upstream_outputs.iter() {
        hasher.update(&output.component_spec_hash);
        hash_framed(&mut hasher, &output.values_json);
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn hash_framed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn ambient_environment_digest() -> [u8; 32] {
    let mut environment = std::env::vars_os()
        .filter(|(name, _)| name != "GITHUB_ACTIONS")
        .collect::<Vec<_>>();
    environment.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"henosis.dev/k8s-render-environment/v1\0");
    for (name, value) in environment {
        hash_framed(&mut hasher, name.as_os_str().as_bytes());
        hash_framed(&mut hasher, value.as_os_str().as_bytes());
    }
    hasher.update(b"GITHUB_ACTIONS=true\0");
    *hasher.finalize().as_bytes()
}

fn is_commit_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn reset_render_output(output_dir: &Path) -> Result<(), EngineError> {
    if output_dir
        .try_exists()
        .map_err(|error| io_error("k8s.renderer.scratch", "inspect render output", error))?
    {
        fs::remove_dir_all(output_dir)
            .map_err(|error| io_error("k8s.renderer.scratch", "clear render output", error))?;
    }
    fs::create_dir_all(output_dir)
        .map_err(|error| io_error("k8s.renderer.scratch", "recreate render output", error))
}

fn read_render_outputs(
    output_dir: &Path,
    desired: &DesiredSlice,
    expected_environment: &str,
) -> Result<Vec<ComponentOutput>, EngineError> {
    let bytes = fs::read(output_dir.join("manifest.json"))
        .map_err(|error| io_error("k8s.renderer.output", "read renderer manifest", error))?;
    let manifest: RendererManifest = serde_json::from_slice(&bytes)
        .map_err(|error| simple_error("k8s.renderer.output", &error.to_string()))?;
    if manifest.environment != expected_environment {
        return Err(simple_error(
            "k8s.renderer.output",
            "renderer returned a different environment identity",
        ));
    }
    let expected = desired
        .component_names()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let actual = manifest.components.keys().cloned().collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(simple_error(
            "k8s.renderer.output",
            "renderer component set differs from the complete desired slice",
        ));
    }
    manifest
        .components
        .into_iter()
        .map(|(name, rendered)| {
            let component_spec_hash = desired
                .component_named(&name)
                .expect("renderer component set was checked")
                .spec_hash;
            Ok(ComponentOutput {
                component_spec_hash,
                values: rendered.outputs,
            })
        })
        .collect()
}

fn embed_generation_receipt(output_dir: &Path, desired: &DesiredSlice) -> Result<(), EngineError> {
    let path = output_dir.join("manifest.json");
    let bytes = fs::read(&path)
        .map_err(|error| io_error("k8s.publisher.receipt", "read renderer manifest", error))?;
    let mut manifest: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| simple_error("k8s.publisher.receipt", &error.to_string()))?;
    let object = manifest.as_object_mut().ok_or_else(|| {
        simple_error(
            "k8s.publisher.receipt",
            "renderer manifest must be a JSON object",
        )
    })?;
    if object.contains_key("henosis") {
        return Err(simple_error(
            "k8s.publisher.receipt",
            "renderer manifest already defines the reserved henosis field",
        ));
    }
    let receipt = GenerationReceipt {
        api_version: "henosis.dev/generation-receipt/v1",
        graph_id: hex::encode(desired.graph_id),
        generation: desired.generation.to_string(),
        graph_digest: desired.graph_digest(),
        component_spec_hashes: desired
            .components
            .iter()
            .map(|component| hex::encode(component.spec_hash))
            .collect(),
    };
    object.insert(
        "henosis".into(),
        serde_json::to_value(receipt)
            .map_err(|error| simple_error("k8s.publisher.receipt", &error.to_string()))?,
    );
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| simple_error("k8s.publisher.receipt", &error.to_string()))?;
    fs::write(path, bytes)
        .map_err(|error| io_error("k8s.publisher.receipt", "write generation receipt", error))
}

fn validate_generation_receipt_slot(output_dir: &Path) -> Result<(), EngineError> {
    let bytes = fs::read(output_dir.join("manifest.json"))
        .map_err(|error| io_error("k8s.publisher.receipt", "read renderer manifest", error))?;
    let manifest: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| simple_error("k8s.publisher.receipt", &error.to_string()))?;
    let object = manifest.as_object().ok_or_else(|| {
        simple_error(
            "k8s.publisher.receipt",
            "renderer manifest must be a JSON object",
        )
    })?;
    if object.contains_key("henosis") {
        return Err(simple_error(
            "k8s.publisher.receipt",
            "renderer manifest already defines the reserved henosis field",
        ));
    }
    Ok(())
}

fn render_command_error(output: &Output, desired: &DesiredSlice) -> EngineError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    if let Some(json) = stderr.lines().find_map(|line| {
        line.split_once(STRUCTURED_FAILURE_PREFIX)
            .map(|(_, json)| json)
    }) && let Ok(report) = serde_json::from_str::<GateReport>(json)
    {
        let diagnostics = report
            .failures
            .into_iter()
            .map(|failure| failure_diagnostic(failure, desired))
            .collect();
        return EngineError {
            summary: "platform renderer rejected the complete slice".into(),
            diagnostics,
        };
    }
    command_error("k8s.renderer.execute", "render complete slice", output)
}

fn failure_diagnostic(failure: GateFailure, desired: &DesiredSlice) -> Diagnostic {
    let contract_failure = contract_failure_detail(&failure, desired);
    if let Some(issue) = failure
        .excerpt
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<ValidationIssue>(line).ok())
    {
        let mut diagnostic =
            Diagnostic::error(issue.code, issue.message).contract_failure(contract_failure);
        if let Some(pin) = desired.component_named(&issue.component) {
            diagnostic = diagnostic.component(pin.spec_hash);
        }
        if let Some(record) = issue.record {
            diagnostic = diagnostic.pointer(record.path);
        }
        if let Some(help) = issue.help {
            diagnostic = diagnostic.help(help);
        }
        return diagnostic;
    }
    let mut diagnostic =
        Diagnostic::error(format!("k8s.renderer.{}", failure.kind), failure.message)
            .contract_failure(contract_failure);
    if let Some(pin) = desired.component_named(&failure.consumer) {
        diagnostic = diagnostic.component(pin.spec_hash);
    }
    diagnostic
}

fn contract_failure_detail(failure: &GateFailure, desired: &DesiredSlice) -> ContractFailureDetail {
    let kind = match failure.kind.as_str() {
        "compile" => ContractFailureKind::Compile,
        "render" => ContractFailureKind::Render,
        "validate" => ContractFailureKind::Validate,
        "resolve" => ContractFailureKind::Resolve,
        _ => ContractFailureKind::Render,
    };
    let source_url = desired
        .component_named(&failure.consumer)
        .zip(source_location(&failure.excerpt))
        .map(|(pin, line)| {
            format!(
                "https://github.com/{}/blob/{}/henosis/src/index.ts#L{line}",
                pin.context.source.repository, pin.context.source.revision
            )
        })
        .unwrap_or_default();
    ContractFailureDetail {
        consumer: failure.consumer.clone(),
        producer: failure.producer.clone(),
        pinned_sha: failure.pinned_sha.clone().unwrap_or_default(),
        resolved_sha: failure.resolved_sha.clone().unwrap_or_default(),
        outputs_schema_at_pinned_json: failure
            .outputs_schema_at_pinned
            .as_ref()
            .and_then(|schema| serde_json::to_vec(schema).ok())
            .unwrap_or_default(),
        outputs_schema_at_resolved_json: failure
            .outputs_schema_at_resolved
            .as_ref()
            .and_then(|schema| serde_json::to_vec(schema).ok())
            .unwrap_or_default(),
        consumed_paths: failure.consumed_paths.clone(),
        kind,
        excerpt: failure.excerpt.clone(),
        source_url,
    }
}

fn source_location(excerpt: &str) -> Option<u64> {
    let marker = "/henosis/src/index.ts(";
    let (_, tail) = excerpt.split_once(marker)?;
    tail.split_once(',')?.0.parse().ok()
}

async fn git<const N: usize>(
    directory: &Path,
    config: &EngineConfig,
    args: [&str; N],
) -> Result<(), EngineError> {
    let output = git_status(directory, config, args).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error(
            "k8s.publisher.git",
            "run Git command",
            &output,
        ))
    }
}

async fn git_stdout<const N: usize>(
    directory: &Path,
    config: &EngineConfig,
    args: [&str; N],
) -> Result<String, EngineError> {
    let output = git_command(directory, config, args).await?;
    if !output.status.success() {
        return Err(command_error(
            "k8s.publisher.git",
            "run Git command",
            &output,
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| simple_error("k8s.publisher.git", &error.to_string()))
}

async fn git_output<const N: usize>(
    directory: &Path,
    config: &EngineConfig,
    args: [&str; N],
) -> Result<Output, EngineError> {
    let output = git_command(directory, config, args).await?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(command_error(
            "k8s.publisher.git",
            "run Git command",
            &output,
        ))
    }
}

async fn remote_ref_sha(
    directory: &Path,
    config: &EngineConfig,
    reference: &str,
) -> Result<Option<String>, EngineError> {
    let output = git_status(
        directory,
        config,
        ["ls-remote", "--exit-code", "--heads", "origin", reference],
    )
    .await?;
    if output.status.success() {
        let line = String::from_utf8(output.stdout)
            .map_err(|error| simple_error("k8s.publisher.git", &error.to_string()))?;
        Ok(line.split_whitespace().next().map(str::to_owned))
    } else if output.status.code() == Some(2) {
        Ok(None)
    } else {
        Err(command_error(
            "k8s.publisher.git",
            "query remote branch",
            &output,
        ))
    }
}

async fn git_status<const N: usize>(
    directory: &Path,
    config: &EngineConfig,
    args: [&str; N],
) -> Result<Output, EngineError> {
    git_command(directory, config, args).await
}

async fn git_command<const N: usize>(
    directory: &Path,
    config: &EngineConfig,
    args: [&str; N],
) -> Result<Output, EngineError> {
    ensure_askpass(directory)?;
    Command::new("git")
        .args(args)
        .current_dir(directory)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", directory.join(".git/henosis-askpass"))
        .env("HENOSIS_GITHUB_TOKEN_FILE", &config.github_token_file)
        .output()
        .await
        .map_err(|error| io_error("k8s.publisher.git", "start Git", error))
}

fn ensure_askpass(directory: &Path) -> Result<(), EngineError> {
    let git_dir = directory.join(".git");
    if !git_dir.exists() {
        // `git init` must run before an askpass helper can live under `.git`.
        return Ok(());
    }
    let helper = git_dir.join("henosis-askpass");
    if !helper.exists() {
        fs::write(
            &helper,
            "#!/bin/sh\ncase \"$1\" in\n  *Username*) printf '%s\\n' x-access-token ;;\n  *) exec \
             cat \"$HENOSIS_GITHUB_TOKEN_FILE\" ;;\nesac\n",
        )
        .map_err(|error| io_error("k8s.publisher.auth", "write Git askpass helper", error))?;
        let mut permissions = fs::metadata(&helper)
            .map_err(|error| io_error("k8s.publisher.auth", "inspect Git askpass helper", error))?
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&helper, permissions)
            .map_err(|error| io_error("k8s.publisher.auth", "secure Git askpass helper", error))?;
    }
    Ok(())
}

fn clear_worktree(root: &Path) -> Result<(), EngineError> {
    for entry in fs::read_dir(root)
        .map_err(|error| io_error("k8s.publisher.copy", "read Git worktree", error))?
    {
        let entry = entry
            .map_err(|error| io_error("k8s.publisher.copy", "read Git worktree entry", error))?;
        if entry.file_name() == OsStr::new(".git") {
            continue;
        }
        remove_path(&entry.path())?;
    }
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), EngineError> {
    for entry in fs::read_dir(source)
        .map_err(|error| io_error("k8s.publisher.copy", "read rendered world", error))?
    {
        let entry = entry
            .map_err(|error| io_error("k8s.publisher.copy", "read rendered world entry", error))?;
        let destination = target.join(entry.file_name());
        if entry
            .file_type()
            .map_err(|error| io_error("k8s.publisher.copy", "inspect rendered world entry", error))?
            .is_dir()
        {
            fs::create_dir_all(&destination).map_err(|error| {
                io_error("k8s.publisher.copy", "create output directory", error)
            })?;
            copy_tree(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), destination)
                .map_err(|error| io_error("k8s.publisher.copy", "copy rendered file", error))?;
        }
    }
    Ok(())
}

fn remove_path(path: &Path) -> Result<(), EngineError> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .map_err(|error| io_error("k8s.publisher.copy", "clear Git worktree", error))
}

fn truncate(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn simple_error(code: &str, message: &str) -> EngineError {
    EngineError {
        summary: message.into(),
        diagnostics: vec![Diagnostic::error(code, message)],
    }
}

fn io_error(code: &str, action: &str, error: std::io::Error) -> EngineError {
    simple_error(code, &format!("{action}: {error}"))
}

fn command_error(code: &str, action: &str, output: &Output) -> EngineError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let evidence = if stderr.is_empty() { &stdout } else { &stderr };
    let message = if evidence.is_empty() {
        format!("{action} failed with status {}", output.status)
    } else {
        evidence.to_string()
    };
    EngineError {
        summary: format!("{action} failed"),
        diagnostics: vec![Diagnostic::error(code, message)],
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;

    use iddqd::IdOrdMap;

    use super::*;
    use crate::context::API_VERSION;
    use crate::context::ComponentContext;
    use crate::context::EnvironmentContext;
    use crate::context::ImageContext;
    use crate::context::SourceContext;
    use crate::slice::ComponentPin;
    use crate::slice::UpstreamOutput;

    fn recipe_desired() -> DesiredSlice {
        let component = ComponentPin {
            spec_hash: [3; 32],
            name: "service-a".into(),
            context: ComponentContext {
                api_version: API_VERSION.into(),
                environment: EnvironmentContext { id: "dev".into() },
                source: SourceContext {
                    repository: "henosis-playground/service-a".into(),
                    revision: "a".repeat(40),
                },
                image: ImageContext {
                    digest: format!("sha256:{}", "b".repeat(64)),
                },
            },
        };
        DesiredSlice {
            graph_id: [2; 16],
            generation: 4,
            sequence: 9,
            environment: "dev".into(),
            components: IdOrdMap::from_iter_unique([component]).unwrap(),
            upstream_outputs: IdOrdMap::new(),
        }
    }

    fn recipe_key_for(desired: &DesiredSlice, runner: &PreparedRunner) -> String {
        render_recipe_key(
            desired,
            &desired.manifest_toml(&desired.environment).unwrap(),
            &desired.manifest_toml("dev").unwrap(),
            runner,
        )
    }

    fn make_executable(path: &Path) {
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[test]
    fn external_command_diagnostic_preserves_stderr_verbatim() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: b"ignored stdout".to_vec(),
            stderr: b"first line\nsecond line\n".to_vec(),
        };
        let error = command_error("k8s.test", "test command", &output);
        assert_eq!(error.diagnostics()[0].message, "first line\nsecond line\n");
    }

    #[test]
    fn publication_policy_is_strict_and_environment_specific() {
        let policies: PublicationPolicies = serde_json::from_str(
            r#"{
                "default": "direct",
                "environments": {"preview_3jhc7x633z88188fzqhcbbrf84": "pr-gated"}
            }"#,
        )
        .unwrap();
        assert_eq!(
            policies.for_environment("preview_3jhc7x633z88188fzqhcbbrf84"),
            PublicationPolicy::PrGated
        );
        assert_eq!(policies.for_environment("dev"), PublicationPolicy::Direct);
        assert!(
            serde_json::from_str::<PublicationPolicies>(r#"{"default":"direct","environment":{}}"#)
                .is_err()
        );
    }

    #[test]
    fn deploy_remote_requires_exact_playground_owner() {
        assert!(
            parse_deploy_repository("https://github.com/henosis-playground/deploy.git").is_ok()
        );
        assert!(
            parse_deploy_repository("https://github.com/henosis-playground-evil/deploy.git")
                .is_err()
        );
        assert!(parse_deploy_repository("https://github.com/other/deploy.git").is_err());
    }

    #[test]
    fn render_recipe_separates_occurrence_from_every_semantic_input() {
        let desired = recipe_desired();
        let runner = PreparedRunner {
            entrypoint: "/cache/runner".into(),
            platform_sha: "c".repeat(40),
            prepare_recipe_digest: [4; 32],
            entrypoint_digest: [5; 32],
        };
        let baseline = recipe_key_for(&desired, &runner);

        let mut later_occurrence = desired.clone();
        later_occurrence.graph_id = [8; 16];
        later_occurrence.generation = 99;
        later_occurrence.sequence = 100;
        assert_eq!(baseline, recipe_key_for(&later_occurrence, &runner));

        let mut changed_spec = desired.clone();
        let mut component = changed_spec.components.iter().next().unwrap().clone();
        component.spec_hash = [6; 32];
        changed_spec.components = IdOrdMap::from_iter_unique([component]).unwrap();
        assert_ne!(baseline, recipe_key_for(&changed_spec, &runner));

        let mut changed_manifest = desired.clone();
        let mut component = changed_manifest.components.iter().next().unwrap().clone();
        component.context.source.revision = "d".repeat(40);
        changed_manifest.components = IdOrdMap::from_iter_unique([component]).unwrap();
        assert_ne!(baseline, recipe_key_for(&changed_manifest, &runner));

        let mut changed_upstream = desired.clone();
        changed_upstream.upstream_outputs = IdOrdMap::from_iter_unique([UpstreamOutput {
            component_spec_hash: [7; 32],
            values_json: br#"{"url":"https://example.test"}"#.to_vec(),
        }])
        .unwrap();
        assert_ne!(baseline, recipe_key_for(&changed_upstream, &runner));

        let mut changed_runner = runner.clone();
        changed_runner.platform_sha = "e".repeat(40);
        assert_ne!(baseline, recipe_key_for(&desired, &changed_runner));
    }

    #[test]
    fn render_cache_excludes_generation_receipts() {
        let raw = tempfile::tempdir().unwrap();
        fs::write(
            raw.path().join("manifest.json"),
            br#"{"environment":"dev","components":{"service-a":{"outputs":{}}}}"#,
        )
        .unwrap();
        let desired = recipe_desired();
        let outputs = read_render_outputs(raw.path(), &desired, &desired.environment).unwrap();
        validate_generation_receipt_slot(raw.path()).unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let cache = RenderCache::new(cache_root.path().into(), 1);
        let key = "a".repeat(64);
        cache.store(&key, raw.path()).unwrap();

        embed_generation_receipt(raw.path(), &desired).unwrap();
        let restored = tempfile::tempdir().unwrap();
        assert_eq!(
            cache.restore(&key, restored.path()).unwrap(),
            CacheLookup::Hit
        );
        let restored_manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(restored.path().join("manifest.json")).unwrap())
                .unwrap();
        assert!(restored_manifest.get("henosis").is_none());
        assert_eq!(
            read_render_outputs(restored.path(), &desired, &desired.environment).unwrap(),
            outputs
        );
    }

    #[tokio::test]
    async fn identical_recipe_skips_renderer_but_embeds_a_fresh_receipt() {
        let root = tempfile::tempdir().unwrap();
        let runner_root = root.path().join("runner");
        fs::create_dir_all(&runner_root).unwrap();
        let entrypoint = runner_root.join("henosis-runner");
        let counter = root.path().join("render-count");
        fs::write(
            &entrypoint,
            format!(
                "#!/bin/sh\ncount=$(cat '{}' 2>/dev/null || printf 0)\nprintf '%s\\n' \"$((count \
                 + 1))\" > '{}'\nmkdir -p \"$4\"\nprintf '%s\\n' \
                 '{{\"environment\":\"dev\",\"components\":{{\"service-a\":{{\"outputs\":\
                 {{}}}}}}}}' > \"$4/manifest.json\"\n",
                counter.display(),
                counter.display(),
            ),
        )
        .unwrap();
        make_executable(&entrypoint);
        fs::write(
            runner_root.join(".henosis-platform-sha"),
            format!("{}\n", "c".repeat(40)),
        )
        .unwrap();
        let prepare = root.path().join("prepare-runner");
        fs::write(
            &prepare,
            format!("#!/bin/sh\nprintf '%s\\n' '{}'\n", entrypoint.display()),
        )
        .unwrap();
        make_executable(&prepare);
        let engine = Engine::new(EngineConfig {
            prepare_runner: prepare,
            platform_ref: "origin/main".into(),
            platform_checkout: root.path().join("checkout"),
            runner_cache: root.path().join("runner-cache"),
            deploy_remote: "https://github.com/henosis-playground/deploy.git".into(),
            github_token_file: root.path().join("token"),
            scratch_root: root.path().join("scratch"),
            render_cache_max_entries: 4,
            publication_policies: PublicationPolicies::default(),
        })
        .unwrap();
        let desired = recipe_desired();

        let first = engine.render(&desired).await.unwrap();
        assert_eq!(fs::read_to_string(&counter).unwrap().trim(), "1");
        let first_manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(first.output_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(first_manifest["henosis"]["generation"], "4");

        let mut later_occurrence = desired;
        later_occurrence.graph_id = [9; 16];
        later_occurrence.generation = 5;
        later_occurrence.sequence = 10;
        let second = engine.render(&later_occurrence).await.unwrap();
        assert_eq!(fs::read_to_string(&counter).unwrap().trim(), "1");
        let second_manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(second.output_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(second_manifest["henosis"]["generation"], "5");
        assert_eq!(second_manifest["henosis"]["graphId"], hex::encode([9; 16]));
    }

    #[test]
    fn generation_receipt_is_embedded_in_renderer_manifest() {
        let temporary = tempfile::tempdir().unwrap();
        fs::write(
            temporary.path().join("manifest.json"),
            br#"{"environment":"dev","components":{}}"#,
        )
        .unwrap();
        let component = ComponentPin {
            spec_hash: [3; 32],
            name: "service-a".into(),
            context: ComponentContext {
                api_version: API_VERSION.into(),
                environment: EnvironmentContext { id: "dev".into() },
                source: SourceContext {
                    repository: "henosis-playground/service-a".into(),
                    revision: "a".repeat(40),
                },
                image: ImageContext {
                    digest: format!("sha256:{}", "b".repeat(64)),
                },
            },
        };
        let desired = DesiredSlice {
            graph_id: [2; 16],
            generation: 42,
            sequence: 9,
            environment: "dev".into(),
            components: IdOrdMap::from_iter_unique([component]).unwrap(),
            upstream_outputs: IdOrdMap::new(),
        };

        embed_generation_receipt(temporary.path(), &desired).unwrap();
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(temporary.path().join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(
            manifest["henosis"]["apiVersion"],
            "henosis.dev/generation-receipt/v1"
        );
        assert_eq!(manifest["henosis"]["generation"], "42");
        assert_eq!(
            manifest["henosis"]["componentSpecHashes"][0],
            hex::encode([3; 32])
        );
        assert_eq!(manifest["henosis"]["graphDigest"], desired.graph_digest());
        assert!(manifest["henosis"].get("sequence").is_none());
    }

    #[test]
    fn renderer_contract_failure_crosses_the_connector_losslessly() {
        let component = ComponentPin {
            spec_hash: [3; 32],
            name: "service-b".into(),
            context: ComponentContext {
                api_version: API_VERSION.into(),
                environment: EnvironmentContext { id: "dev".into() },
                source: SourceContext {
                    repository: "henosis-playground/service-b".into(),
                    revision: "b".repeat(40),
                },
                image: ImageContext {
                    digest: format!("sha256:{}", "c".repeat(64)),
                },
            },
        };
        let desired = DesiredSlice {
            graph_id: [2; 16],
            generation: 4,
            sequence: 9,
            environment: "dev".into(),
            components: IdOrdMap::from_iter_unique([component]).unwrap(),
            upstream_outputs: IdOrdMap::new(),
        };
        let failure = GateFailure {
            consumer: "service-b".into(),
            producer: "service-a".into(),
            pinned_sha: Some("a".repeat(40)),
            resolved_sha: Some("d".repeat(40)),
            outputs_schema_at_pinned: Some(serde_json::json!({
                "kind": "object",
                "shape": {"api": {"kind": "url"}, "port": {"kind": "number"}}
            })),
            outputs_schema_at_resolved: Some(serde_json::json!({
                "kind": "object",
                "shape": {"port": {"kind": "string"}}
            })),
            consumed_paths: vec!["api".into(), "port".into()],
            kind: "compile".into(),
            message: "service-b consumes incompatible service-a outputs".into(),
            excerpt: "service-b/henosis/src/index.ts(25,32): error TS2339".into(),
        };

        let diagnostic = failure_diagnostic(failure, &desired);
        let detail = diagnostic.contract_failure.as_ref().unwrap();
        assert_eq!(detail.consumer, "service-b");
        assert_eq!(detail.producer, "service-a");
        assert_eq!(detail.consumed_paths, ["api", "port"]);
        assert_eq!(detail.pinned_sha, "a".repeat(40));
        assert_eq!(detail.resolved_sha, "d".repeat(40));
        assert!(!detail.outputs_schema_at_pinned_json.is_empty());
        assert!(!detail.outputs_schema_at_resolved_json.is_empty());
        assert_eq!(
            detail.source_url,
            format!(
                "https://github.com/henosis-playground/service-b/blob/{}/henosis/src/index.ts#L25",
                "b".repeat(40)
            )
        );
    }
}
