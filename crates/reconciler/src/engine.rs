//! Real platform-runner and desired-state Git adapters.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;

use henosis_proto::proto::henosis::v1::ComponentOutputs;
use henosis_proto::proto::henosis::v1::Diagnostic;
use henosis_proto::proto::henosis::v1::DiagnosticSeverity;
use reqwest::Method;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use thiserror::Error;
use tokio::process::Command;

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
    pub outputs: Vec<ComponentOutputs>,
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
        Ok(Self { config, github })
    }

    /// Resolve the connector policy for one validated environment.
    pub fn publication_policy(&self, environment: &str) -> PublicationPolicy {
        self.config
            .publication_policies
            .for_environment(environment)
    }

    /// Provision the real platform runner and render a complete world in
    /// isolation.
    pub async fn render(&self, desired: &DesiredSlice) -> Result<RenderedWorld, EngineError> {
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
        fs::write(
            manifest_dir.join("desired.toml"),
            desired
                .manifest_toml(&desired.environment)
                .map_err(|error| simple_error("k8s.context.invalid", &error.to_string()))?,
        )
        .map_err(|error| io_error("k8s.renderer.manifest", "write desired manifest", error))?;
        fs::write(
            manifest_dir.join("dev.toml"),
            desired
                .manifest_toml("dev")
                .map_err(|error| simple_error("k8s.context.invalid", &error.to_string()))?,
        )
        .map_err(|error| io_error("k8s.renderer.manifest", "write dev manifest", error))?;

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
        let entrypoint = entrypoint.trim();
        if entrypoint.is_empty() {
            return Err(simple_error(
                "k8s.runner.prepare",
                "prepare-runner returned an empty entrypoint",
            ));
        }
        let rendered = Command::new(entrypoint)
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
        let outputs = read_render_outputs(&output_dir, desired)?;
        Ok(RenderedWorld {
            _temporary: temporary,
            output_dir,
            outputs,
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

        let component_names = desired.components.keys().cloned().collect::<Vec<_>>();
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
            let environment = proposal_branch_environment(&proposal.proposal_branch)?;
            let repository = self.git_repository(&environment).await?;
            if repository.expected_sha.is_empty() {
                return Err(simple_error(
                    "k8s.review.merge",
                    "merged proposal target branch is missing",
                ));
            }
            return Ok(ProposalStatus::Merged(repository.expected_sha));
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

#[derive(Deserialize)]
struct GateReport {
    failures: Vec<GateFailure>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GateFailure {
    consumer: String,
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

fn read_render_outputs(
    output_dir: &Path,
    desired: &DesiredSlice,
) -> Result<Vec<ComponentOutputs>, EngineError> {
    let bytes = fs::read(output_dir.join("manifest.json"))
        .map_err(|error| io_error("k8s.renderer.output", "read renderer manifest", error))?;
    let manifest: RendererManifest = serde_json::from_slice(&bytes)
        .map_err(|error| simple_error("k8s.renderer.output", &error.to_string()))?;
    if manifest.environment != desired.environment {
        return Err(simple_error(
            "k8s.renderer.output",
            "renderer returned a different environment identity",
        ));
    }
    let expected = desired.components.keys().cloned().collect::<BTreeSet<_>>();
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
            let id = desired.components[&name].id.to_vec();
            let values = serde_json::to_vec(&rendered.outputs)
                .map_err(|error| simple_error("k8s.renderer.output", &error.to_string()))?;
            Ok(ComponentOutputs::default()
                .with_component_id(id)
                .with_values_json(values))
        })
        .collect()
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
    if let Some(issue) = failure
        .excerpt
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<ValidationIssue>(line).ok())
    {
        let mut diagnostic = Diagnostic::default()
            .with_code(issue.code)
            .with_message(issue.message)
            .with_severity(DiagnosticSeverity::Error);
        if let Some(pin) = desired.components.get(&issue.component) {
            diagnostic = diagnostic.with_component_id(pin.id.to_vec());
        }
        if let Some(record) = issue.record {
            diagnostic = diagnostic.with_pointer(record.path);
        }
        if let Some(help) = issue.help {
            diagnostic = diagnostic.with_help(help);
        }
        return diagnostic;
    }
    let mut diagnostic = Diagnostic::default()
        .with_code(format!("k8s.renderer.{}", failure.kind))
        .with_message(failure.message)
        .with_help(failure.excerpt)
        .with_severity(DiagnosticSeverity::Error);
    if let Some(pin) = desired.components.get(&failure.consumer) {
        diagnostic = diagnostic.with_component_id(pin.id.to_vec());
    }
    diagnostic
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
        diagnostics: vec![
            Diagnostic::default()
                .with_code(code)
                .with_message(message)
                .with_severity(DiagnosticSeverity::Error),
        ],
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
        diagnostics: vec![
            Diagnostic::default()
                .with_code(code)
                .with_message(message)
                .with_severity(DiagnosticSeverity::Error),
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;

    use super::*;

    #[test]
    fn external_command_diagnostic_preserves_stderr_verbatim() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: b"ignored stdout".to_vec(),
            stderr: b"first line\nsecond line\n".to_vec(),
        };
        let error = command_error("k8s.test", "test command", &output);
        assert_eq!(
            error.diagnostics()[0].message.as_deref(),
            Some("first line\nsecond line\n")
        );
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
}
