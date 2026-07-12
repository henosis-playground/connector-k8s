//! Service process for the Henosis Kubernetes reconciler.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use connectrpc::Router;
use connectrpc::Server;
use henosis_k8s_reconciler::ConnectorHandler;
use henosis_k8s_reconciler::engine::Engine;
use henosis_k8s_reconciler::engine::EngineConfig;
use henosis_k8s_reconciler::engine::PublicationPolicies;
use henosis_k8s_reconciler::reconciler::CoreReporter;
use henosis_k8s_reconciler::reconciler::Reconciler;
use henosis_k8s_reconciler::reconciler::ReconcilerConfig;
use http::Uri;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("henosis=info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .try_init()?;

    let state_dir = path_env("HENOSIS_STATE_DIR", "/var/lib/henosis-connector-k8s/state");
    let engine = Engine::new(EngineConfig {
        prepare_runner: path_env(
            "HENOSIS_PREPARE_RUNNER",
            "/opt/henosis/platform/scripts/prepare-runner.sh",
        ),
        platform_ref: string_env("HENOSIS_PLATFORM_REF", "origin/main"),
        platform_checkout: path_env(
            "HENOSIS_PLATFORM_CHECKOUT",
            "/var/lib/henosis-connector-k8s/platform",
        ),
        runner_cache: path_env(
            "HENOSIS_RUNNER_CACHE_DIR",
            "/var/lib/henosis-connector-k8s/runner-cache",
        ),
        deploy_remote: string_env(
            "HENOSIS_DEPLOY_REMOTE",
            "https://github.com/henosis-playground/deploy.git",
        ),
        github_token_file: path_env("HENOSIS_GITHUB_TOKEN_FILE", "/run/secrets/github-pat"),
        scratch_root: state_dir.join("scratch"),
        publication_policies: publication_policies()?,
    })?;
    let core_uri = string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?;
    let core_token = env::var("HENOSIS_CORE_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let reporter = Arc::new(CoreReporter::new(core_uri, core_token));
    let reconciler = Arc::new(Reconciler::new(
        ReconcilerConfig { state_dir },
        engine,
        reporter,
    )?);
    reconciler.resume().await?;

    let handler = Arc::new(ConnectorHandler::new(reconciler));
    let router = Router::new().add_service(handler);
    let bind = string_env("HENOSIS_BIND", "0.0.0.0:8081");
    let server = Server::bind(bind).await?;
    server
        .serve_with_graceful_shutdown(router, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}

fn string_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn path_env(name: &str, default: &str) -> PathBuf {
    PathBuf::from(string_env(name, default))
}

fn publication_policies() -> Result<PublicationPolicies, serde_json::Error> {
    serde_json::from_str(&string_env(
        "HENOSIS_PUBLICATION_POLICIES",
        r#"{"default":"direct","environments":{}}"#,
    ))
}
