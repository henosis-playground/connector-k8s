//! Service process for the Henosis Kubernetes reconciler.

use std::env;
use std::path::PathBuf;

use connector_sdk::RuntimeConfig;
use connector_sdk::ServeConfig;
use henosis_k8s_reconciler::ConnectorConfig;
use henosis_k8s_reconciler::KubernetesConnector;
use henosis_k8s_reconciler::engine::Engine;
use henosis_k8s_reconciler::engine::EngineConfig;
use henosis_k8s_reconciler::engine::PublicationPolicies;
use http::Uri;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state_dir = path_env(
        "HENOSIS_STATE_DIR",
        "/var/lib/henosis-connector-k8s/state-sdk-v1",
    );
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
        render_cache_max_entries: usize_env("HENOSIS_RENDER_CACHE_MAX_ENTRIES", 64)?,
        publication_policies: publication_policies()?,
    })?;
    let connector = KubernetesConnector::new(
        ConnectorConfig {
            target_state_dir: state_dir.join("target-v1"),
        },
        engine,
    )?;
    let core_token = env::var("HENOSIS_CORE_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    connector_sdk::serve(
        ServeConfig {
            bind: string_env("HENOSIS_BIND", "0.0.0.0:8081"),
            core_uri: string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?,
            core_token,
            runtime: RuntimeConfig::new(state_dir),
            telemetry_filter: "henosis=info,connector_sdk=info".into(),
        },
        connector,
    )
    .await?;
    Ok(())
}

fn string_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn path_env(name: &str, default: &str) -> PathBuf {
    PathBuf::from(string_env(name, default))
}

fn usize_env(name: &str, default: usize) -> Result<usize, std::num::ParseIntError> {
    env::var(name)
        .map(|value| value.parse())
        .unwrap_or_else(|_| Ok(default))
}

fn publication_policies() -> Result<PublicationPolicies, serde_json::Error> {
    serde_json::from_str(&string_env(
        "HENOSIS_PUBLICATION_POLICIES",
        r#"{"default":"direct","environments":{}}"#,
    ))
}
