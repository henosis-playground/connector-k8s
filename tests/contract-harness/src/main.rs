//! Contract-faithful callback server and live slice driver.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use buffa::Message as _;
use buffa::MessageField;
use connector_sdk::henosis_proto::connect::henosis::v1::ConnectorCallbackService;
use connector_sdk::henosis_proto::connect::henosis::v1::ConnectorServiceClient;
use connector_sdk::henosis_proto::proto::henosis::v1::ComponentDispositionKind;
use connector_sdk::henosis_proto::proto::henosis::v1::ComponentSpec;
use connector_sdk::henosis_proto::proto::henosis::v1::FetchSliceRequest;
use connector_sdk::henosis_proto::proto::henosis::v1::FetchSliceResponse;
use connector_sdk::henosis_proto::proto::henosis::v1::GraphSlice;
use connector_sdk::henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
use connector_sdk::henosis_proto::proto::henosis::v1::RegisteredComponentSpec;
use connector_sdk::henosis_proto::proto::henosis::v1::ReportSliceRequest;
use connector_sdk::henosis_proto::proto::henosis::v1::ReportSliceResponse;
use connector_sdk::henosis_proto::proto::henosis::v1::RetireSliceRequest;
use connectrpc::ConnectError;
use connectrpc::ErrorCode;
use connectrpc::RequestContext;
use connectrpc::Router;
use connectrpc::Server;
use connectrpc::ServiceRequest;
use connectrpc::ServiceResult;
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use henosis_k8s_reconciler::CONNECTOR_NAME;
use henosis_k8s_reconciler::context::API_VERSION;
use henosis_k8s_reconciler::context::ComponentContext;
use henosis_k8s_reconciler::context::EnvironmentContext;
use henosis_k8s_reconciler::context::ImageContext;
use henosis_k8s_reconciler::context::SourceContext;
use http::Uri;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

struct Callback {
    reports: mpsc::UnboundedSender<ReportSliceRequest>,
    slice: GraphSlice,
}

impl ConnectorCallbackService for Callback {
    async fn report_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ReportSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<ReportSliceResponse> + Send + use<'a>> {
        let request = request.to_owned_message();
        let publishable = !request.report.outputs.is_empty();
        if publishable != request.publication_id.is_some() {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "publication_id presence does not match report outputs",
            ));
        }
        self.reports.send(request).map_err(|_| {
            ConnectError::new(ErrorCode::Internal, "live-proof report receiver closed")
        })?;
        let response = if publishable {
            ReportSliceResponse::default().with_publication_sequence(1)
        } else {
            ReportSliceResponse::default()
        };
        Ok(response.into())
    }

    async fn fetch_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, FetchSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<FetchSliceResponse> + Send + use<'a>> {
        if request.graph_id != self.slice.graph_id.as_deref()
            || request.connector != Some(CONNECTOR_NAME)
            || request.sequence != self.slice.sequence
        {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                "the requested exact slice is not retained",
            ));
        }
        Ok(FetchSliceResponse {
            slice: MessageField::some(self.slice.clone()),
            ..Default::default()
        }
        .into())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Evidence {
    accepted_sequence: u64,
    environment: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_report: Option<ReportSliceRequest>,
    report: ReportSliceRequest,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let callback_bind = env::var("HENOSIS_CALLBACK_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let connector_uri = env::var("HENOSIS_CONNECTOR_URL")
        .unwrap_or_else(|_| "http://connector-k8s:8081".into())
        .parse::<Uri>()?;
    let evidence_path = PathBuf::from(
        env::var("HENOSIS_EVIDENCE_PATH").unwrap_or_else(|_| "/evidence/report.json".into()),
    );
    let environment = env::var("HENOSIS_ENVIRONMENT")
        .unwrap_or_else(|_| "preview_3jhc7x633z88188fzqhcbbrf84".into());
    let expect_cycle = env::var("HENOSIS_EXPECT_REPORT").as_deref() == Ok("review-cycle");

    if env::var("HENOSIS_ACTION").as_deref() == Ok("retire") {
        let client =
            ConnectorServiceClient::new(HttpClient::plaintext(), ClientConfig::new(connector_uri));
        let retired_generation = tokio::time::timeout(Duration::from_secs(1_200), async {
            loop {
                match client.retire_slice(retire_request()).await {
                    Ok(response) => {
                        break response
                            .view()
                            .retired_generation
                            .ok_or("connector omitted retired_generation");
                    }
                    Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
        })
        .await??;
        if let Some(parent) = evidence_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            evidence_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "environment": environment,
                "retiredGeneration": retired_generation,
            }))?,
        )?;
        return Ok(());
    }

    let request = live_request(&environment)?;
    let slice = request
        .slice
        .as_option()
        .ok_or("live request omitted its slice")?
        .clone();
    let (report_tx, mut report_rx) = mpsc::unbounded_channel();
    let callback = Arc::new(Callback {
        reports: report_tx,
        slice,
    });
    let router = Router::new().add_service(callback);
    let server = Server::bind(callback_bind).await?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .serve_with_graceful_shutdown(router, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client =
        ConnectorServiceClient::new(HttpClient::plaintext(), ClientConfig::new(connector_uri));
    let accepted_sequence = loop {
        match client.reconcile_slice(request.clone()).await {
            Ok(response) => {
                break response
                    .view()
                    .accepted_sequence
                    .ok_or("connector omitted accepted_sequence")?;
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    };

    let (pending_report, final_report) = tokio::time::timeout(Duration::from_secs(1_200), async {
        let mut pending = None;
        while let Some(request) = report_rx.recv().await {
            let report = request.report.as_option()?;
            let awaiting_review = report.dispositions.iter().all(|disposition| {
                disposition.kind.as_ref().is_some_and(|kind| {
                    kind.to_i32() == ComponentDispositionKind::Reconciling as i32
                })
            }) && report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.as_deref() == Some("k8s.awaiting-review"));
            if awaiting_review {
                pending = Some(request.clone());
                if !expect_cycle {
                    return Some((pending, request));
                }
                continue;
            }
            let ready = report.dispositions.iter().all(|disposition| {
                disposition
                    .kind
                    .as_ref()
                    .is_some_and(|kind| kind.to_i32() == ComponentDispositionKind::Ready as i32)
            });
            if ready && report.outputs.len() == 2 && report.diagnostics.is_empty() {
                return Some((pending, request));
            }
        }
        None
    })
    .await?
    .ok_or("connector callback stream ended before a complete READY report")?;

    if let Some(parent) = evidence_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        evidence_path,
        serde_json::to_vec_pretty(&Evidence {
            accepted_sequence,
            environment,
            pending_report,
            report: final_report,
        })?,
    )?;
    let _ = shutdown_tx.send(());
    server_task.await??;
    Ok(())
}

fn retire_request() -> RetireSliceRequest {
    let graph_byte = env::var("HENOSIS_GRAPH_BYTE")
        .ok()
        .and_then(|value| u8::from_str_radix(&value, 16).ok())
        .unwrap_or(0x72);
    RetireSliceRequest {
        slice: MessageField::some(GraphSlice {
            graph_id: Some(vec![graph_byte; 16]),
            generation: Some(
                env::var("HENOSIS_GENERATION")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(1),
            ),
            connector: Some(CONNECTOR_NAME.into()),
            sequence: Some(
                env::var("HENOSIS_SEQUENCE")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn live_request(environment: &str) -> Result<ReconcileSliceRequest, serde_json::Error> {
    let graph_byte = env::var("HENOSIS_GRAPH_BYTE")
        .ok()
        .and_then(|value| u8::from_str_radix(&value, 16).ok())
        .unwrap_or(0x72);
    let components = vec![
        component(
            "service-a",
            "henosis-playground/service-a",
            "ca73c9ae5b6579ad0b6b77b80fb77b54fc5fd595",
            "sha256:b808fd4ef39b8f18309b6e266f7ab84d466ee8713c20f832248ae35cc5b64586",
            environment,
        )?,
        component(
            "service-b",
            "henosis-playground/service-b",
            "4ab590bd33410df836baa7fe3a08d3999b2d2a8a",
            "sha256:f0744d67c15d0c74d6c79444a394455b458a59c4b25ea4e037ad4cdf22f377d1",
            environment,
        )?,
    ];
    Ok(ReconcileSliceRequest {
        slice: MessageField::some(GraphSlice {
            graph_id: Some(vec![graph_byte; 16]),
            generation: Some(
                env::var("HENOSIS_GENERATION")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(1),
            ),
            connector: Some(CONNECTOR_NAME.into()),
            components,
            sequence: Some(
                env::var("HENOSIS_SEQUENCE")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
            ),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn component(
    name: &str,
    repository: &str,
    revision: &str,
    digest: &str,
    environment: &str,
) -> Result<RegisteredComponentSpec, serde_json::Error> {
    let context = ComponentContext {
        api_version: API_VERSION.into(),
        environment: EnvironmentContext {
            id: environment.into(),
        },
        source: SourceContext {
            repository: repository.into(),
            revision: revision.into(),
        },
        image: ImageContext {
            digest: digest.into(),
        },
        borrow: None,
    };
    let spec = ComponentSpec {
        name: Some(name.into()),
        connector: Some(CONNECTOR_NAME.into()),
        connector_context: Some(serde_json::to_vec(&context)?),
        ..Default::default()
    };
    let hash = blake3::hash(&spec.encode_to_vec()).as_bytes().to_vec();
    Ok(RegisteredComponentSpec {
        hash: Some(hash),
        spec: MessageField::some(spec),
        ..Default::default()
    })
}
