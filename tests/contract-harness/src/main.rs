//! Contract-faithful callback server and live slice driver.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use buffa::MessageField;
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
use henosis_proto::connect::henosis::v1::ConnectorCallbackService;
use henosis_proto::connect::henosis::v1::ConnectorServiceClient;
use henosis_proto::proto::henosis::v1::Component;
use henosis_proto::proto::henosis::v1::ComponentDispositionKind;
use henosis_proto::proto::henosis::v1::ComponentRevision;
use henosis_proto::proto::henosis::v1::FetchSliceRequest;
use henosis_proto::proto::henosis::v1::FetchSliceResponse;
use henosis_proto::proto::henosis::v1::GraphSlice;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
use henosis_proto::proto::henosis::v1::ReportSliceRequest;
use henosis_proto::proto::henosis::v1::ReportSliceResponse;
use http::Uri;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

struct Callback {
    reports: mpsc::UnboundedSender<ReportSliceRequest>,
}

impl ConnectorCallbackService for Callback {
    async fn report_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ReportSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<ReportSliceResponse> + Send + use<'a>> {
        let request = request.to_owned_message();
        let publishable = !request.report.outputs.is_empty();
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
        _request: ServiceRequest<'_, FetchSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<FetchSliceResponse> + Send + use<'a>> {
        Err::<connectrpc::Response<FetchSliceResponse>, ConnectError>(ConnectError::new(
            ErrorCode::Unimplemented,
            "the live-proof harness does not exercise recovery fetch",
        ))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Evidence {
    accepted_generation: u64,
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

    let (report_tx, mut report_rx) = mpsc::unbounded_channel();
    let callback = Arc::new(Callback { reports: report_tx });
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
    let request = live_request(&environment)?;
    let accepted_generation = loop {
        match client.reconcile_slice(request.clone()).await {
            Ok(response) => {
                break response
                    .view()
                    .accepted_generation
                    .ok_or("connector omitted accepted_generation")?;
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
            accepted_generation,
            environment,
            pending_report,
            report: final_report,
        })?,
    )?;
    let _ = shutdown_tx.send(());
    server_task.await??;
    Ok(())
}

fn live_request(environment: &str) -> Result<ReconcileSliceRequest, serde_json::Error> {
    let graph_byte = env::var("HENOSIS_GRAPH_BYTE")
        .ok()
        .and_then(|value| u8::from_str_radix(&value, 16).ok())
        .unwrap_or(0x72);
    let components = vec![
        component(
            [0x11; 16],
            "service-a",
            "henosis-playground/service-a",
            "ca73c9ae5b6579ad0b6b77b80fb77b54fc5fd595",
            "sha256:b808fd4ef39b8f18309b6e266f7ab84d466ee8713c20f832248ae35cc5b64586",
            environment,
        )?,
        component(
            [0x22; 16],
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
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn component(
    id: [u8; 16],
    name: &str,
    repository: &str,
    revision: &str,
    digest: &str,
    environment: &str,
) -> Result<Component, serde_json::Error> {
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
    };
    Ok(Component {
        id: Some(id.to_vec()),
        name: Some(name.into()),
        revision: MessageField::some(ComponentRevision {
            source: Some(repository.into()),
            revision: Some(revision.into()),
            ..Default::default()
        }),
        connector: Some(CONNECTOR_NAME.into()),
        context: Some(serde_json::to_vec(&context)?),
        ..Default::default()
    })
}
