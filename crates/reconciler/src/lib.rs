//! Slice-holistic Kubernetes reconciliation, renderer execution, and
//! desired-state publication.

use std::sync::Arc;

use connectrpc::ConnectError;
use connectrpc::ErrorCode;
use connectrpc::RequestContext;
use connectrpc::ServiceRequest;
use connectrpc::ServiceResult;
use henosis_proto::connect::henosis::v1::ConnectorService;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequest;
use henosis_proto::proto::henosis::v1::ReconcileSliceResponse;
use henosis_proto::proto::henosis::v1::RetireSliceRequest;
use henosis_proto::proto::henosis::v1::RetireSliceResponse;

pub mod context;
pub mod engine;
pub mod reconciler;
pub mod review;
pub mod slice;
pub mod telemetry;

use reconciler::ReconcileError;
use reconciler::Reconciler;

/// Registry key served by this connector.
pub const CONNECTOR_NAME: &str = "k8s";

/// Generated-contract handler backed by a durable reconciler.
pub struct ConnectorHandler {
    reconciler: Arc<Reconciler>,
}

impl ConnectorHandler {
    /// Wrap a configured reconciler in the generated `ConnectRPC` handler.
    pub fn new(reconciler: Arc<Reconciler>) -> Self {
        Self { reconciler }
    }
}

impl ConnectorService for ConnectorHandler {
    async fn reconcile_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ReconcileSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<ReconcileSliceResponse> + Send + use<'a>> {
        let generation = self
            .reconciler
            .accept(request.to_owned_message())
            .await
            .map_err(connect_error)?;
        Ok(ReconcileSliceResponse::default()
            .with_accepted_generation(generation)
            .into())
    }

    async fn retire_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, RetireSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<RetireSliceResponse> + Send + use<'a>> {
        let request = request.to_owned_message();
        let slice = request
            .slice
            .as_option()
            .ok_or_else(|| ConnectError::invalid_argument("slice is required"))?;
        if slice.connector.as_deref() != Some(CONNECTOR_NAME) {
            return Err(ConnectError::invalid_argument(format!(
                "slice.connector must be {CONNECTOR_NAME:?}"
            )));
        }
        let graph_id: [u8; 16] = slice
            .graph_id
            .as_deref()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                ConnectError::invalid_argument("slice.graph_id must contain exactly 16 bytes")
            })?;
        let generation = slice
            .generation
            .filter(|generation| *generation > 0)
            .ok_or_else(|| {
                ConnectError::invalid_argument("slice.generation must be greater than zero")
            })?;
        let retired = self
            .reconciler
            .retire(graph_id, generation)
            .await
            .map_err(connect_error)?;
        Ok(RetireSliceResponse::default()
            .with_retired_generation(retired)
            .into())
    }
}

fn connect_error(error: ReconcileError) -> ConnectError {
    let code = match &error {
        ReconcileError::Invalid(_) => ErrorCode::InvalidArgument,
        ReconcileError::Retired | ReconcileError::GenerationConflict(_) => {
            ErrorCode::FailedPrecondition
        }
        ReconcileError::Report(_) => ErrorCode::Unavailable,
        ReconcileError::State(_) | ReconcileError::Engine(_) => ErrorCode::Internal,
    };
    ConnectError::new(code, error.to_string())
}
