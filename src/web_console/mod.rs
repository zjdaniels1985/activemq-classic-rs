//! Built-in administrative web console (the Rust counterpart of the ActiveMQ
//! Classic `/admin` console) plus a JSON REST API under `/api`.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::core::security::constant_time_eq;
use crate::core::{Body, BrokerCore, CoreError, Destination, Message, Value};

#[derive(Clone)]
struct ConsoleState {
    core: Arc<BrokerCore>,
    admin_token: Option<Arc<str>>,
}

/// Serves the console until the listener fails.
pub async fn serve(
    core: Arc<BrokerCore>,
    listener: TcpListener,
    admin_token: Option<String>,
) -> io::Result<()> {
    tracing::info!(address = %listener.local_addr()?, "web console ready");
    let state = ConsoleState {
        core,
        admin_token: admin_token
            .filter(|token| !token.trim().is_empty())
            .map(Arc::from),
    };
    axum::serve(listener, router(state))
        .await
        .map_err(io::Error::other)
}

fn router(state: ConsoleState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/admin", get(index))
        .route("/api/broker", get(broker_info))
        .route(
            "/api/destinations",
            get(destinations).post(create_destination),
        )
        .route(
            "/api/destinations/{kind}/{name}",
            delete(delete_destination),
        )
        .route("/api/destinations/{kind}/{name}/purge", post(purge))
        .route("/api/destinations/{kind}/{name}/send", post(send))
        .route("/api/destinations/{kind}/{name}/messages", get(browse))
        .route(
            "/api/destinations/{kind}/{name}/messages/{id}",
            get(get_message).delete(remove_message),
        )
        .route(
            "/api/destinations/{kind}/{name}/messages/{id}/move",
            post(move_message),
        )
        .route(
            "/api/destinations/{kind}/{name}/messages/{id}/retry",
            post(retry_message),
        )
        .route("/api/connections", get(connections))
        .route("/api/connections/{id}", delete(close_connection))
        .route("/api/consumers", get(consumers))
        .route("/api/subscriptions", get(subscriptions))
        .route(
            "/api/subscriptions/{client_id}/{name}",
            delete(delete_subscription),
        )
        .route("/api/jobs", get(jobs))
        .route("/api/jobs/{id}", delete(remove_job))
        .with_state(state)
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl From<CoreError> for ApiError {
    fn from(error: CoreError) -> Self {
        let status = match error {
            CoreError::NotFound(_) => StatusCode::NOT_FOUND,
            CoreError::Security(_) => StatusCode::FORBIDDEN,
            CoreError::InvalidDestination(_) | CoreError::InvalidSelector(_) => {
                StatusCode::BAD_REQUEST
            }
            CoreError::IllegalState(_) | CoreError::Transaction(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, error.message().to_owned())
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

fn authorize(state: &ConsoleState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(token) = &state.admin_token else {
        return Ok(());
    };
    let valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), token.as_bytes()));
    if valid {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "unauthorized".to_owned(),
        ))
    }
}

fn destination(kind: &str, name: &str) -> Result<Destination, ApiError> {
    Destination::parse_uri(&format!("{kind}://{name}")).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("invalid destination {kind}://{name}"),
        )
    })
}

fn not_found(what: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
}

#[derive(Serialize)]
struct Count {
    count: usize,
}

#[derive(Serialize)]
struct Done {
    ok: bool,
}

async fn index() -> Html<&'static str> {
    Html(include_str!("console.html"))
}

async fn broker_info(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<crate::core::BrokerInfo> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.info()))
}

async fn destinations(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<Vec<crate::core::DestinationStats>> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.destinations()))
}

#[derive(Deserialize)]
struct CreateDestination {
    /// e.g. `queue://orders` or `topic://prices`
    uri: String,
}

async fn create_destination(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Json(request): Json<CreateDestination>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    let destination = Destination::parse_uri(&request.uri).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "uri must look like queue://NAME or topic://NAME".to_owned(),
        )
    })?;
    state.core.create_destination(&destination)?;
    Ok(Json(Done { ok: true }))
}

async fn delete_destination(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name)): Path<(String, String)>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    state.core.delete_destination(&destination(&kind, &name)?)?;
    Ok(Json(Done { ok: true }))
}

async fn purge(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name)): Path<(String, String)>,
) -> ApiResult<Count> {
    authorize(&state, &headers)?;
    let count = state.core.purge(&destination(&kind, &name)?)?;
    Ok(Json(Count { count }))
}

#[derive(Deserialize)]
struct BrowseQuery {
    limit: Option<usize>,
}

async fn browse(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name)): Path<(String, String)>,
    Query(query): Query<BrowseQuery>,
) -> ApiResult<Vec<crate::core::MessageView>> {
    authorize(&state, &headers)?;
    let limit = query.limit.unwrap_or(400).min(10_000);
    Ok(Json(state.core.browse(&destination(&kind, &name)?, limit)?))
}

async fn get_message(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name, id)): Path<(String, String, String)>,
) -> ApiResult<crate::core::MessageView> {
    authorize(&state, &headers)?;
    let message = state
        .core
        .get_message(&destination(&kind, &name)?, &id)
        .ok_or_else(|| not_found("message"))?;
    Ok(Json(crate::core::MessageView::from_message(&message)))
}

async fn remove_message(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name, id)): Path<(String, String, String)>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    if state
        .core
        .remove_message(&destination(&kind, &name)?, &id)?
    {
        Ok(Json(Done { ok: true }))
    } else {
        Err(not_found("message"))
    }
}

#[derive(Deserialize)]
struct MoveRequest {
    to: String,
}

async fn move_message(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name, id)): Path<(String, String, String)>,
    Json(request): Json<MoveRequest>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    let to = Destination::parse_uri(&request.to)
        .or_else(|| Some(Destination::queue(request.to.clone())))
        .filter(|destination| !destination.name.is_empty())
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "invalid target".to_owned()))?;
    if state
        .core
        .move_message(&destination(&kind, &name)?, &id, &to)?
    {
        Ok(Json(Done { ok: true }))
    } else {
        Err(not_found("message"))
    }
}

async fn retry_message(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name, id)): Path<(String, String, String)>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    if state.core.retry_message(&destination(&kind, &name)?, &id)? {
        Ok(Json(Done { ok: true }))
    } else {
        Err(not_found("message"))
    }
}

#[derive(Deserialize)]
struct SendRequest {
    body: String,
    #[serde(default)]
    properties: BTreeMap<String, serde_json::Value>,
    #[serde(default = "default_true")]
    persistent: bool,
    priority: Option<u8>,
    correlation_id: Option<String>,
    message_type: Option<String>,
    reply_to: Option<String>,
    /// Time to live in milliseconds.
    time_to_live: Option<i64>,
    /// Scheduler delay in milliseconds (AMQ_SCHEDULED_DELAY).
    delay: Option<i64>,
    /// Number of copies to send.
    count: Option<usize>,
}

fn default_true() -> bool {
    true
}

fn json_to_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(number) => match number.as_i64() {
            Some(value) => Value::Long(value),
            None => Value::Double(number.as_f64().unwrap_or_default()),
        },
        serde_json::Value::String(value) => Value::String(value),
        other => Value::String(other.to_string()),
    }
}

async fn send(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((kind, name)): Path<(String, String)>,
    Json(request): Json<SendRequest>,
) -> ApiResult<Count> {
    authorize(&state, &headers)?;
    let target = destination(&kind, &name)?;
    let count = request.count.unwrap_or(1).clamp(1, 10_000);
    for _ in 0..count {
        let mut message = Message::new(target.clone(), Body::Text(request.body.clone()));
        message.persistent = request.persistent;
        message.priority = request.priority.unwrap_or(4).min(9);
        message.correlation_id = request.correlation_id.clone();
        message.message_type = request.message_type.clone();
        message.reply_to = request.reply_to.as_deref().and_then(Destination::parse_uri);
        if let Some(ttl) = request.time_to_live.filter(|ttl| *ttl > 0) {
            message.expiration = message.timestamp + ttl;
        }
        for (key, value) in &request.properties {
            message
                .properties
                .insert(key.clone(), json_to_value(value.clone()));
        }
        if let Some(delay) = request.delay.filter(|delay| *delay > 0) {
            message
                .properties
                .insert("AMQ_SCHEDULED_DELAY".to_owned(), Value::Long(delay));
        }
        message.user_id = Some("web-console".to_owned());
        state.core.admin_send(message)?;
    }
    Ok(Json(Count { count }))
}

async fn connections(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<Vec<crate::core::ConnectionStats>> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.connections()))
}

async fn close_connection(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    if state.core.close_connection(id) {
        Ok(Json(Done { ok: true }))
    } else {
        Err(not_found("connection"))
    }
}

async fn consumers(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<Vec<crate::core::ConsumerStats>> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.consumers()))
}

async fn subscriptions(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<Vec<crate::core::SubscriptionStats>> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.durable_subscriptions()))
}

async fn delete_subscription(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path((client_id, name)): Path<(String, String)>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    state.core.delete_durable(&client_id, &name)?;
    Ok(Json(Done { ok: true }))
}

async fn jobs(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> ApiResult<Vec<crate::core::JobStats>> {
    authorize(&state, &headers)?;
    Ok(Json(state.core.jobs()))
}

async fn remove_job(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Done> {
    authorize(&state, &headers)?;
    if state.core.remove_job(&id) {
        Ok(Json(Done { ok: true }))
    } else {
        Err(not_found("job"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body as HttpBody;
    use axum::http::Request;
    use tower::ServiceExt;

    fn app(token: Option<&str>) -> (Arc<BrokerCore>, Router) {
        let core = BrokerCore::in_memory();
        let state = ConsoleState {
            core: Arc::clone(&core),
            admin_token: token.map(Arc::from),
        };
        (core, router(state))
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/json");
        }
        let response = app
            .clone()
            .oneshot(
                request
                    .body(HttpBody::from(body.unwrap_or_default().to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    #[tokio::test]
    async fn token_is_required_when_configured() {
        let (_core, app) = app(Some("secret"));
        let (status, _) = call(&app, "GET", "/api/broker", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let response = app
            .clone()
            .oneshot(
                Request::get("/api/broker")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(HttpBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn manages_destinations_and_messages() {
        let (_core, app) = app(None);
        let (status, _) = call(
            &app,
            "POST",
            "/api/destinations",
            Some(r#"{"uri":"queue://orders"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, sent) = call(
            &app,
            "POST",
            "/api/destinations/queue/orders/send",
            Some(r#"{"body":"hello","count":2,"properties":{"a":1}}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(sent["count"], 2);
        let (_, messages) =
            call(&app, "GET", "/api/destinations/queue/orders/messages", None).await;
        let messages = messages.as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["body"], "hello");
        let id = messages[0]["message_id"].as_str().unwrap().to_owned();
        let (status, _) = call(
            &app,
            "POST",
            &format!("/api/destinations/queue/orders/messages/{id}/move"),
            Some(r#"{"to":"queue://archive"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (_, destinations) = call(&app, "GET", "/api/destinations", None).await;
        let pending = |uri: &str| {
            destinations
                .as_array()
                .unwrap()
                .iter()
                .find(|d| d["uri"] == uri)
                .map(|d| d["pending"].as_u64().unwrap())
        };
        assert_eq!(pending("queue://orders"), Some(1));
        assert_eq!(pending("queue://archive"), Some(1));
        let (_, purged) = call(&app, "POST", "/api/destinations/queue/orders/purge", None).await;
        assert_eq!(purged["count"], 1);
        let (status, _) = call(&app, "DELETE", "/api/destinations/queue/archive", None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(
            &app,
            "GET",
            "/api/destinations/queue/archive/messages",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn schedules_and_removes_jobs() {
        let (_core, app) = app(None);
        call(
            &app,
            "POST",
            "/api/destinations/queue/later/send",
            Some(r#"{"body":"x","delay":60000}"#),
        )
        .await;
        let (_, jobs) = call(&app, "GET", "/api/jobs", None).await;
        let id = jobs[0]["id"].as_str().unwrap().to_owned();
        let (status, _) = call(&app, "DELETE", &format!("/api/jobs/{id}"), None).await;
        assert_eq!(status, StatusCode::OK);
        let (_, jobs) = call(&app, "GET", "/api/jobs", None).await;
        assert!(jobs.as_array().unwrap().is_empty());
    }

    #[test]
    fn index_page_is_embedded() {
        assert!(include_str!("console.html").contains("\"/destinations\""));
    }
}
