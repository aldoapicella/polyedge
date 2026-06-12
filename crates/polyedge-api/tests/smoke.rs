use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use polyedge_api::{app, smoke_paths};
use polyedge_config::RuntimeSettings;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn smoke_paths_return_success() {
    let app = app(RuntimeSettings::default());
    for path in smoke_paths() {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
}

#[tokio::test]
async fn api_contract_routes_remain_reachable() {
    let app = app(RuntimeSettings::default());
    let cases = [
        (Method::GET, "/api/v1/health", None, StatusCode::OK),
        (Method::GET, "/api/v1/status", None, StatusCode::OK),
        (Method::GET, "/api/v1/snapshot", None, StatusCode::OK),
        (Method::GET, "/api/v1/markets", None, StatusCode::OK),
        (Method::GET, "/api/v1/markets/current", None, StatusCode::OK),
        (
            Method::GET,
            "/api/v1/markets/missing-market",
            None,
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/api/v1/markets/missing-market/chart?range=full",
            None,
            StatusCode::OK,
        ),
        (Method::GET, "/api/v1/orders", None, StatusCode::OK),
        (Method::GET, "/api/v1/fills", None, StatusCode::OK),
        (Method::GET, "/api/v1/decisions", None, StatusCode::OK),
        (
            Method::GET,
            "/api/v1/events/recent?limit=5",
            None,
            StatusCode::OK,
        ),
        (Method::GET, "/api/v1/pnl", None, StatusCode::OK),
        (
            Method::POST,
            "/api/v1/reports/build",
            Some(json!({})),
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/api/v1/reports/daily/2026-06-10",
            None,
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/api/v1/reports/rust-shadow-latest",
            None,
            StatusCode::OK,
        ),
        (
            Method::POST,
            "/api/v1/control/pause",
            Some(json!({"reason": "smoke"})),
            StatusCode::OK,
        ),
        (
            Method::POST,
            "/api/v1/control/resume",
            Some(json!({"reason": "smoke"})),
            StatusCode::OK,
        ),
        (
            Method::POST,
            "/api/v1/control/kill-switch",
            Some(json!({"enabled": true, "reason": "smoke"})),
            StatusCode::OK,
        ),
        (Method::GET, "/api/v1/config/current", None, StatusCode::OK),
        (
            Method::POST,
            "/api/v1/config/validate",
            Some(json!({})),
            StatusCode::OK,
        ),
        (
            Method::POST,
            "/api/v1/config/apply",
            Some(json!({"config": {}, "reason": "smoke"})),
            StatusCode::OK,
        ),
    ];
    for (method, path, body, expected_status) in cases {
        let request = json_request(method, path, body);
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), expected_status, "{path}");
    }
}

#[tokio::test]
async fn latest_report_is_empty_payload_before_first_build() {
    let app = app(RuntimeSettings::default());

    let response = app
        .oneshot(json_request(Method::GET, "/api/v1/reports/latest", None))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(payload.get("job").is_some_and(serde_json::Value::is_null));
    assert!(payload
        .get("report")
        .is_some_and(serde_json::Value::is_null));
}

#[tokio::test]
async fn pnl_missing_event_source_returns_empty_report() {
    let app = app(RuntimeSettings::default());
    let missing_source = "/tmp/polyedge-definitely-missing-events.jsonl";

    let response = app
        .clone()
        .oneshot(json_request(
            Method::GET,
            &format!("/api/v1/pnl?prefix={missing_source}"),
            None,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["source"]["available"], false);
    assert_eq!(payload["replay_estimate"]["event_count"], 0);
    assert!(payload["replay_estimate"]["notes"]
        .as_array()
        .is_some_and(|notes| notes.iter().any(|note| note
            .as_str()
            .is_some_and(|text| text.contains("event source was not found")))));

    let response = app
        .oneshot(json_request(
            Method::POST,
            "/api/v1/reports/build",
            Some(json!({ "prefix": missing_source })),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["report"]["source"]["available"], false);
}

#[tokio::test]
async fn api_requires_bearer_token_when_enabled() {
    let app = app(auth_settings(Some("secret-token")));

    let missing = app
        .clone()
        .oneshot(json_request(Method::GET, "/api/v1/health", None))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let wrong = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/health")
                .header(header::AUTHORIZATION, "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let ok = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/health")
                .header(header::AUTHORIZATION, "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
}

#[tokio::test]
async fn api_auth_reports_missing_configured_token() {
    let app = app(auth_settings(None));

    let response = app
        .oneshot(json_request(Method::GET, "/api/v1/health", None))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

fn json_request(method: Method, path: &str, body: Option<serde_json::Value>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    let bytes = body
        .map(|value| serde_json::to_vec(&value).unwrap())
        .unwrap_or_default();
    builder.body(Body::from(bytes)).unwrap()
}

fn auth_settings(token: Option<&str>) -> RuntimeSettings {
    let mut settings = RuntimeSettings::default();
    settings.deploy.require_api_auth = true;
    settings.deploy.api_bearer_token = token.map(str::to_owned);
    settings
}
