use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::env;
use std::time::Duration;

const DEFAULT_ARM_API_VERSION: &str = "2026-01-01";
const DEFAULT_MANAGEMENT_ENDPOINT: &str = "https://management.azure.com";

#[derive(Clone)]
pub struct AzureJobClient {
    endpoint: String,
    subscription_id: String,
    resource_group: String,
    api_version: String,
    client_id: Option<String>,
    agent: ureq::Agent,
}

#[derive(Clone, Debug)]
pub struct JobStartOptions {
    pub env: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct AzureLogAnalyticsClient {
    endpoint: String,
    workspace_id: String,
    client_id: Option<String>,
    agent: ureq::Agent,
}

impl AzureJobClient {
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(subscription_id) = env::var("AZURE_SUBSCRIPTION_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };
        let Some(resource_group) = env::var("AZURE_RESOURCE_GROUP")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            endpoint: env::var("AZURE_MANAGEMENT_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_MANAGEMENT_ENDPOINT.to_owned())
                .trim_end_matches('/')
                .to_owned(),
            subscription_id,
            resource_group,
            api_version: env::var("AZURE_ARM_API_VERSION")
                .unwrap_or_else(|_| DEFAULT_ARM_API_VERSION.to_owned()),
            client_id: env::var("AZURE_CLIENT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(3))
                .timeout_read(Duration::from_secs(8))
                .timeout_write(Duration::from_secs(8))
                .build(),
        }))
    }

    pub fn list_executions(&self, job_name: &str) -> Result<Vec<Value>, String> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/jobs/{}/executions?api-version={}",
            self.endpoint,
            self.subscription_id,
            self.resource_group,
            path_segment(job_name),
            self.api_version
        );
        let token = self.access_token()?;
        let response = self
            .agent
            .get(&url)
            .set("Authorization", &format!("Bearer {token}"))
            .set("Accept", "application/json")
            .call()
            .map_err(arm_error)?;
        let payload = response_json(response, "Azure job execution")?;
        Ok(payload["value"].as_array().cloned().unwrap_or_default())
    }

    pub fn start_job(
        &self,
        job_name: &str,
        options: Option<JobStartOptions>,
    ) -> Result<Value, String> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/jobs/{}/start?api-version={}",
            self.endpoint,
            self.subscription_id,
            self.resource_group,
            path_segment(job_name),
            self.api_version
        );
        let token = self.access_token()?;
        let body = start_body(options);
        let body_text = serde_json::to_string(&body).map_err(|error| {
            format!("Azure job start request was not JSON serializable: {error}")
        })?;
        let response = self
            .agent
            .post(&url)
            .set("Authorization", &format!("Bearer {token}"))
            .set("Accept", "application/json")
            .set("Content-Type", "application/json")
            .send_string(&body_text)
            .map_err(arm_error)?;
        let status = response.status();
        let location = response.header("Location").map(str::to_owned);
        let payload = response_json(response, "Azure job start").unwrap_or(Value::Null);
        Ok(json!({
            "status_code": status,
            "location": location,
            "payload": payload
        }))
    }

    fn access_token(&self) -> Result<String, String> {
        if let Ok(token) = env::var("AZURE_MANAGEMENT_BEARER_TOKEN") {
            if !token.trim().is_empty() {
                return Ok(token);
            }
        }
        managed_identity_token(
            &self.agent,
            self.client_id.as_deref(),
            "https://management.azure.com/",
        )
    }
}

impl AzureLogAnalyticsClient {
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(workspace_id) = env::var("AZURE_LOG_ANALYTICS_WORKSPACE_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            endpoint: env::var("AZURE_LOG_ANALYTICS_ENDPOINT")
                .unwrap_or_else(|_| "https://api.loganalytics.azure.com".to_owned())
                .trim_end_matches('/')
                .to_owned(),
            workspace_id,
            client_id: env::var("AZURE_CLIENT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(3))
                .timeout_read(Duration::from_secs(15))
                .timeout_write(Duration::from_secs(8))
                .build(),
        }))
    }

    pub fn execution_logs(&self, job_name: &str, execution_id: &str) -> Result<Value, String> {
        let query = container_app_job_log_query(job_name, execution_id);
        let body = json!({
            "query": query,
            "timespan": "P7D"
        });
        let body_text = serde_json::to_string(&body)
            .map_err(|error| format!("Log query request was not JSON serializable: {error}"))?;
        let url = format!(
            "{}/v1/workspaces/{}/query",
            self.endpoint,
            path_segment(&self.workspace_id)
        );
        let token = self.access_token()?;
        let response = self
            .agent
            .post(&url)
            .set("Authorization", &format!("Bearer {token}"))
            .set("Accept", "application/json")
            .set("Content-Type", "application/json")
            .set("Prefer", "wait=10")
            .send_string(&body_text)
            .map_err(|error| {
                format!(
                    "Azure Log Analytics query failed: {}",
                    sanitized_error(error)
                )
            })?;
        response_json(response, "Azure Log Analytics query")
    }

    fn access_token(&self) -> Result<String, String> {
        if let Ok(token) = env::var("AZURE_LOG_ANALYTICS_BEARER_TOKEN") {
            if !token.trim().is_empty() {
                return Ok(token);
            }
        }
        managed_identity_token(
            &self.agent,
            self.client_id.as_deref(),
            "https://api.loganalytics.azure.com",
        )
    }
}

pub fn latest_execution_summary(executions: &[Value]) -> Option<Value> {
    executions
        .iter()
        .max_by_key(|execution| {
            execution_ts(execution, "startTime")
                .or_else(|| execution_ts(execution, "endTime"))
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        })
        .map(execution_summary)
}

pub fn execution_summary(execution: &Value) -> Value {
    let properties = &execution["properties"];
    let start = properties["startTime"].as_str();
    let finish = properties["endTime"].as_str();
    json!({
        "execution_name": execution["name"].as_str(),
        "execution_id": execution["id"].as_str(),
        "status": properties["status"].as_str().unwrap_or("unknown"),
        "last_start": start,
        "last_finish": finish,
        "duration": duration_seconds(start, finish),
        "running": properties["status"].as_str().is_some_and(|status| status.eq_ignore_ascii_case("Running")),
        "exit_code": Value::Null,
        "error": Value::Null
    })
}

fn start_body(options: Option<JobStartOptions>) -> Value {
    let Some(options) = options else {
        return json!({});
    };
    if options.env.is_empty() {
        return json!({});
    }
    json!({
        "containers": [
            {
                "name": "research-job",
                "env": options.env.into_iter().map(|(name, value)| json!({ "name": name, "value": value })).collect::<Vec<_>>()
            }
        ]
    })
}

fn execution_ts(execution: &Value, key: &str) -> Option<DateTime<Utc>> {
    execution["properties"][key]
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn duration_seconds(start: Option<&str>, finish: Option<&str>) -> Value {
    let (Some(start), Some(finish)) = (start, finish) else {
        return Value::Null;
    };
    let Ok(start) = DateTime::parse_from_rfc3339(start) else {
        return Value::Null;
    };
    let Ok(finish) = DateTime::parse_from_rfc3339(finish) else {
        return Value::Null;
    };
    json!(finish.signed_duration_since(start).num_seconds().max(0))
}

fn arm_error(error: ureq::Error) -> String {
    format!("Azure ARM request failed: {}", sanitized_error(error))
}

fn response_json(response: ureq::Response, label: &str) -> Result<Value, String> {
    let text = response
        .into_string()
        .map_err(|error| format!("{label} response could not be read: {error}"))?;
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).map_err(|error| format!("{label} response was not JSON: {error}"))
}

fn managed_identity_token(
    agent: &ureq::Agent,
    client_id: Option<&str>,
    resource: &str,
) -> Result<String, String> {
    let identity_endpoint = env::var("IDENTITY_ENDPOINT").ok();
    let identity_header = env::var("IDENTITY_HEADER").ok();
    let url = managed_identity_token_url(identity_endpoint.as_deref(), client_id, resource);
    let mut request = agent
        .get(&url)
        .set("Metadata", "true")
        .set("Accept", "application/json");
    if let (Some(_), Some(header)) = (identity_endpoint.as_deref(), identity_header.as_deref()) {
        request = request.set("X-IDENTITY-HEADER", header);
    }
    let response = request.call().map_err(|error| {
        format!(
            "managed identity token unavailable: {}",
            sanitized_error(error)
        )
    })?;
    let payload = response_json(response, "managed identity token")?;
    payload["access_token"]
        .as_str()
        .map(str::to_owned)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| "managed identity token response was missing access_token".to_owned())
}

fn managed_identity_token_url(
    identity_endpoint: Option<&str>,
    client_id: Option<&str>,
    resource: &str,
) -> String {
    let mut url = match identity_endpoint {
        Some(endpoint) if !endpoint.trim().is_empty() => format!(
            "{}?api-version=2019-08-01&resource={}",
            endpoint,
            query_component(resource)
        ),
        _ => format!(
            "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={}",
            query_component(resource)
        ),
    };
    if let Some(client_id) = client_id {
        url.push_str("&client_id=");
        url.push_str(&query_component(client_id));
    }
    url
}

fn container_app_job_log_query(job_name: &str, execution_id: &str) -> String {
    let job_name = kql_string(job_name);
    let execution_id = kql_string(execution_id);
    format!(
        r#"let targetJob = '{job_name}';
let targetExecution = '{execution_id}';
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppSystemLogs_CL
| where TimeGenerated > ago(7d)
| where tostring(JobName_s) == targetJob
    or tostring(ContainerAppName_s) == targetJob
    or tostring(ResourceName) == targetJob
    or tostring(_ResourceId) has strcat('/jobs/', targetJob)
| where isempty(targetExecution)
    or tostring(ExecutionName_s) == targetExecution
    or tostring(ReplicaName_s) has targetExecution
    or tostring(ContainerName_s) has targetExecution
    or tostring(Log_s) has targetExecution
| project TimeGenerated,
          Level = coalesce(tostring(Level_s), tostring(LogLevel_s), ''),
          Message = coalesce(tostring(Log_s), tostring(Message), tostring(Reason_s), tostring(Type_s), ''),
          Replica = tostring(ReplicaName_s),
          Container = tostring(ContainerName_s)
| order by TimeGenerated desc
| take 200"#
    )
}

fn kql_string(value: &str) -> String {
    value
        .chars()
        .take(160)
        .flat_map(|ch| match ch {
            '\'' => "''".chars().collect::<Vec<_>>(),
            '\n' | '\r' | '\t' => vec![' '],
            ch if ch.is_control() => Vec::new(),
            ch => vec![ch],
        })
        .collect()
}

fn sanitized_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(status, response) => {
            let text = response.into_string().unwrap_or_default();
            format!(
                "HTTP {status}: {}",
                text.chars().take(400).collect::<String>()
            )
        }
        ureq::Error::Transport(error) => error.to_string(),
    }
}

fn path_segment(value: &str) -> String {
    query_component(value)
}

fn query_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_execution_summary_picks_newest_start() {
        let executions = vec![
            json!({"name": "old", "properties": {"startTime": "2026-06-14T01:00:00Z", "endTime": "2026-06-14T01:01:00Z", "status": "Succeeded"}}),
            json!({"name": "new", "properties": {"startTime": "2026-06-15T01:00:00Z", "status": "Running"}}),
        ];
        let summary = latest_execution_summary(&executions).expect("summary");
        assert_eq!(summary["execution_name"], "new");
        assert_eq!(summary["status"], "Running");
        assert_eq!(summary["running"], true);
    }

    #[test]
    fn execution_summary_computes_duration() {
        let execution = json!({
            "name": "done",
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/job/executions/done",
            "properties": {
                "startTime": "2026-06-15T01:00:00Z",
                "endTime": "2026-06-15T01:02:05Z",
                "status": "Succeeded"
            }
        });
        let summary = execution_summary(&execution);
        assert_eq!(summary["duration"], 125);
        assert_eq!(summary["running"], false);
    }

    #[test]
    fn log_query_escapes_execution_id() {
        let query = container_app_job_log_query("polyedge-job", "exec'bad\nvalue");
        assert!(query.contains("let targetExecution = 'exec''bad value';"));
        assert!(!query.contains("exec'bad\nvalue"));
    }

    #[test]
    fn managed_identity_token_url_uses_container_apps_endpoint() {
        let url = managed_identity_token_url(
            Some("http://127.0.0.1:42356/msi/token"),
            Some("client/id"),
            "https://management.azure.com/",
        );
        assert_eq!(
            url,
            "http://127.0.0.1:42356/msi/token?api-version=2019-08-01&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id=client%2Fid"
        );
    }
}
