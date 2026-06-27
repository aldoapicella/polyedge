use super::*;

pub const DEFAULT_EXCLUSION_FILE: &str = "data_quality/exclusion_windows.yaml";
pub const DEFAULT_FROZEN_CANDIDATES_FILE: &str = "research/configs/frozen_candidates.yaml";
pub const DEFAULT_PROSPECTIVE_SINCE: &str = "2026-06-14T00:00:00Z";
pub const FROZEN_CANDIDATE_NAMES: [&str; 4] = [
    "static_baseline",
    "dynamic_quote_style",
    "full_deterministic_profile",
    "dynamic_safety_only",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExclusionRegistry {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub windows: Vec<ExclusionWindowRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExclusionWindowRecord {
    pub id: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub reason: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub default_exclude: bool,
}

impl ExclusionRegistry {
    pub fn default_windows(&self) -> Vec<ExcludedTimeWindow> {
        self.windows
            .iter()
            .filter(|window| window.default_exclude)
            .map(|window| ExcludedTimeWindow {
                start: window.start,
                end: window.end,
            })
            .collect()
    }

    pub fn as_json(&self) -> Value {
        json!({
            "version": self.version,
            "updated_at": ts(self.updated_at),
            "windows": self.windows.iter().map(ExclusionWindowRecord::as_json).collect::<Vec<_>>()
        })
    }
}

impl ExclusionWindowRecord {
    pub fn as_json(&self) -> Value {
        json!({
            "id": self.id,
            "start": ts(self.start),
            "end": ts(self.end),
            "end_exclusive": ts(self.end),
            "reason": self.reason,
            "evidence": self.evidence,
            "default_exclude": self.default_exclude
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrozenCandidateRegistry {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub research_only: bool,
    #[serde(default)]
    pub paper_only: bool,
    #[serde(default)]
    pub enabled_by_default: bool,
    pub selection_rule: String,
    #[serde(default)]
    pub candidates: Vec<FrozenCandidateRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrozenCandidateRecord {
    pub name: String,
    pub profile: String,
    pub candidate_version: String,
    pub config_hash: String,
    pub created_at: DateTime<Utc>,
    pub frozen_since: DateTime<Utc>,
    pub reason: String,
    #[serde(default)]
    pub enabled_by_default: bool,
    #[serde(default)]
    pub deployment_allowed: bool,
    #[serde(default)]
    pub notes: String,
}

impl FrozenCandidateRegistry {
    pub fn as_json(&self) -> Value {
        json!({
            "version": self.version,
            "updated_at": ts(self.updated_at),
            "research_only": self.research_only,
            "paper_only": self.paper_only,
            "enabled_by_default": self.enabled_by_default,
            "selection_rule": self.selection_rule,
            "candidates": self.candidates.iter().map(FrozenCandidateRecord::as_json).collect::<Vec<_>>(),
            "required_candidates": FROZEN_CANDIDATE_NAMES
        })
    }

    fn validate_required_candidates(&self) -> Result<(), ResearchError> {
        for required in FROZEN_CANDIDATE_NAMES {
            if !self
                .candidates
                .iter()
                .any(|candidate| candidate.name == required)
            {
                return Err(ResearchError::InvalidInput(format!(
                    "frozen candidate registry is missing {required}"
                )));
            }
        }
        if self
            .candidates
            .iter()
            .any(|candidate| candidate.enabled_by_default || candidate.deployment_allowed)
        {
            return Err(ResearchError::InvalidInput(
                "frozen candidates must be disabled by default and disallowed for deployment"
                    .to_owned(),
            ));
        }
        for candidate in &self.candidates {
            if candidate.candidate_version.trim().is_empty()
                || candidate.config_hash.trim().is_empty()
                || candidate.reason.trim().is_empty()
            {
                return Err(ResearchError::InvalidInput(format!(
                    "frozen candidate {} missing immutable version/hash/reason metadata",
                    candidate.name
                )));
            }
            if candidate.frozen_since < candidate.created_at {
                return Err(ResearchError::InvalidInput(format!(
                    "frozen candidate {} frozen_since cannot be before created_at",
                    candidate.name
                )));
            }
        }
        Ok(())
    }
}

impl FrozenCandidateRecord {
    fn as_json(&self) -> Value {
        json!({
            "name": self.name,
            "profile": self.profile,
            "candidate_version": self.candidate_version,
            "config_hash": self.config_hash,
            "created_at": ts(self.created_at),
            "frozen_since": ts(self.frozen_since),
            "reason": self.reason,
            "enabled_by_default": self.enabled_by_default,
            "deployment_allowed": self.deployment_allowed,
            "notes": self.notes
        })
    }
}

pub fn load_exclusion_registry(path: &Path) -> Result<ExclusionRegistry, ResearchError> {
    let text = fs::read_to_string(path)?;
    let registry = parse_exclusion_registry_yaml(&text)?;
    for window in &registry.windows {
        if window.start >= window.end {
            return Err(ResearchError::InvalidInput(format!(
                "exclusion window {} start must be before end",
                window.id
            )));
        }
    }
    Ok(registry)
}

pub fn load_default_exclusions(path: &Path) -> Result<Vec<ExcludedTimeWindow>, ResearchError> {
    Ok(load_exclusion_registry(path)?.default_windows())
}

pub fn load_frozen_candidate_registry(
    path: &Path,
) -> Result<FrozenCandidateRegistry, ResearchError> {
    let text = fs::read_to_string(path)?;
    let registry = parse_frozen_candidate_yaml(&text)?;
    registry.validate_required_candidates()?;
    Ok(registry)
}

fn parse_exclusion_registry_yaml(text: &str) -> Result<ExclusionRegistry, ResearchError> {
    let mut version = None;
    let mut updated_at = None;
    let mut windows = Vec::new();
    let mut current: Option<ExclusionWindowBuilder> = None;
    let mut in_evidence = false;
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(value) = yaml_value(line, "version") {
            version = value.parse::<u32>().ok();
            continue;
        }
        if let Some(value) = yaml_value(line, "updated_at") {
            updated_at = parse_rfc3339_utc(&unquote(value));
            continue;
        }
        if let Some(value) = line.strip_prefix("- id:") {
            if let Some(builder) = current.take() {
                windows.push(builder.finish()?);
            }
            current = Some(ExclusionWindowBuilder {
                id: Some(unquote(value.trim())),
                ..ExclusionWindowBuilder::default()
            });
            in_evidence = false;
            continue;
        }
        let Some(builder) = current.as_mut() else {
            continue;
        };
        if let Some(value) = yaml_value(line, "start") {
            builder.start = parse_rfc3339_utc(&unquote(value));
            in_evidence = false;
        } else if let Some(value) = yaml_value(line, "end") {
            builder.end = parse_rfc3339_utc(&unquote(value));
            in_evidence = false;
        } else if let Some(value) = yaml_value(line, "reason") {
            builder.reason = Some(unquote(value));
            in_evidence = false;
        } else if line == "evidence:" {
            in_evidence = true;
        } else if let Some(value) = yaml_value(line, "default_exclude") {
            builder.default_exclude = parse_yaml_bool(value);
            in_evidence = false;
        } else if in_evidence {
            if let Some(value) = line.strip_prefix("- ") {
                builder.evidence.push(unquote(value.trim()));
            }
        }
    }
    if let Some(builder) = current.take() {
        windows.push(builder.finish()?);
    }
    Ok(ExclusionRegistry {
        version: version.ok_or_else(|| {
            ResearchError::InvalidInput("exclusion registry missing version".to_owned())
        })?,
        updated_at: updated_at.ok_or_else(|| {
            ResearchError::InvalidInput("exclusion registry missing updated_at".to_owned())
        })?,
        windows,
    })
}

#[derive(Default)]
struct ExclusionWindowBuilder {
    id: Option<String>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    reason: Option<String>,
    evidence: Vec<String>,
    default_exclude: bool,
}

impl ExclusionWindowBuilder {
    fn finish(self) -> Result<ExclusionWindowRecord, ResearchError> {
        let id = self
            .id
            .ok_or_else(|| ResearchError::InvalidInput("exclusion window missing id".to_owned()))?;
        Ok(ExclusionWindowRecord {
            id,
            start: self.start.ok_or_else(|| {
                ResearchError::InvalidInput("exclusion window missing start".to_owned())
            })?,
            end: self.end.ok_or_else(|| {
                ResearchError::InvalidInput("exclusion window missing end".to_owned())
            })?,
            reason: self.reason.ok_or_else(|| {
                ResearchError::InvalidInput("exclusion window missing reason".to_owned())
            })?,
            evidence: self.evidence,
            default_exclude: self.default_exclude,
        })
    }
}

fn parse_frozen_candidate_yaml(text: &str) -> Result<FrozenCandidateRegistry, ResearchError> {
    let mut version = None;
    let mut updated_at = None;
    let mut research_only = false;
    let mut paper_only = false;
    let mut enabled_by_default = false;
    let mut selection_rule = None;
    let mut candidates = Vec::new();
    let mut current: Option<FrozenCandidateBuilder> = None;
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(value) = yaml_value(line, "version") {
            version = value.parse::<u32>().ok();
            continue;
        }
        if let Some(value) = yaml_value(line, "updated_at") {
            updated_at = parse_rfc3339_utc(&unquote(value));
            continue;
        }
        if let Some(value) = yaml_value(line, "research_only") {
            research_only = parse_yaml_bool(value);
            continue;
        }
        if let Some(value) = yaml_value(line, "paper_only") {
            paper_only = parse_yaml_bool(value);
            continue;
        }
        if let Some(value) = yaml_value(line, "enabled_by_default") {
            if let Some(candidate) = current.as_mut() {
                candidate.enabled_by_default = parse_yaml_bool(value);
            } else {
                enabled_by_default = parse_yaml_bool(value);
            }
            continue;
        }
        if let Some(value) = yaml_value(line, "selection_rule") {
            selection_rule = Some(unquote(value));
            continue;
        }
        if let Some(value) = line.strip_prefix("- name:") {
            if let Some(builder) = current.take() {
                candidates.push(builder.finish()?);
            }
            current = Some(FrozenCandidateBuilder {
                name: Some(unquote(value.trim())),
                ..FrozenCandidateBuilder::default()
            });
            continue;
        }
        let Some(builder) = current.as_mut() else {
            continue;
        };
        if let Some(value) = yaml_value(line, "profile") {
            builder.profile = Some(unquote(value));
        } else if let Some(value) = yaml_value(line, "candidate_version") {
            builder.candidate_version = Some(unquote(value));
        } else if let Some(value) = yaml_value(line, "config_hash") {
            builder.config_hash = Some(unquote(value));
        } else if let Some(value) = yaml_value(line, "created_at") {
            builder.created_at = parse_rfc3339_utc(&unquote(value));
        } else if let Some(value) = yaml_value(line, "frozen_since") {
            builder.frozen_since = parse_rfc3339_utc(&unquote(value));
        } else if let Some(value) = yaml_value(line, "reason") {
            builder.reason = Some(unquote(value));
        } else if let Some(value) = yaml_value(line, "deployment_allowed") {
            builder.deployment_allowed = parse_yaml_bool(value);
        } else if let Some(value) = yaml_value(line, "notes") {
            builder.notes = unquote(value);
        }
    }
    if let Some(builder) = current.take() {
        candidates.push(builder.finish()?);
    }
    Ok(FrozenCandidateRegistry {
        version: version.ok_or_else(|| {
            ResearchError::InvalidInput("frozen candidate registry missing version".to_owned())
        })?,
        updated_at: updated_at.ok_or_else(|| {
            ResearchError::InvalidInput("frozen candidate registry missing updated_at".to_owned())
        })?,
        research_only,
        paper_only,
        enabled_by_default,
        selection_rule: selection_rule.ok_or_else(|| {
            ResearchError::InvalidInput(
                "frozen candidate registry missing selection_rule".to_owned(),
            )
        })?,
        candidates,
    })
}

#[derive(Default)]
struct FrozenCandidateBuilder {
    name: Option<String>,
    profile: Option<String>,
    candidate_version: Option<String>,
    config_hash: Option<String>,
    created_at: Option<DateTime<Utc>>,
    frozen_since: Option<DateTime<Utc>>,
    reason: Option<String>,
    enabled_by_default: bool,
    deployment_allowed: bool,
    notes: String,
}

impl FrozenCandidateBuilder {
    fn finish(self) -> Result<FrozenCandidateRecord, ResearchError> {
        Ok(FrozenCandidateRecord {
            name: self.name.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing name".to_owned())
            })?,
            profile: self.profile.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing profile".to_owned())
            })?,
            candidate_version: self.candidate_version.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing candidate_version".to_owned())
            })?,
            config_hash: self.config_hash.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing config_hash".to_owned())
            })?,
            created_at: self.created_at.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing created_at".to_owned())
            })?,
            frozen_since: self.frozen_since.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing frozen_since".to_owned())
            })?,
            reason: self.reason.ok_or_else(|| {
                ResearchError::InvalidInput("frozen candidate missing reason".to_owned())
            })?,
            enabled_by_default: self.enabled_by_default,
            deployment_allowed: self.deployment_allowed,
            notes: self.notes,
        })
    }
}

fn yaml_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.strip_prefix(&format!("{key}:")).map(str::trim)
}

fn unquote(value: &str) -> String {
    value.trim().trim_matches('"').trim_matches('\'').to_owned()
}

fn parse_yaml_bool(value: &str) -> bool {
    matches!(unquote(value).as_str(), "true" | "True" | "TRUE" | "yes")
}
