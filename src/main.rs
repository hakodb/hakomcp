use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
#[cfg(feature = "cloud-sync")]
use hakodb::cloud_sync::{CloudSync, CloudSyncMode};
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::engine::CollectionStats;
use hakodb::engine::{BatchMutation, ChangeEvent, ChangeKind, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::index::traverse::{TraverseDir, TraverseSpec};
use hakodb::index::vector::{decode_f32s, distance, encode_f32s, Metric};
#[cfg(feature = "net-sync")]
use hakodb::net_sync::{DiscoveryMode, NetSyncer};
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::plan::QueryPlan;
use hakodb::query::query::{AggregateOp, Query};
use hakodb::schema::CollectionSchema;
use serde_json::{json, Value as JsonValue};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const RESOURCE_PREFIX: &str = "hakodb://collections/";
const PROTOCOL_VERSION: &str = "2024-11-05";
const DEFAULT_MAX_QUERY_ROWS: usize = 1000;
const DEFAULT_MAX_RESULT_BYTES: usize = 1_048_576;
const RESOURCE_SAMPLE_LIMIT: usize = 100;
const HIDDEN_COLLECTION_PREFIX: &str = "__hako_";

type RpcResult<T> = std::result::Result<T, RpcError>;

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
        }
    }

    fn resource_not_found(uri: &str) -> Self {
        Self {
            code: -32002,
            message: format!("Resource not found: {uri}"),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            code: -32603,
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    Read,
    Data,
    Sync,
    Admin,
}

impl Profile {
    fn parse(raw: &str) -> Result<Self> {
        Ok(match raw {
            "read" => Self::Read,
            "data" => Self::Data,
            "sync" => Self::Sync,
            "admin" => Self::Admin,
            other => bail!("`--profile` must be read, data, sync, or admin, got `{other}`"),
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Data => "data",
            Self::Sync => "sync",
            Self::Admin => "admin",
        }
    }

    fn allows_tool(self, name: &str) -> bool {
        match name {
            "document_get"
            | "query"
            | "find_near"
            | "find_paths"
            | "collection_list"
            | "collection_describe"
            | "collection_stats"
            | "aggregate"
            | "explain"
            | "index_list"
            | "server_status"
            | "job_list"
            | "job_get"
            | "watch_create"
            | "watch_delete" => true,
            "document_put" | "document_patch" | "document_delete" | "document_batch"
            | "index_create" | "schema_set" | "schema_clear" => {
                matches!(self, Self::Data | Self::Sync | Self::Admin)
            }
            "job_cancel" | "compact" | "vacuum" | "flush" | "documents_delete_where" => {
                matches!(self, Self::Admin)
            }
            "sync_start" | "sync_stop" | "sync_status" | "sync_peers" | "sync_versions" => {
                matches!(self, Self::Sync | Self::Admin)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    fn parse(raw: &str) -> Result<Self> {
        Ok(match raw {
            "text" => Self::Text,
            "json" => Self::Json,
            other => bail!("`--log-format` must be text or json, got `{other}`"),
        })
    }
}

#[derive(Debug, Clone)]
struct ServerConfig {
    profile: Profile,
    allow_network: bool,
    allow_destructive: bool,
    subscribe_to: Vec<String>,
    collection_allow: Vec<String>,
    collection_deny: Vec<String>,
    max_query_rows: usize,
    max_result_bytes: usize,
    log_format: LogFormat,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            profile: Profile::Data,
            allow_network: false,
            allow_destructive: false,
            subscribe_to: Vec::new(),
            collection_allow: Vec::new(),
            collection_deny: Vec::new(),
            max_query_rows: DEFAULT_MAX_QUERY_ROWS,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            log_format: LogFormat::Text,
        }
    }
}

impl ServerConfig {
    fn validate(&self) -> Result<()> {
        if self.profile == Profile::Sync && !self.allow_network {
            bail!("profile `sync` requires --allow-network");
        }
        if self.max_query_rows == 0 {
            bail!("--max-query-rows must be >= 1");
        }
        if self.max_result_bytes == 0 {
            bail!("--max-result-bytes must be >= 1");
        }
        Ok(())
    }

    fn subscriptions_enabled(&self) -> bool {
        !self.subscribe_to.is_empty()
    }

    fn collection_allowed(&self, name: &str) -> bool {
        if pattern_match(&self.collection_deny, name) {
            return false;
        }
        if !self.collection_allow.is_empty() && !pattern_match(&self.collection_allow, name) {
            return false;
        }
        if name.starts_with(HIDDEN_COLLECTION_PREFIX)
            && !pattern_match(&self.collection_allow, name)
        {
            return false;
        }
        true
    }
}

struct Cli {
    db: PathBuf,
    config: ServerConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone)]
struct JobRecord {
    id: String,
    kind: String,
    state: JobState,
    progress: f64,
    created_at: u64,
    started_at: Option<u64>,
    finished_at: Option<u64>,
    result: Option<JsonValue>,
    error: Option<String>,
    cancellable: bool,
}

impl JobRecord {
    fn to_json(&self) -> JsonValue {
        json!({
            "id": self.id,
            "kind": self.kind,
            "state": self.state.as_str(),
            "progress": self.progress,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error": self.error,
            "cancellable": self.cancellable
        })
    }
}

struct JobManager {
    jobs: Mutex<HashMap<String, JobRecord>>,
    next_id: AtomicU64,
}

impl JobManager {
    fn new() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn create(&self, kind: &str, cancellable: bool) -> String {
        let id = format!("job-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let record = JobRecord {
            id: id.clone(),
            kind: kind.to_string(),
            state: JobState::Queued,
            progress: 0.0,
            created_at: now_millis(),
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            cancellable,
        };
        self.jobs.lock().unwrap().insert(id.clone(), record);
        id
    }

    fn start(&self, id: &str) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(id) {
            job.state = JobState::Running;
            job.started_at = Some(now_millis());
        }
    }

    fn progress(&self, id: &str, progress: f64) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(id) {
            job.progress = progress;
        }
    }

    fn succeed(&self, id: &str, result: JsonValue) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(id) {
            job.state = JobState::Succeeded;
            job.progress = 1.0;
            job.finished_at = Some(now_millis());
            job.result = Some(result);
        }
    }

    fn fail(&self, id: &str, error: String) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(id) {
            job.state = JobState::Failed;
            job.finished_at = Some(now_millis());
            job.error = Some(error);
        }
    }

    fn get(&self, id: &str) -> Option<JobRecord> {
        self.jobs.lock().unwrap().get(id).cloned()
    }

    fn list(&self) -> Vec<JobRecord> {
        let mut jobs: Vec<JobRecord> = self.jobs.lock().unwrap().values().cloned().collect();
        jobs.sort_by_key(|job| job.created_at);
        jobs
    }

    fn cancel(&self, id: &str) -> std::result::Result<(), String> {
        let mut jobs = self.jobs.lock().unwrap();
        let Some(job) = jobs.get_mut(id) else {
            return Err(format!("unknown job `{id}`"));
        };
        if !job.cancellable {
            return Err(format!("job `{id}` is not cancellable"));
        }
        match job.state {
            JobState::Queued | JobState::Running => {
                job.state = JobState::Cancelled;
                job.finished_at = Some(now_millis());
                Ok(())
            }
            state => Err(format!("job `{id}` is already {}", state.as_str())),
        }
    }
}

struct Watch {
    id: String,
    collection: String,
    plan: QueryPlan,
    matched: HashSet<String>,
}

enum SyncController {
    #[cfg(feature = "net-sync")]
    Mesh(Arc<NetSyncer>),
    #[cfg(feature = "cloud-sync")]
    Cloud(Arc<CloudSync>),
    #[cfg(not(any(feature = "net-sync", feature = "cloud-sync")))]
    #[allow(dead_code)]
    Disabled,
}

struct Server {
    db: Arc<Hako>,
    config: ServerConfig,
    initialized: AtomicBool,
    notifier: Option<tokio::sync::mpsc::Sender<JsonValue>>,
    subscriptions: Arc<Mutex<HashSet<String>>>,
    watches: Arc<Mutex<HashMap<String, Watch>>>,
    watch_collections: Arc<Mutex<HashSet<String>>>,
    next_watch_id: AtomicU64,
    jobs: Arc<JobManager>,
    shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    runtime: Option<tokio::runtime::Handle>,
    sync: Arc<Mutex<Option<SyncController>>>,
}

impl Server {
    fn new(
        db: Arc<Hako>,
        config: ServerConfig,
        notifier: Option<tokio::sync::mpsc::Sender<JsonValue>>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        if config.profile == Profile::Read {
            db.set_read_only(true);
        }
        Self {
            db,
            config,
            initialized: AtomicBool::new(false),
            notifier,
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            watches: Arc::new(Mutex::new(HashMap::new())),
            watch_collections: Arc::new(Mutex::new(HashSet::new())),
            next_watch_id: AtomicU64::new(1),
            jobs: Arc::new(JobManager::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            runtime,
            sync: Arc::new(Mutex::new(None)),
        }
    }

    fn log(&self, message: &str) {
        match self.config.log_format {
            LogFormat::Text => eprintln!("[hakomcp] {message}"),
            LogFormat::Json => {
                let value = json!({"level": "info", "target": "hakomcp", "message": message});
                eprintln!("{value}");
            }
        }
    }

    fn handle(&self, message: &JsonValue) -> Option<JsonValue> {
        let id = message.get("id").cloned();
        let method = message.get("method").and_then(JsonValue::as_str);

        // Notifications (no id) need no response.
        if id.is_none() {
            if method == Some("notifications/initialized") {
                self.initialized.store(true, Ordering::SeqCst);
            }
            return None;
        }
        let id = id.unwrap();
        let Some(method) = method else {
            return Some(error_response(
                id,
                -32600,
                "Invalid Request: missing method",
            ));
        };

        if method != "initialize" && method != "ping" && !self.initialized.load(Ordering::SeqCst) {
            return Some(error_response(id, -32002, "Server not initialized"));
        }

        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        let result: RpcResult<JsonValue> = match method {
            "initialize" => {
                self.initialized.store(true, Ordering::SeqCst);
                Ok(self.initialize(&params))
            }
            "ping" => Ok(json!({})),
            "tools/list" => Ok(self.tools_list()),
            "tools/call" => self.tools_call(&params),
            "resources/list" => self.resources_list(),
            "resources/read" => self.resources_read(&params),
            "resources/templates/list" => Ok(json!({ "resourceTemplates": [] })),
            "resources/subscribe" => self.resources_subscribe(&params),
            "resources/unsubscribe" => self.resources_unsubscribe(&params),
            _ => Err(RpcError::method_not_found(method)),
        };

        Some(match result {
            Ok(value) => success_response(id, value),
            Err(error) => error_response(id, error.code, error.message),
        })
    }

    fn initialize(&self, _params: &JsonValue) -> JsonValue {
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false },
                "resources": {
                    "subscribe": self.config.subscriptions_enabled(),
                    "listChanged": false
                }
            },
            "serverInfo": {
                "name": "hakomcp",
                "version": env!("CARGO_PKG_VERSION")
            },
            "instructions": self.instructions()
        })
    }

    fn instructions(&self) -> String {
        let subscriptions = if self.config.subscriptions_enabled() {
            "Change subscriptions are enabled only for the --subscribe-to collections."
        } else {
            "Change notifications are disabled; set --subscribe-to to opt in."
        };
        format!(
            "hakomcp {} controls one local HakoDB database. Profile: {}. Query rows are capped at {}. Tool results are capped at {} bytes. {} MCP is the control plane; HakoDB mesh/cloud sync is the data plane. Do not use this server as a high-QPS application driver.",
            env!("CARGO_PKG_VERSION"),
            self.config.profile.as_str(),
            self.config.max_query_rows,
            self.config.max_result_bytes,
            subscriptions
        )
    }

    fn tools_list(&self) -> JsonValue {
        let mut tools = vec![
            json!({
                "name": "document_get",
                "description": "Read one document by collection and id. Legacy alias: `get`.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "id": { "type": "string" }
                    },
                    "required": ["collection", "id"]
                }
            }),
            json!({
                "name": "document_put",
                "description": "Insert or replace a document. Fields hold ordinary JSON values; `vectors` holds vector-index fields (LE f32 arrays) and overrides same-named doc fields. Legacy alias: `put`.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "id": { "type": "string" },
                        "doc": { "type": "object" },
                        "vectors": {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": { "type": "number" }
                            }
                        }
                    },
                    "required": ["collection", "doc"]
                }
            }),
            json!({
                "name": "query",
                "description": "Query a collection. `filters` are AND clauses, `any` adds OR clauses, and `order_by` sorts the result.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "filters": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "any": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "elem_match": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "filters": {
                                        "type": "array",
                                        "items": { "$ref": "#/$defs/filter" }
                                    }
                                },
                                "required": ["field", "filters"]
                            }
                        },
                        "order_by": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "direction": { "type": "string", "enum": ["asc", "desc"] }
                                },
                                "required": ["field"]
                            }
                        },
                        "limit": { "type": "integer", "minimum": 0 },
                        "offset": { "type": "integer", "minimum": 0 },
                        "cursor": { "type": "string" },
                        "select": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["collection"],
                    "$defs": {
                        "filter": {
                            "type": "object",
                            "properties": {
                                "field": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": [
                                        "eq", "ne", "gt", "gte", "lt", "lte",
                                        "match", "match_prefix", "contains",
                                        "starts_with", "in", "array_contains",
                                        "array_contains_any", "array_all_eq", "not_in"
                                    ]
                                },
                                "value": {}
                            },
                            "required": ["field", "op", "value"]
                        }
                    }
                }
            }),
            json!({
                "name": "index_create",
                "description": "Create an index. `kind` is secondary, fts, vector, composite, edge, or traverse. Legacy alias: `create_index`.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "field": { "type": "string" },
                        "kind": { "type": "string", "enum": ["secondary", "fts", "vector", "composite", "edge", "traverse"] },
                        "dim": { "type": "integer", "minimum": 1 },
                        "metric": { "type": "string", "enum": ["cosine", "l2"] },
                        "fields": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "direction": { "type": "string", "enum": ["asc", "desc"] }
                                },
                                "required": ["field"]
                            }
                        },
                        "unique": { "type": "boolean" },
                        "sparse": { "type": "boolean" }
                    },
                    "required": ["collection", "kind"]
                }
            }),
            json!({
                "name": "find_near",
                "description": "Exact KNN search over a vector index. `vector` is the query embedding and `k` is the hit count. Hits are nearest-first; `score` is the metric distance (lower is closer).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "field": { "type": "string" },
                        "vector": {
                            "type": "array",
                            "items": { "type": "number" }
                        },
                        "k": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["collection", "field", "vector"]
                }
            }),
            json!({
                "name": "find_paths",
                "description": "Walk references from one document. Incoming walks need a traverse or edge index; `via_edge` and `weight_field` use an edge collection.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "id": { "type": "string" },
                        "direction": { "type": "string", "enum": ["out", "in", "both"] },
                        "edge_field": { "type": "string" },
                        "max_depth": { "type": "integer", "minimum": 1 },
                        "limit": { "type": "integer", "minimum": 1 },
                        "via_edge": { "type": "string" },
                        "weight_field": { "type": "string" }
                    },
                    "required": ["collection", "id"]
                }
            }),
            json!({
                "name": "collection_list",
                "description": "List visible collections with document counts.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "collection_describe",
                "description": "Return one collection's schema, indexes, statistics, and residency.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" }
                    },
                    "required": ["collection"]
                }
            }),
            json!({
                "name": "collection_stats",
                "description": "Return size, WAL, blob, tombstone, and residency statistics for one collection.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" }
                    },
                    "required": ["collection"]
                }
            }),
            json!({
                "name": "schema_set",
                "description": "Declare or replace a collection schema. Fields are top-level: name, kind, required, default.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "schema": {
                            "type": "object",
                            "properties": {
                                "fields": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": { "type": "string" },
                                            "kind": { "type": "string" },
                                            "required": { "type": "boolean" },
                                            "default": {}
                                        },
                                        "required": ["name", "kind"]
                                    }
                                }
                            },
                            "required": ["fields"]
                        }
                    },
                    "required": ["collection", "schema"]
                }
            }),
            json!({
                "name": "schema_clear",
                "description": "Remove a collection schema. Existing documents are unaffected.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" }
                    },
                    "required": ["collection"]
                }
            }),
            json!({
                "name": "document_patch",
                "description": "Patch named fields on one document.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "id": { "type": "string" },
                        "updates": { "type": "object" }
                    },
                    "required": ["collection", "id", "updates"]
                }
            }),
            json!({
                "name": "document_delete",
                "description": "Delete one document by collection and id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "id": { "type": "string" }
                    },
                    "required": ["collection", "id"]
                }
            }),
            json!({
                "name": "document_batch",
                "description": "Apply an atomic batch of put, patch, and delete mutations.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "mutations": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "op": { "type": "string", "enum": ["put", "patch", "delete"] },
                                    "collection": { "type": "string" },
                                    "id": { "type": "string" },
                                    "doc": { "type": "object" },
                                    "vectors": {
                                        "type": "object",
                                        "additionalProperties": {
                                            "type": "array",
                                            "items": { "type": "number" }
                                        }
                                    },
                                    "updates": { "type": "object" }
                                },
                                "required": ["op", "collection"]
                            }
                        }
                    },
                    "required": ["mutations"]
                }
            }),
            json!({
                "name": "aggregate",
                "description": "Run count, sum, or avg over a collection query.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "filters": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "any": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "elem_match": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "filters": {
                                        "type": "array",
                                        "items": { "$ref": "#/$defs/filter" }
                                    }
                                },
                                "required": ["field", "filters"]
                            }
                        },
                        "op": { "type": "string", "enum": ["count", "sum", "avg"] },
                        "field": { "type": "string" }
                    },
                    "required": ["collection", "op"],
                    "$defs": {
                        "filter": {
                            "type": "object",
                            "properties": {
                                "field": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": [
                                        "eq", "ne", "gt", "gte", "lt", "lte",
                                        "match", "match_prefix", "contains",
                                        "starts_with", "in", "array_contains",
                                        "array_contains_any", "array_all_eq", "not_in"
                                    ]
                                },
                                "value": {}
                            },
                            "required": ["field", "op", "value"]
                        }
                    }
                }
            }),
            json!({
                "name": "explain",
                "description": "Return the engine query plan and the indexes available for a query.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "filters": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "any": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "elem_match": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "filters": {
                                        "type": "array",
                                        "items": { "$ref": "#/$defs/filter" }
                                    }
                                },
                                "required": ["field", "filters"]
                            }
                        },
                        "order_by": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "direction": { "type": "string", "enum": ["asc", "desc"] }
                                },
                                "required": ["field"]
                            }
                        },
                        "limit": { "type": "integer", "minimum": 0 },
                        "offset": { "type": "integer", "minimum": 0 },
                        "select": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["collection"],
                    "$defs": {
                        "filter": {
                            "type": "object",
                            "properties": {
                                "field": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": [
                                        "eq", "ne", "gt", "gte", "lt", "lte",
                                        "match", "match_prefix", "contains",
                                        "starts_with", "in", "array_contains",
                                        "array_contains_any", "array_all_eq", "not_in"
                                    ]
                                },
                                "value": {}
                            },
                            "required": ["field", "op", "value"]
                        }
                    }
                }
            }),
            json!({
                "name": "index_list",
                "description": "List simple, secondary, FTS, composite, vector, and traverse indexes.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "job_list",
                "description": "List recent background jobs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "job_get",
                "description": "Return one background job by id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"]
                }
            }),
            json!({
                "name": "job_cancel",
                "description": "Cancel a job when the job declares itself cancellable.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"]
                }
            }),
            json!({
                "name": "compact",
                "description": "Start a background full-database compaction job.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "vacuum",
                "description": "Start a background vacuum job for one collection or all allowed collections.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "flush",
                "description": "Flush WAL and blobs to disk.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "documents_delete_where",
                "description": "Count or delete documents matching a query. Always dry-run first; deletion needs --allow-destructive.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "filters": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "any": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "elem_match": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "filters": {
                                        "type": "array",
                                        "items": { "$ref": "#/$defs/filter" }
                                    }
                                },
                                "required": ["field", "filters"]
                            }
                        },
                        "dry_run": { "type": "boolean" }
                    },
                    "required": ["collection", "dry_run"],
                    "$defs": {
                        "filter": {
                            "type": "object",
                            "properties": {
                                "field": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": [
                                        "eq", "ne", "gt", "gte", "lt", "lte",
                                        "match", "match_prefix", "contains",
                                        "starts_with", "in", "array_contains",
                                        "array_contains_any", "array_all_eq", "not_in"
                                    ]
                                },
                                "value": {}
                            },
                            "required": ["field", "op", "value"]
                        }
                    }
                }
            }),
            json!({
                "name": "watch_create",
                "description": "Create a filtered watch for an allowlisted collection. Notifications arrive on hakodb://watches/{id}.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "filters": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "any": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/filter" }
                        },
                        "elem_match": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": { "type": "string" },
                                    "filters": {
                                        "type": "array",
                                        "items": { "$ref": "#/$defs/filter" }
                                    }
                                },
                                "required": ["field", "filters"]
                            }
                        }
                    },
                    "required": ["collection"],
                    "$defs": {
                        "filter": {
                            "type": "object",
                            "properties": {
                                "field": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": [
                                        "eq", "ne", "gt", "gte", "lt", "lte",
                                        "match", "match_prefix", "contains",
                                        "starts_with", "in", "array_contains",
                                        "array_contains_any", "array_all_eq", "not_in"
                                    ]
                                },
                                "value": {}
                            },
                            "required": ["field", "op", "value"]
                        }
                    }
                }
            }),
            json!({
                "name": "watch_delete",
                "description": "Remove a filtered watch by id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"]
                }
            }),
            json!({
                "name": "sync_start",
                "description": "Start engine-native mesh or cloud sync. Secrets come from the environment.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "mode": { "type": "string", "enum": ["mesh", "cloud_server", "cloud_client"] },
                        "node_id": { "type": "string" },
                        "room": { "type": "string" },
                        "port": { "type": "integer", "minimum": 1 },
                        "bind": { "type": "string" },
                        "server_url": { "type": "string" },
                        "static_peers": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "discovery": { "type": "string", "enum": ["mdns", "broadcast", "both", "off"] },
                        "relay": { "type": "boolean" },
                        "max_clients": { "type": "integer", "minimum": 0 }
                    },
                    "required": ["mode", "node_id", "room"]
                }
            }),
            json!({
                "name": "sync_stop",
                "description": "Stop the active sync controller.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "sync_status",
                "description": "Return redacted sync status. Secrets are never included.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "sync_peers",
                "description": "Return connected peer identities and mesh dial state.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "sync_versions",
                "description": "Return the per-collection version map used by catch-up.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "server_status",
                "description": "Return hakomcp version, engine, profile, limits, and subscription state.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }),
        ];
        tools.retain(|tool| {
            tool["name"]
                .as_str()
                .map(|name| self.config.profile.allows_tool(name))
                .unwrap_or(false)
        });
        json!({ "tools": tools })
    }

    fn tools_call(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        let requested = params
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing tool name"))?;
        let name = canonical_tool_name(requested);

        let known = matches!(
            name,
            "document_get"
                | "document_put"
                | "document_patch"
                | "document_delete"
                | "document_batch"
                | "query"
                | "aggregate"
                | "explain"
                | "index_create"
                | "index_list"
                | "collection_list"
                | "collection_describe"
                | "collection_stats"
                | "schema_set"
                | "schema_clear"
                | "find_near"
                | "find_paths"
                | "job_list"
                | "job_get"
                | "job_cancel"
                | "compact"
                | "vacuum"
                | "flush"
                | "documents_delete_where"
                | "watch_create"
                | "watch_delete"
                | "sync_start"
                | "sync_stop"
                | "sync_status"
                | "sync_peers"
                | "sync_versions"
                | "server_status"
        );
        if !known {
            return Err(RpcError::invalid(format!("unknown tool: {requested}")));
        }
        if !self.config.profile.allows_tool(name) {
            return Err(RpcError::invalid(format!(
                "tool `{requested}` is not available in profile `{}`",
                self.config.profile.as_str()
            )));
        }

        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let result = match name {
            "document_get" => self.tool_get(&args),
            "document_put" => self.tool_put(&args),
            "document_patch" => self.tool_patch(&args),
            "document_delete" => self.tool_delete(&args),
            "document_batch" => self.tool_batch(&args),
            "query" => self.tool_query(&args),
            "aggregate" => self.tool_aggregate(&args),
            "explain" => self.tool_explain(&args),
            "index_create" => self.tool_create_index(&args),
            "index_list" => self.tool_index_list(&args),
            "collection_list" => self.tool_collection_list(),
            "collection_describe" => self.tool_collection_describe(&args),
            "collection_stats" => self.tool_collection_stats(&args),
            "schema_set" => self.tool_schema_set(&args),
            "schema_clear" => self.tool_schema_clear(&args),
            "find_near" => self.tool_find_near(&args),
            "find_paths" => self.tool_find_paths(&args),
            "job_list" => self.tool_job_list(),
            "job_get" => self.tool_job_get(&args),
            "job_cancel" => self.tool_job_cancel(&args),
            "compact" => self.tool_compact(),
            "vacuum" => self.tool_vacuum(&args),
            "flush" => self.tool_flush(),
            "documents_delete_where" => self.tool_delete_where(&args),
            "watch_create" => self.tool_watch_create(&args),
            "watch_delete" => self.tool_watch_delete(&args),
            "sync_start" => self.tool_sync_start(&args),
            "sync_stop" => self.tool_sync_stop(),
            "sync_status" => self.tool_sync_status(),
            "sync_peers" => self.tool_sync_peers(),
            "sync_versions" => self.tool_sync_versions(),
            "server_status" => self.tool_server_status(),
            _ => unreachable!("known tool handled above"),
        };

        Ok(match result {
            Ok(value) => self.tool_success(value),
            Err(error) => tool_error(error.to_string()),
        })
    }

    fn ensure_collection(&self, collection: &str) -> Result<()> {
        if !self.config.collection_allowed(collection) {
            bail!("collection `{collection}` is not accessible");
        }
        Ok(())
    }

    fn tool_get(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let id = required_str(args, "id")?;
        let doc = self.db.get(collection, id)?;
        Ok(match doc {
            Some(doc) => json!({
                "found": true,
                "document": doc.to_json_with_id(id)
            }),
            None => json!({ "found": false, "document": null }),
        })
    }

    fn tool_put(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let doc = doc_from_args(args)?;
        let id = match opt_str(args, "id")? {
            Some(id) => id.to_string(),
            None => generate_id(),
        };
        let stored = self.db.put(collection, &id, &doc)?;
        Ok(json!({
            "ok": true,
            "collection": collection,
            "id": stored
        }))
    }

    fn tool_patch(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let id = required_str(args, "id")?;
        let updates = updates_from_args(args)?;
        let stored = self.db.patch(collection, id, updates)?;
        Ok(json!({ "ok": true, "collection": collection, "id": stored }))
    }

    fn tool_delete(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let id = required_str(args, "id")?;
        let stored = self.db.delete(collection, id)?;
        Ok(json!({ "ok": true, "collection": collection, "id": stored }))
    }

    fn tool_batch(&self, args: &JsonValue) -> Result<JsonValue> {
        let mutations = args
            .get("mutations")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("`mutations` must be an array"))?;
        if mutations.is_empty() {
            bail!("`mutations` must not be empty");
        }

        let mut batch = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let collection = required_str(mutation, "collection")?;
            self.ensure_collection(collection)?;
            let op = required_str(mutation, "op")?;
            match op {
                "put" => {
                    let doc = doc_from_args(mutation)?;
                    let doc_id = match opt_str(mutation, "id")? {
                        Some(id) => id.to_string(),
                        None => generate_id(),
                    };
                    batch.push(BatchMutation::Put {
                        collection: collection.to_string(),
                        doc_id,
                        doc,
                    });
                }
                "patch" => {
                    let doc_id = required_str(mutation, "id")?.to_string();
                    let updates = updates_from_args(mutation)?;
                    batch.push(BatchMutation::Patch {
                        collection: collection.to_string(),
                        doc_id,
                        updates,
                    });
                }
                "delete" => {
                    let doc_id = required_str(mutation, "id")?.to_string();
                    batch.push(BatchMutation::Delete {
                        collection: collection.to_string(),
                        doc_id,
                    });
                }
                other => bail!("`op` must be put, patch, or delete, got `{other}`"),
            }
        }

        let ids = self.db.write_batch(batch)?;
        Ok(json!({ "ok": true, "count": ids.len(), "ids": ids }))
    }

    fn tool_collection_list(&self) -> Result<JsonValue> {
        let stats = self.db.get_stats();
        let collections: Vec<JsonValue> = self
            .db
            .list_collections()?
            .into_iter()
            .filter(|collection| self.config.collection_allowed(collection))
            .map(|collection| {
                let count = stats
                    .get(&format!("{collection}_count"))
                    .copied()
                    .unwrap_or(0);
                json!({ "name": collection, "count": count })
            })
            .collect();
        Ok(json!({ "collections": collections }))
    }

    fn tool_collection_describe(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        self.ensure_collection_exists(collection)?;
        let schema = self
            .db
            .schema(collection)
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| anyhow!(error))?;
        let indexes = serde_json::to_value(self.db.list_indexes(Some(collection)))
            .map_err(|error| anyhow!(error))?;
        let stats = stats_to_json(&self.db.collection_stats(collection)?);
        Ok(json!({
            "collection": collection,
            "schema": schema,
            "indexes": indexes,
            "stats": stats
        }))
    }

    fn tool_collection_stats(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        self.ensure_collection_exists(collection)?;
        let stats = stats_to_json(&self.db.collection_stats(collection)?);
        Ok(json!({ "collection": collection, "stats": stats }))
    }

    fn tool_schema_set(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let schema = args
            .get("schema")
            .ok_or_else(|| anyhow!("missing `schema`"))?;
        let schema: CollectionSchema = serde_json::from_value(schema.clone())
            .map_err(|error| anyhow!("invalid schema: {error}"))?;
        self.db.set_schema(collection, schema)?;
        Ok(json!({ "ok": true, "collection": collection }))
    }

    fn tool_schema_clear(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        self.db.clear_schema(collection)?;
        Ok(json!({ "ok": true, "collection": collection }))
    }

    fn ensure_collection_exists(&self, collection: &str) -> Result<()> {
        if !self
            .db
            .list_collections()?
            .iter()
            .any(|name| name == collection)
        {
            bail!("collection `{collection}` does not exist");
        }
        Ok(())
    }

    fn tool_query(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let query = query_with_order_select(query_with_filters(collection, args)?, args)?;

        let requested_limit = opt_usize(args, "limit", 50)?;
        let limit = requested_limit.min(self.config.max_query_rows);
        let offset = cursor_offset(args, opt_usize(args, "offset", 0)?)?;
        let query = query.limit(limit.saturating_add(1)).offset(offset);

        let mut rows = self.db.query(query)?;
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let documents: Vec<JsonValue> = rows
            .iter()
            .map(|(id, doc)| doc.to_json_with_id(id))
            .collect();
        let next_cursor = has_more.then(|| (offset + documents.len()).to_string());
        Ok(json!({
            "collection": collection,
            "count": documents.len(),
            "limit": limit,
            "next_cursor": next_cursor,
            "documents": documents
        }))
    }

    fn tool_aggregate(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let op = required_str(args, "op")?;
        let mut query = query_with_filters(collection, args)?;
        query = match op {
            "count" => query.aggregate(AggregateOp::Count),
            "sum" => query.aggregate(AggregateOp::Sum(required_str(args, "field")?.to_string())),
            "avg" => query.aggregate(AggregateOp::Avg(required_str(args, "field")?.to_string())),
            other => bail!("`op` must be count, sum, or avg, got `{other}`"),
        };
        let results = self.db.execute_aggregation(query)?;
        Ok(json!({ "collection": collection, "op": op, "results": results }))
    }

    fn tool_explain(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let query = query_with_order_select(query_with_filters(collection, args)?, args)?;
        let report = self.db.explain_report(query)?;
        let plan = serde_json::to_value(&report).map_err(|error| anyhow!(error))?;
        Ok(json!({ "collection": collection, "plan": plan }))
    }

    fn tool_create_index(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let kind = required_str(args, "kind")?;
        match kind {
            "secondary" => {
                let field = required_str(args, "field")?;
                self.db.create_index(collection, field)?;
                Ok(json!({ "ok": true, "collection": collection, "field": field, "kind": kind }))
            }
            "fts" => {
                let field = required_str(args, "field")?;
                self.db.create_fts_index(collection, field)?;
                Ok(json!({ "ok": true, "collection": collection, "field": field, "kind": kind }))
            }
            "vector" => {
                let field = required_str(args, "field")?;
                let dim = required_u32(args, "dim")?;
                let metric = match opt_str(args, "metric")?.unwrap_or("cosine") {
                    "cosine" => Metric::Cosine,
                    "l2" => Metric::L2,
                    other => bail!("`metric` must be cosine or l2, got `{other}`"),
                };
                self.db
                    .create_vector_index(collection, field, dim, metric)?;
                Ok(json!({ "ok": true, "collection": collection, "field": field, "kind": kind }))
            }
            "composite" => {
                let fields = composite_fields(args)?;
                let unique = args
                    .get("unique")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                let sparse = args
                    .get("sparse")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(true);
                let index_id = self
                    .db
                    .create_composite_index_ex(collection, fields, unique, sparse, None)?;
                Ok(json!({
                    "ok": true,
                    "collection": collection,
                    "kind": kind,
                    "index_id": index_id
                }))
            }
            "edge" => {
                self.db.create_edge_index(collection)?;
                Ok(json!({ "ok": true, "collection": collection, "kind": kind }))
            }
            "traverse" => {
                self.db.create_traverse_index(collection)?;
                Ok(json!({ "ok": true, "collection": collection, "kind": kind }))
            }
            other => bail!(
                "`kind` must be secondary, fts, vector, composite, edge, or traverse, got `{other}`"
            ),
        }
    }

    fn tool_index_list(&self, args: &JsonValue) -> Result<JsonValue> {
        if let Some(collection) = opt_str(args, "collection")? {
            self.ensure_collection(collection)?;
            let indexes = self.db.list_indexes(Some(collection));
            return serde_json::to_value(indexes).map_err(|error| anyhow!(error));
        }

        let mut value =
            serde_json::to_value(self.db.list_indexes(None)).map_err(|error| anyhow!(error))?;
        filter_index_map(&mut value, "simple", &|name| {
            self.config.collection_allowed(name)
        });
        filter_index_map(&mut value, "secondary", &|name| {
            self.config.collection_allowed(name)
        });
        filter_index_map(&mut value, "fts", &|name| {
            self.config.collection_allowed(name)
        });
        filter_index_map(&mut value, "vector", &|name| {
            self.config.collection_allowed(name)
        });
        if let Some(entries) = value.get_mut("composite").and_then(JsonValue::as_array_mut) {
            entries.retain(|entry| {
                entry["collection"]
                    .as_str()
                    .map(|name| self.config.collection_allowed(name))
                    .unwrap_or(false)
            });
        }
        if let Some(entries) = value.get_mut("traverse").and_then(JsonValue::as_array_mut) {
            entries.retain(|entry| {
                entry
                    .as_str()
                    .map(|name| self.config.collection_allowed(name))
                    .unwrap_or(false)
            });
        }
        Ok(value)
    }

    fn ensure_subscribe_collection(&self, collection: &str) -> Result<()> {
        if !self.config.subscriptions_enabled() {
            bail!("subscriptions are disabled; set --subscribe-to to opt in");
        }
        if !pattern_match(&self.config.subscribe_to, collection) {
            bail!("collection `{collection}` is not in --subscribe-to");
        }
        Ok(())
    }

    fn ensure_collection_watcher(&self, collection: &str) {
        let mut started = self.watch_collections.lock().unwrap();
        if !started.insert(collection.to_string()) {
            return;
        }
        drop(started);

        // Register before returning so a write that follows the subscribe
        // response cannot race the watcher thread startup.
        let receiver = self.db.watch_collection(collection);
        let db = self.db.clone();
        let notifier = self.notifier.clone();
        let subscriptions = self.subscriptions.clone();
        let watches = self.watches.clone();
        let shutdown = self.shutdown.clone();
        let collection = collection.to_string();
        std::thread::spawn(move || {
            watcher_loop(
                receiver,
                db,
                notifier,
                subscriptions,
                watches,
                shutdown,
                collection,
            );
        });
    }

    fn resources_subscribe(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        if !self.config.subscriptions_enabled() {
            return Err(RpcError::invalid(
                "subscriptions are disabled; set --subscribe-to to opt in",
            ));
        }
        let uri = params
            .get("uri")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing resource `uri`"))?;
        let collection = uri
            .strip_prefix(RESOURCE_PREFIX)
            .filter(|name| !name.is_empty() && !name.contains('/'))
            .ok_or_else(|| RpcError::invalid("only collection resources can be subscribed"))?;
        if !self.config.collection_allowed(collection)
            || !pattern_match(&self.config.subscribe_to, collection)
        {
            return Err(RpcError::resource_not_found(uri));
        }
        self.subscriptions.lock().unwrap().insert(uri.to_string());
        self.ensure_collection_watcher(collection);
        Ok(json!({}))
    }

    fn resources_unsubscribe(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        let uri = params
            .get("uri")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing resource `uri`"))?;
        self.subscriptions.lock().unwrap().remove(uri);
        Ok(json!({}))
    }

    fn tool_watch_create(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        self.ensure_subscribe_collection(collection)?;
        let query = query_with_filters(collection, args)?;
        let plan = self.db.plan_for_watch(&query);
        let id = format!(
            "watch-{}",
            self.next_watch_id.fetch_add(1, Ordering::Relaxed)
        );
        let uri = format!("hakodb://watches/{id}");
        self.watches.lock().unwrap().insert(
            id.clone(),
            Watch {
                id: id.clone(),
                collection: collection.to_string(),
                plan,
                matched: HashSet::new(),
            },
        );
        self.ensure_collection_watcher(collection);
        Ok(json!({ "ok": true, "id": id, "uri": uri }))
    }

    fn tool_watch_delete(&self, args: &JsonValue) -> Result<JsonValue> {
        let id = required_str(args, "id")?;
        let removed = self.watches.lock().unwrap().remove(id).is_some();
        if !removed {
            bail!("unknown watch `{id}`");
        }
        Ok(json!({ "ok": true, "id": id }))
    }

    fn tool_job_list(&self) -> Result<JsonValue> {
        let jobs: Vec<JsonValue> = self.jobs.list().iter().map(JobRecord::to_json).collect();
        Ok(json!({ "jobs": jobs }))
    }

    fn tool_job_get(&self, args: &JsonValue) -> Result<JsonValue> {
        let id = required_str(args, "id")?;
        let job = self
            .jobs
            .get(id)
            .ok_or_else(|| anyhow!("unknown job `{id}`"))?;
        Ok(job.to_json())
    }

    fn tool_job_cancel(&self, args: &JsonValue) -> Result<JsonValue> {
        let id = required_str(args, "id")?;
        self.jobs.cancel(id).map_err(|error| anyhow!(error))?;
        Ok(json!({ "ok": true, "id": id }))
    }

    fn tool_compact(&self) -> Result<JsonValue> {
        let id = self.jobs.create("compact", false);
        let jobs = self.jobs.clone();
        let db = self.db.clone();
        let job_id = id.clone();
        std::thread::spawn(move || {
            jobs.start(&job_id);
            match db.compact() {
                Ok(()) => jobs.succeed(&job_id, json!({ "ok": true })),
                Err(error) => jobs.fail(&job_id, error.to_string()),
            }
        });
        Ok(json!({ "ok": true, "job_id": id }))
    }

    fn tool_vacuum(&self, args: &JsonValue) -> Result<JsonValue> {
        let collections: Vec<String> = match opt_str(args, "collection")? {
            Some(collection) => {
                self.ensure_collection(collection)?;
                vec![collection.to_string()]
            }
            None => self
                .db
                .list_collections()?
                .into_iter()
                .filter(|collection| self.config.collection_allowed(collection))
                .collect(),
        };
        if collections.is_empty() {
            bail!("no collections to vacuum");
        }

        let id = self.jobs.create("vacuum", false);
        let jobs = self.jobs.clone();
        let db = self.db.clone();
        let job_id = id.clone();
        std::thread::spawn(move || {
            jobs.start(&job_id);
            let total = collections.len();
            for (index, collection) in collections.iter().enumerate() {
                if let Err(error) = db.vacuum(collection) {
                    jobs.fail(&job_id, error.to_string());
                    return;
                }
                jobs.progress(&job_id, (index + 1) as f64 / total as f64);
            }
            jobs.succeed(&job_id, json!({ "ok": true, "collections": total }));
        });
        Ok(json!({ "ok": true, "job_id": id }))
    }

    fn tool_flush(&self) -> Result<JsonValue> {
        self.db.flush()?;
        Ok(json!({ "ok": true }))
    }

    fn tool_delete_where(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let dry_run = args
            .get("dry_run")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| anyhow!("`dry_run` must be a boolean"))?;
        let query = query_with_filters(collection, args)?;

        if dry_run {
            let count = self
                .db
                .execute_aggregation(query.aggregate(AggregateOp::Count))?;
            let count = count.get("count").copied().unwrap_or(0.0) as u64;
            return Ok(json!({ "dry_run": true, "count": count }));
        }
        if !self.config.allow_destructive {
            bail!("actual deletion requires --allow-destructive");
        }

        let id = self.jobs.create("documents_delete_where", false);
        let jobs = self.jobs.clone();
        let db = self.db.clone();
        let job_id = id.clone();
        std::thread::spawn(move || {
            jobs.start(&job_id);
            match db.delete_where(query) {
                Ok(deleted) => jobs.succeed(&job_id, json!({ "deleted": deleted })),
                Err(error) => jobs.fail(&job_id, error.to_string()),
            }
        });
        Ok(json!({ "ok": true, "job_id": id }))
    }

    fn require_network(&self) -> Result<()> {
        if !self.config.allow_network {
            bail!("--allow-network is required to start sync");
        }
        Ok(())
    }

    fn tool_sync_start(&self, args: &JsonValue) -> Result<JsonValue> {
        self.require_network()?;
        if self.sync.lock().unwrap().is_some() {
            bail!("sync is already running; call sync_stop first");
        }
        let mode = required_str(args, "mode")?;
        let node_id = required_str(args, "node_id")?;
        let room = required_str(args, "room")?;
        let room_key = secret_env("HAKOMCP_SYNC_ROOM_KEY")?;
        match mode {
            "mesh" => self.start_mesh(args, node_id, room, &room_key),
            "cloud_server" => self.start_cloud_server(args, node_id, room, &room_key),
            "cloud_client" => self.start_cloud_client(args, node_id, room, &room_key),
            other => bail!("`mode` must be mesh, cloud_server, or cloud_client, got `{other}`"),
        }
    }

    #[cfg(feature = "net-sync")]
    fn start_mesh(
        &self,
        args: &JsonValue,
        node_id: &str,
        room: &str,
        room_key: &str,
    ) -> Result<JsonValue> {
        let port = required_u32(args, "port")?;
        if port == 0 || port > u16::MAX as u32 {
            bail!("`port` must be between 1 and {}", u16::MAX);
        }
        let static_peers: Vec<String> = args
            .get("static_peers")
            .and_then(JsonValue::as_array)
            .map(|peers| {
                peers
                    .iter()
                    .map(|peer| {
                        peer.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| anyhow!("`static_peers` entries must be strings"))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let discovery = match opt_str(args, "discovery")?.unwrap_or("mdns") {
            "mdns" => DiscoveryMode::Mdns,
            "broadcast" => DiscoveryMode::Broadcast,
            "both" => DiscoveryMode::Both,
            "off" => DiscoveryMode::Off,
            other => bail!("`discovery` must be mdns, broadcast, both, or off, got `{other}`"),
        };
        let relay = args
            .get("relay")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);

        let handle = self
            .runtime
            .clone()
            .ok_or_else(|| anyhow!("no async runtime available"))?;
        let syncer = Arc::new(
            NetSyncer::new(self.db.clone(), node_id, room_key, Vec::new())
                .with_static_peers(static_peers)
                .with_relay(relay)
                .with_discovery(discovery),
        );
        *self.sync.lock().unwrap() = Some(SyncController::Mesh(syncer.clone()));
        handle.spawn(async move {
            if let Err(error) = syncer.start(port as u16).await {
                eprintln!("[hakomcp] mesh sync failed: {error}");
            }
        });
        Ok(json!({ "ok": true, "mode": "mesh", "room": room, "port": port }))
    }

    #[cfg(not(feature = "net-sync"))]
    fn start_mesh(
        &self,
        _args: &JsonValue,
        _node_id: &str,
        _room: &str,
        _room_key: &str,
    ) -> Result<JsonValue> {
        bail!("this build was compiled without the net-sync feature")
    }

    #[cfg(feature = "cloud-sync")]
    fn start_cloud_server(
        &self,
        args: &JsonValue,
        node_id: &str,
        room: &str,
        room_key: &str,
    ) -> Result<JsonValue> {
        let bind = required_str(args, "bind")?;
        let auth_token = secret_env("HAKOMCP_CLOUD_AUTH_TOKEN")?;
        let handle = self
            .runtime
            .clone()
            .ok_or_else(|| anyhow!("no async runtime available"))?;
        let controller = Arc::new(CloudSync::server(
            self.db.clone(),
            node_id,
            &auth_token,
            room,
            room_key,
        ));
        controller.set_max_clients_per_room(opt_usize(args, "max_clients", 0)?);
        *self.sync.lock().unwrap() = Some(SyncController::Cloud(controller.clone()));
        let start_bind = bind.to_string();
        let reported_bind = bind.to_string();
        handle.spawn(async move {
            if let Err(error) = controller.start(&start_bind).await {
                eprintln!("[hakomcp] cloud server sync failed: {error}");
            }
        });
        Ok(json!({ "ok": true, "mode": "cloud_server", "room": room, "bind": reported_bind }))
    }

    #[cfg(not(feature = "cloud-sync"))]
    fn start_cloud_server(
        &self,
        _args: &JsonValue,
        _node_id: &str,
        _room: &str,
        _room_key: &str,
    ) -> Result<JsonValue> {
        bail!("this build was compiled without the cloud-sync feature")
    }

    #[cfg(feature = "cloud-sync")]
    fn start_cloud_client(
        &self,
        args: &JsonValue,
        node_id: &str,
        room: &str,
        room_key: &str,
    ) -> Result<JsonValue> {
        let server_url = required_str(args, "server_url")?;
        let auth_token = secret_env("HAKOMCP_CLOUD_AUTH_TOKEN")?;
        let handle = self
            .runtime
            .clone()
            .ok_or_else(|| anyhow!("no async runtime available"))?;
        let controller = Arc::new(CloudSync::client(
            self.db.clone(),
            node_id,
            room,
            room_key,
            &auth_token,
        ));
        if let Ok(api_key) = std::env::var("HAKOMCP_CLOUD_API_KEY") {
            if !api_key.is_empty() {
                controller.set_api_key(Some(api_key));
            }
        }
        *self.sync.lock().unwrap() = Some(SyncController::Cloud(controller.clone()));
        let start_url = server_url.to_string();
        let reported_url = server_url.to_string();
        handle.spawn(async move {
            if let Err(error) = controller.start(&start_url).await {
                eprintln!("[hakomcp] cloud client sync failed: {error}");
            }
        });
        Ok(json!({ "ok": true, "mode": "cloud_client", "room": room, "server_url": reported_url }))
    }

    #[cfg(not(feature = "cloud-sync"))]
    fn start_cloud_client(
        &self,
        _args: &JsonValue,
        _node_id: &str,
        _room: &str,
        _room_key: &str,
    ) -> Result<JsonValue> {
        bail!("this build was compiled without the cloud-sync feature")
    }

    fn tool_sync_stop(&self) -> Result<JsonValue> {
        let controller = self.sync.lock().unwrap().take();
        match controller {
            Some(controller) => {
                stop_sync_controller(&controller);
                Ok(json!({ "ok": true, "active": false }))
            }
            None => Ok(json!({ "ok": true, "active": false })),
        }
    }

    fn tool_sync_status(&self) -> Result<JsonValue> {
        let guard = self.sync.lock().unwrap();
        let Some(controller) = guard.as_ref() else {
            return Ok(json!({ "active": false }));
        };
        match controller {
            #[cfg(feature = "net-sync")]
            SyncController::Mesh(syncer) => {
                let mut value =
                    serde_json::to_value(syncer.status()).map_err(|error| anyhow!(error))?;
                value["active"] = json!(true);
                value["mode"] = json!("mesh");
                value["dial"] = serde_json::to_value(syncer.mesh_dial_status())
                    .map_err(|error| anyhow!(error))?;
                Ok(value)
            }
            #[cfg(feature = "cloud-sync")]
            SyncController::Cloud(cloud) => {
                let status = cloud.status();
                let mode = match status.mode {
                    CloudSyncMode::Server => "cloud_server",
                    CloudSyncMode::Client => "cloud_client",
                };
                Ok(json!({
                    "active": true,
                    "mode": mode,
                    "connected": status.connected,
                    "room_name": status.room_name,
                    "active_clients": status.active_clients,
                    "queued_writes": status.queued_writes,
                    "peers": cloud.peer_list().len()
                }))
            }
            #[cfg(not(any(feature = "net-sync", feature = "cloud-sync")))]
            SyncController::Disabled => Ok(json!({ "active": false, "compiled": false })),
        }
    }

    fn tool_sync_peers(&self) -> Result<JsonValue> {
        let guard = self.sync.lock().unwrap();
        let Some(controller) = guard.as_ref() else {
            return Ok(json!({ "active": false, "peers": [] }));
        };
        match controller {
            #[cfg(feature = "net-sync")]
            SyncController::Mesh(syncer) => Ok(json!({
                "active": true,
                "mode": "mesh",
                "status": serde_json::to_value(syncer.status()).map_err(|error| anyhow!(error))?,
                "dial": serde_json::to_value(syncer.mesh_dial_status())
                    .map_err(|error| anyhow!(error))?
            })),
            #[cfg(feature = "cloud-sync")]
            SyncController::Cloud(cloud) => {
                let peers: Vec<String> = cloud
                    .peer_list()
                    .into_iter()
                    .map(|peer| peer.peer_key)
                    .collect();
                Ok(json!({ "active": true, "mode": "cloud", "peers": peers }))
            }
            #[cfg(not(any(feature = "net-sync", feature = "cloud-sync")))]
            SyncController::Disabled => Ok(json!({ "active": false, "peers": [] })),
        }
    }

    fn tool_sync_versions(&self) -> Result<JsonValue> {
        let versions: std::collections::HashMap<String, i64> = self
            .db
            .get_version_map()
            .into_iter()
            .filter(|(collection, _)| self.config.collection_allowed(collection))
            .collect();
        Ok(json!({ "versions": versions }))
    }

    fn tool_find_near(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let field = required_str(args, "field")?;
        let vector = args
            .get("vector")
            .ok_or_else(|| anyhow!("missing `vector`"))?;
        let query = json_to_f32s(vector)?;
        let k = opt_usize(args, "k", 10)?.min(self.config.max_query_rows);
        if k == 0 {
            bail!("`k` must be >= 1");
        }

        let metric = self.db.vector_metric(collection, field);
        let hits = self.db.find_near(collection, field, &query, k)?;
        let hits: Vec<JsonValue> = hits
            .iter()
            .map(|(id, doc)| {
                let score = metric.and_then(|metric| match doc.get(field) {
                    Some(Value::Binary(bytes)) => decode_f32s(bytes).ok().and_then(|stored| {
                        let score = distance(metric, &query, &stored);
                        score.is_finite().then_some(score)
                    }),
                    _ => None,
                });
                json!({
                    "id": id,
                    "score": score,
                    "document": doc.to_json_with_id(id)
                })
            })
            .collect();
        Ok(json!({
            "collection": collection,
            "field": field,
            "count": hits.len(),
            "hits": hits
        }))
    }

    fn tool_find_paths(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let id = required_str(args, "id")?;
        let direction = match opt_str(args, "direction")?.unwrap_or("out") {
            "out" => TraverseDir::Outgoing,
            "in" => TraverseDir::Incoming,
            "both" => TraverseDir::Both,
            other => bail!("`direction` must be out, in, or both, got `{other}`"),
        };
        let spec = TraverseSpec {
            direction,
            edge_field: opt_str(args, "edge_field")?.map(str::to_string),
            max_depth: opt_usize(args, "max_depth", 1)?,
            limit: opt_usize(args, "limit", 100)?.min(self.config.max_query_rows),
            via_edge: opt_str(args, "via_edge")?.map(str::to_string),
            weight_field: opt_str(args, "weight_field")?.map(str::to_string),
        };
        let hits = self.db.find_paths(collection, id, &spec)?;
        let hits: Vec<JsonValue> = hits
            .iter()
            .map(|hit| {
                json!({
                    "collection": hit.collection,
                    "id": hit.id,
                    "depth": hit.depth,
                    "cost": hit.cost,
                    "document": hit.doc.to_json_with_id(&hit.id)
                })
            })
            .collect();
        Ok(json!({
            "collection": collection,
            "id": id,
            "count": hits.len(),
            "hits": hits
        }))
    }

    fn tool_server_status(&self) -> Result<JsonValue> {
        Ok(json!({
            "name": "hakomcp",
            "version": env!("CARGO_PKG_VERSION"),
            "protocolVersion": PROTOCOL_VERSION,
            "engine": "hakodb",
            "profile": self.config.profile.as_str(),
            "limits": {
                "maxQueryRows": self.config.max_query_rows,
                "maxResultBytes": self.config.max_result_bytes,
                "resourceSample": RESOURCE_SAMPLE_LIMIT.min(self.config.max_query_rows)
            },
            "subscriptions": {
                "enabled": self.config.subscriptions_enabled(),
                "active": self.subscriptions.lock().unwrap().len(),
                "collections": self.config.subscribe_to
            },
            "watches": self.watches.lock().unwrap().len(),
            "jobs": self.jobs.list().len(),
            "sync": { "active": self.sync.lock().unwrap().is_some() },
            "network": { "enabled": self.config.allow_network },
            "destructive": { "enabled": self.config.allow_destructive }
        }))
    }

    fn resources_list(&self) -> RpcResult<JsonValue> {
        let collections = self.db.list_collections().map_err(RpcError::internal)?;
        let stats = self.db.get_stats();
        let mut resources: Vec<JsonValue> = collections
            .into_iter()
            .filter(|collection| self.config.collection_allowed(collection))
            .flat_map(|collection| {
                let count = stats
                    .get(&format!("{collection}_count"))
                    .copied()
                    .unwrap_or(0);
                [
                    json!({
                        "uri": format!("{RESOURCE_PREFIX}{collection}"),
                        "name": collection,
                        "description": format!("HakoDB collection ({count} documents)"),
                        "mimeType": "application/json"
                    }),
                    json!({
                        "uri": format!("{RESOURCE_PREFIX}{collection}/schema"),
                        "name": format!("{collection} schema"),
                        "description": "Declared collection schema",
                        "mimeType": "application/json"
                    }),
                    json!({
                        "uri": format!("{RESOURCE_PREFIX}{collection}/indexes"),
                        "name": format!("{collection} indexes"),
                        "description": "Collection index definitions",
                        "mimeType": "application/json"
                    }),
                    json!({
                        "uri": format!("{RESOURCE_PREFIX}{collection}/stats"),
                        "name": format!("{collection} stats"),
                        "description": "Collection size and residency statistics",
                        "mimeType": "application/json"
                    }),
                ]
            })
            .collect();
        resources.push(json!({
            "uri": "hakodb://server/status",
            "name": "server status",
            "description": "hakomcp profile, limits, and subscription state",
            "mimeType": "application/json"
        }));
        resources.push(json!({
            "uri": "hakodb://server/jobs",
            "name": "jobs",
            "description": "Recent background jobs",
            "mimeType": "application/json"
        }));
        if self.config.profile.allows_tool("sync_status") {
            resources.push(json!({
                "uri": "hakodb://sync/status",
                "name": "sync status",
                "description": "Redacted mesh or cloud sync status",
                "mimeType": "application/json"
            }));
            resources.push(json!({
                "uri": "hakodb://sync/peers",
                "name": "sync peers",
                "description": "Connected sync peers and dial state",
                "mimeType": "application/json"
            }));
            resources.push(json!({
                "uri": "hakodb://sync/versions",
                "name": "sync versions",
                "description": "Per-collection version map for catch-up",
                "mimeType": "application/json"
            }));
        }
        for watch in self.watches.lock().unwrap().values() {
            resources.push(json!({
                "uri": format!("hakodb://watches/{}", watch.id),
                "name": format!("watch {}", watch.id),
                "description": format!("Filtered watch on {}", watch.collection),
                "mimeType": "application/json"
            }));
        }
        Ok(json!({ "resources": resources }))
    }

    fn resource_json(&self, uri: &str, value: &JsonValue) -> RpcResult<JsonValue> {
        let text = serde_json::to_string_pretty(value).map_err(RpcError::internal)?;
        if text.len() > self.config.max_result_bytes {
            return Err(RpcError::internal(format!(
                "resource exceeds --max-result-bytes ({})",
                self.config.max_result_bytes
            )));
        }
        Ok(json!({
            "contents": [{
                "uri": uri,
                "mimeType": "application/json",
                "text": text
            }]
        }))
    }

    fn resources_read(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        let uri = params
            .get("uri")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing resource `uri`"))?;

        if uri == "hakodb://server/status" {
            let status = self.tool_server_status().map_err(RpcError::internal)?;
            return self.resource_json(uri, &status);
        }
        if uri == "hakodb://server/jobs" {
            let jobs = self.tool_job_list().map_err(RpcError::internal)?;
            return self.resource_json(uri, &jobs);
        }
        if let Some(id) = uri.strip_prefix("hakodb://server/jobs/") {
            let job = self
                .tool_job_get(&json!({ "id": id }))
                .map_err(RpcError::internal)?;
            return self.resource_json(uri, &job);
        }
        if let Some(id) = uri.strip_prefix("hakodb://watches/") {
            let watches = self.watches.lock().unwrap();
            let Some(watch) = watches.get(id) else {
                return Err(RpcError::resource_not_found(uri));
            };
            let value = json!({
                "id": watch.id,
                "collection": watch.collection,
                "matched": watch.matched.len()
            });
            return self.resource_json(uri, &value);
        }
        if uri.starts_with("hakodb://sync/") {
            if !self.config.profile.allows_tool("sync_status") {
                return Err(RpcError::resource_not_found(uri));
            }
            let value = match uri {
                "hakodb://sync/status" => self.tool_sync_status(),
                "hakodb://sync/peers" => self.tool_sync_peers(),
                "hakodb://sync/versions" => self.tool_sync_versions(),
                _ => return Err(RpcError::resource_not_found(uri)),
            }
            .map_err(RpcError::internal)?;
            return self.resource_json(uri, &value);
        }

        let rest = uri
            .strip_prefix(RESOURCE_PREFIX)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| RpcError::resource_not_found(uri))?;
        let (collection, kind) = match rest.split_once('/') {
            Some((collection, "schema")) => (collection, "schema"),
            Some((collection, "indexes")) => (collection, "indexes"),
            Some((collection, "stats")) => (collection, "stats"),
            Some(_) => return Err(RpcError::resource_not_found(uri)),
            None => (rest, "sample"),
        };

        if !self.config.collection_allowed(collection) {
            return Err(RpcError::resource_not_found(uri));
        }

        let collections = self.db.list_collections().map_err(RpcError::internal)?;
        if !collections.iter().any(|name| name == collection) {
            return Err(RpcError::resource_not_found(uri));
        }

        if kind != "sample" {
            let value = match kind {
                "schema" => self
                    .db
                    .schema(collection)
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(RpcError::internal)?
                    .unwrap_or(JsonValue::Null),
                "indexes" => serde_json::to_value(self.db.list_indexes(Some(collection)))
                    .map_err(RpcError::internal)?,
                "stats" => stats_to_json(
                    &self
                        .db
                        .collection_stats(collection)
                        .map_err(RpcError::internal)?,
                ),
                _ => unreachable!("kind filtered above"),
            };
            let text = serde_json::to_string_pretty(&value).map_err(RpcError::internal)?;
            if text.len() > self.config.max_result_bytes {
                return Err(RpcError::internal(format!(
                    "resource exceeds --max-result-bytes ({})",
                    self.config.max_result_bytes
                )));
            }
            return Ok(json!({
                "contents": [{
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": text
                }]
            }));
        }

        let count = self
            .db
            .get_stats()
            .get(&format!("{collection}_count"))
            .copied()
            .unwrap_or(0);
        let sample = RESOURCE_SAMPLE_LIMIT.min(self.config.max_query_rows);
        let rows = self
            .db
            .query(Query::new(collection).limit(sample))
            .map_err(RpcError::internal)?;

        let mut payload = json!({
            "collection": collection,
            "count": count,
            "returned": rows.len(),
            "truncated": count > rows.len(),
            "documents": rows
                .iter()
                .map(|(id, doc)| doc.to_json_with_id(id))
                .collect::<Vec<_>>()
        });
        let mut text = serde_json::to_string_pretty(&payload).map_err(RpcError::internal)?;
        while text.len() > self.config.max_result_bytes
            && payload["documents"]
                .as_array()
                .map(|documents| !documents.is_empty())
                .unwrap_or(false)
        {
            if let Some(documents) = payload["documents"].as_array_mut() {
                documents.pop();
            }
            let returned = payload["documents"]
                .as_array()
                .map(|documents| documents.len())
                .unwrap_or(0);
            payload["returned"] = json!(returned);
            payload["truncated"] = json!(count > returned);
            text = serde_json::to_string_pretty(&payload).map_err(RpcError::internal)?;
        }
        if text.len() > self.config.max_result_bytes {
            return Err(RpcError::internal(format!(
                "resource sample exceeds --max-result-bytes ({})",
                self.config.max_result_bytes
            )));
        }

        Ok(json!({
            "contents": [{
                "uri": uri,
                "mimeType": "application/json",
                "text": text
            }]
        }))
    }

    fn tool_success(&self, value: JsonValue) -> JsonValue {
        let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string());
        if text.len() > self.config.max_result_bytes {
            return tool_error(format!(
                "result of {} bytes exceeds --max-result-bytes ({})",
                text.len(),
                self.config.max_result_bytes
            ));
        }
        json!({ "content": [{ "type": "text", "text": text }] })
    }
}

fn success_response(id: JsonValue, result: JsonValue) -> JsonValue {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_response(id: JsonValue, code: i64, message: impl Into<String>) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() }
    })
}

fn tool_error(message: impl Into<String>) -> JsonValue {
    json!({
        "content": [{ "type": "text", "text": message.into() }],
        "isError": true
    })
}

fn parse_filter(filter: &JsonValue) -> Result<Filter> {
    let field = required_str(filter, "field")?.to_string();
    let op = parse_operator(required_str(filter, "op")?)?;
    let value = filter
        .get("value")
        .ok_or_else(|| anyhow!("filter needs `value`"))?;
    let value = Value::from_json(value.clone()).map_err(|error| anyhow!(error))?;
    Ok(Filter { field, op, value })
}

fn parse_operator(op: &str) -> Result<Operator> {
    Ok(match op {
        "eq" => Operator::Eq,
        "ne" => Operator::Ne,
        "gt" => Operator::Gt,
        "gte" => Operator::Gte,
        "lt" => Operator::Lt,
        "lte" => Operator::Lte,
        "match" => Operator::Match,
        "match_prefix" => Operator::MatchPrefix,
        "contains" => Operator::Contains,
        "starts_with" => Operator::StartsWith,
        "in" => Operator::In,
        "array_contains" => Operator::ArrayContains,
        "array_contains_any" => Operator::ArrayContainsAny,
        "array_all_eq" => Operator::ArrayAllEq,
        "not_in" => Operator::NotIn,
        other => bail!("unknown filter operator `{other}`"),
    })
}

fn canonical_tool_name(name: &str) -> &str {
    match name {
        "get" => "document_get",
        "put" => "document_put",
        "create_index" => "index_create",
        other => other,
    }
}

fn doc_from_args(args: &JsonValue) -> Result<HakoDoc> {
    let fields = args
        .get("doc")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("`doc` must be an object"))?;
    let mut doc = HakoDoc::default();
    for (field, value) in fields {
        let value = Value::from_json(value.clone()).map_err(|error| anyhow!(error))?;
        doc.insert(field.clone(), value);
    }
    if let Some(vectors) = args.get("vectors") {
        let vectors = vectors
            .as_object()
            .ok_or_else(|| anyhow!("`vectors` must be an object"))?;
        for (field, vector) in vectors {
            let vector = json_to_f32s(vector)?;
            doc.insert(field.clone(), Value::Binary(encode_f32s(&vector)));
        }
    }
    Ok(doc)
}

fn updates_from_args(args: &JsonValue) -> Result<Vec<(String, Value)>> {
    let updates = args
        .get("updates")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("`updates` must be an object"))?;
    updates
        .iter()
        .map(|(field, value)| {
            Value::from_json(value.clone())
                .map(|value| (field.clone(), value))
                .map_err(|error| anyhow!(error))
        })
        .collect()
}

fn query_with_filters(collection: &str, args: &JsonValue) -> Result<Query> {
    let mut query = Query::new(collection);
    if let Some(filters) = args.get("filters") {
        let filters = filters
            .as_array()
            .ok_or_else(|| anyhow!("`filters` must be an array"))?;
        for filter in filters {
            let filter = parse_filter(filter)?;
            query = query.where_filter(&filter.field, filter.op, filter.value);
        }
    }
    if let Some(filters) = args.get("any") {
        let filters = filters
            .as_array()
            .ok_or_else(|| anyhow!("`any` must be an array"))?;
        for filter in filters {
            let filter = parse_filter(filter)?;
            query = query.or_where(&filter.field, filter.op, filter.value);
        }
    }
    if let Some(clauses) = args.get("elem_match") {
        let clauses = clauses
            .as_array()
            .ok_or_else(|| anyhow!("`elem_match` must be an array"))?;
        for clause in clauses {
            let field = required_str(clause, "field")?;
            let filters = clause
                .get("filters")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| anyhow!("`elem_match.filters` must be an array"))?;
            let filters = filters
                .iter()
                .map(parse_filter)
                .collect::<Result<Vec<_>>>()?;
            query = query.elem_match(field, filters);
        }
    }
    Ok(query)
}

fn query_with_order_select(mut query: Query, args: &JsonValue) -> Result<Query> {
    if let Some(order_by) = args.get("order_by") {
        let order_by = order_by
            .as_array()
            .ok_or_else(|| anyhow!("`order_by` must be an array"))?;
        for order in order_by {
            let field = required_str(order, "field")?;
            let ascending = match opt_str(order, "direction")?.unwrap_or("asc") {
                "asc" => true,
                "desc" => false,
                other => bail!("`direction` must be asc or desc, got `{other}`"),
            };
            query = query.order_by(field, ascending);
        }
    }
    if let Some(select) = args.get("select") {
        let select = select
            .as_array()
            .ok_or_else(|| anyhow!("`select` must be an array"))?;
        let fields = select
            .iter()
            .map(|field| {
                field
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("`select` entries must be strings"))
            })
            .collect::<Result<Vec<_>>>()?;
        query = query.select_fields(fields);
    }
    Ok(query)
}

fn composite_fields(args: &JsonValue) -> Result<Vec<(String, SortDirection)>> {
    let fields = args
        .get("fields")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("composite indexes need `fields`"))?;
    if fields.is_empty() {
        bail!("composite `fields` must not be empty");
    }
    fields
        .iter()
        .map(|entry| {
            let field = required_str(entry, "field")?.to_string();
            let direction = match opt_str(entry, "direction")?.unwrap_or("asc") {
                "asc" => SortDirection::Asc,
                "desc" => SortDirection::Desc,
                other => bail!("`direction` must be asc or desc, got `{other}`"),
            };
            Ok((field, direction))
        })
        .collect()
}

fn cursor_offset(args: &JsonValue, offset: usize) -> Result<usize> {
    match opt_str(args, "cursor")? {
        Some(cursor) => {
            if args.get("offset").is_some() {
                bail!("use either `cursor` or `offset`, not both");
            }
            cursor
                .parse::<usize>()
                .map_err(|_| anyhow!("`cursor` must be a non-negative integer offset"))
        }
        None => Ok(offset),
    }
}

fn filter_index_map(value: &mut JsonValue, key: &str, allowed: &dyn Fn(&str) -> bool) {
    if let Some(map) = value.get_mut(key).and_then(JsonValue::as_object_mut) {
        map.retain(|name, _| allowed(name.as_str()));
    }
}

fn stats_to_json(stats: &CollectionStats) -> JsonValue {
    json!({
        "collection": stats.collection,
        "doc_count": stats.doc_count,
        "live_bytes": stats.live_bytes,
        "total_bytes": stats.total_bytes,
        "segment_count": stats.segment_count,
        "wal_bytes": stats.wal_bytes,
        "blob_bytes": stats.blob_bytes,
        "tombstone_count": stats.tombstone_count,
        "unloaded": stats.unloaded
    })
}

fn required_str<'a>(value: &'a JsonValue, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("missing string argument `{key}`"))
}

fn opt_str<'a>(value: &'a JsonValue, key: &str) -> Result<Option<&'a str>> {
    match value.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(|| anyhow!("`{key}` must be a string")),
    }
}

fn opt_usize(value: &JsonValue, key: &str, default: usize) -> Result<usize> {
    match value.get(key) {
        None | Some(JsonValue::Null) => Ok(default),
        Some(value) => value
            .as_u64()
            .map(|number| number as usize)
            .ok_or_else(|| anyhow!("`{key}` must be a non-negative integer")),
    }
}

fn required_u32(value: &JsonValue, key: &str) -> Result<u32> {
    let number = value
        .get(key)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| anyhow!("missing integer argument `{key}`"))?;
    u32::try_from(number).map_err(|_| anyhow!("`{key}` is too large"))
}

fn json_to_f32s(value: &JsonValue) -> Result<Vec<f32>> {
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("vector must be an array of numbers"))?;
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| anyhow!("vector entries must be numbers"))?;
        if !number.is_finite() {
            bail!("vector entries must be finite");
        }
        let number = number as f32;
        if !number.is_finite() {
            bail!("vector entry out of f32 range");
        }
        out.push(number);
    }
    Ok(out)
}

fn generate_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:032x}")
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn change_kind(kind: &ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Put => "put",
        ChangeKind::Delete => "delete",
    }
}

fn secret_env(name: &str) -> Result<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => bail!("secret `{name}` is empty"),
        Err(_) => bail!("missing secret `{name}`; set it in the environment"),
    }
}

fn stop_sync_controller(controller: &SyncController) {
    match controller {
        #[cfg(feature = "net-sync")]
        SyncController::Mesh(syncer) => syncer.stop(),
        #[cfg(feature = "cloud-sync")]
        SyncController::Cloud(cloud) => cloud.stop(),
        #[cfg(not(any(feature = "net-sync", feature = "cloud-sync")))]
        SyncController::Disabled => {}
    }
}

fn watcher_loop(
    receiver: std::sync::mpsc::Receiver<ChangeEvent>,
    db: Arc<Hako>,
    notifier: Option<tokio::sync::mpsc::Sender<JsonValue>>,
    subscriptions: Arc<Mutex<HashSet<String>>>,
    watches: Arc<Mutex<HashMap<String, Watch>>>,
    shutdown: Arc<AtomicBool>,
    collection: String,
) {
    let subscription_uri = format!("{RESOURCE_PREFIX}{collection}");
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let event = match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(event) => event,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let subscribed = subscriptions.lock().unwrap().contains(&subscription_uri);
                let has_watch = watches
                    .lock()
                    .unwrap()
                    .values()
                    .any(|watch| watch.collection == collection);
                if !subscribed && !has_watch {
                    break;
                }
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        let mut events = vec![event];
        while !shutdown.load(Ordering::Relaxed) {
            match receiver.recv_timeout(Duration::from_millis(25)) {
                Ok(event) => events.push(event),
                Err(_) => break,
            }
        }

        if subscriptions.lock().unwrap().contains(&subscription_uri) {
            if let Some(notifier) = &notifier {
                let last = events.last().expect("events is never empty");
                let notification = json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/resources/updated",
                    "params": {
                        "uri": subscription_uri,
                        "hako": {
                            "events": events.len(),
                            "id": last.path,
                            "kind": change_kind(&last.kind)
                        }
                    }
                });
                let _ = notifier.try_send(notification);
            }
        }

        let mut updates = Vec::new();
        {
            let mut guard = watches.lock().unwrap();
            for watch in guard
                .values_mut()
                .filter(|watch| watch.collection == collection)
            {
                for event in &events {
                    let id = event.path.to_string();
                    match &event.kind {
                        ChangeKind::Put => {
                            let matched = db
                                .get_raw_bytes(&collection, &id)
                                .ok()
                                .flatten()
                                .map(|bytes| Hako::matches_watch(&id, &bytes, &watch.plan))
                                .unwrap_or(false);
                            let was_matched = watch.matched.contains(&id);
                            if matched && !was_matched {
                                watch.matched.insert(id.clone());
                                updates.push((watch.id.clone(), id, "matched"));
                            } else if !matched && was_matched {
                                watch.matched.remove(&id);
                                updates.push((watch.id.clone(), id, "unmatched"));
                            }
                        }
                        ChangeKind::Delete => {
                            if watch.matched.remove(&id) {
                                updates.push((watch.id.clone(), id, "deleted"));
                            }
                        }
                    }
                }
            }
        }

        if let Some(notifier) = &notifier {
            for (watch_id, id, kind) in updates {
                let notification = json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/resources/updated",
                    "params": {
                        "uri": format!("hakodb://watches/{watch_id}"),
                        "hako": {
                            "collection": collection,
                            "id": id,
                            "kind": kind
                        }
                    }
                });
                let _ = notifier.try_send(notification);
            }
        }
    }
}

fn pattern_match(patterns: &[String], name: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| match pattern.strip_suffix('*') {
            Some(prefix) => name.starts_with(prefix),
            None => pattern == name,
        })
}

fn parse_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_usize(raw: &str, flag: &str) -> Result<usize> {
    raw.parse::<usize>()
        .map_err(|_| anyhow!("`{flag}` must be a non-negative integer, got `{raw}`"))
}

fn next_value(
    args: &[String],
    index: &mut usize,
    inline: Option<&str>,
    flag: &str,
) -> Result<String> {
    if let Some(value) = inline {
        if value.is_empty() {
            bail!("`{flag}` requires a value");
        }
        return Ok(value.to_string());
    }
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| anyhow!("`{flag}` requires a value"))
}

fn parse_args() -> Result<Cli> {
    let mut db = PathBuf::from("hakomcp-data");
    let mut config = ServerConfig::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        let (flag, inline) = match args[index].split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (args[index].clone(), None),
        };
        match flag.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("hakomcp {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "--allow-network" => {
                if inline.is_some() {
                    bail!("`--allow-network` takes no value");
                }
                config.allow_network = true;
                index += 1;
            }
            "--allow-destructive" => {
                if inline.is_some() {
                    bail!("`--allow-destructive` takes no value");
                }
                config.allow_destructive = true;
                index += 1;
            }
            _ => {
                let value = next_value(&args, &mut index, inline.as_deref(), &flag)?;
                match flag.as_str() {
                    "--db" => db = PathBuf::from(value),
                    "--profile" => config.profile = Profile::parse(&value)?,
                    "--subscribe-to" => config.subscribe_to = parse_list(&value),
                    "--collection-allow" => config.collection_allow = parse_list(&value),
                    "--collection-deny" => config.collection_deny = parse_list(&value),
                    "--max-query-rows" => {
                        config.max_query_rows = parse_usize(&value, "--max-query-rows")?
                    }
                    "--max-result-bytes" => {
                        config.max_result_bytes = parse_usize(&value, "--max-result-bytes")?
                    }
                    "--log-format" => config.log_format = LogFormat::parse(&value)?,
                    other => bail!("unknown argument `{other}` (try --help)"),
                }
                index += 1;
            }
        }
    }
    config.validate()?;
    Ok(Cli { db, config })
}

fn print_help() {
    println!("hakomcp {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Production MCP control plane for HakoDB over stdio.");
    println!();
    println!("Usage: hakomcp [OPTIONS]");
    println!();
    println!("  --db PATH                  database directory (default: ./hakomcp-data)");
    println!("  --profile PROFILE          read | data | sync | admin (default: data)");
    println!(
        "  --allow-network            allow sync network listeners (required by --profile sync)"
    );
    println!("  --allow-destructive        allow destructive admin operations");
    println!(
        "  --subscribe-to LIST        collection allowlist for change notifications and watches"
    );
    println!(
        "  --collection-allow LIST    only these collections are visible; supports trailing *"
    );
    println!("  --collection-deny LIST     hide these collections; supports trailing *");
    println!(
        "  --max-query-rows N         hard cap on query rows and vector k (default: {})",
        DEFAULT_MAX_QUERY_ROWS
    );
    println!(
        "  --max-result-bytes N       hard cap on one tool result (default: {})",
        DEFAULT_MAX_RESULT_BYTES
    );
    println!("  --log-format FORMAT        text | json (default: text)");
    println!("  -h, --help                 show this help");
    println!("  -V, --version");
    println!();
    println!("Sync secrets are read from the environment, never from tool arguments:");
    println!("  HAKOMCP_SYNC_ROOM_KEY      mesh and cloud room key");
    println!("  HAKOMCP_CLOUD_AUTH_TOKEN   cloud server or client auth token");
    println!("  HAKOMCP_CLOUD_API_KEY      optional cloud client group API key");
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("[hakomcp] {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = parse_args()?;
    let db = Arc::new(
        Hako::open(&cli.db, HakoConfig::default())
            .with_context(|| format!("open database at {}", cli.db.display()))?,
    );
    let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<String>(64);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<JsonValue>(64);
    let server = Arc::new(Server::new(
        db,
        cli.config.clone(),
        Some(out_tx.clone()),
        Some(tokio::runtime::Handle::current()),
    ));
    server.log(&format!("database {}", cli.db.display()));
    server.log(&format!("profile {}", cli.config.profile.as_str()));

    let worker = {
        let server = server.clone();
        tokio::task::spawn_blocking(move || {
            while let Some(line) = in_rx.blocking_recv() {
                if line.trim().is_empty() {
                    continue;
                }
                let message = match serde_json::from_str::<JsonValue>(&line) {
                    Ok(message) => message,
                    Err(error) => {
                        let response = error_response(
                            JsonValue::Null,
                            -32700,
                            format!("Parse error: {error}"),
                        );
                        if out_tx.blocking_send(response).is_err() {
                            break;
                        }
                        continue;
                    }
                };
                if let Some(response) = server.handle(&message) {
                    if out_tx.blocking_send(response).is_err() {
                        break;
                    }
                }
            }
        })
    };

    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(response) = out_rx.recv().await {
            let mut text = serde_json::to_string(&response)?;
            text.push('\n');
            stdout.write_all(text.as_bytes()).await?;
            stdout.flush().await?;
        }
        Ok::<(), anyhow::Error>(())
    });

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        if in_tx.send(line).await.is_err() {
            break;
        }
    }
    drop(in_tx);
    worker.await.context("worker task join")?;
    server.shutdown.store(true, Ordering::SeqCst);
    drop(server);
    writer.await.context("writer task join")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn temp_server(tag: &str, config: ServerConfig) -> (Arc<Server>, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("hakomcp-{tag}-{}-{nanos}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Arc::new(Hako::open(&dir, HakoConfig::default()).unwrap());
        (Arc::new(Server::new(db, config, None, None)), dir)
    }

    fn request(server: &Server, id: u64, method: &str, params: JsonValue) -> JsonValue {
        server
            .handle(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }))
            .unwrap()
    }

    fn initialize(server: &Server) {
        let init = request(
            server,
            0,
            "initialize",
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1" }
            }),
        );
        assert!(init["result"].is_object(), "{init}");
    }

    fn tool_json(response: &JsonValue) -> JsonValue {
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("missing tool text: {response}"));
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn handshake_tool_and_resource_roundtrip() {
        let (server, dir) = temp_server("roundtrip", ServerConfig::default());
        let init = request(
            &server,
            1,
            "initialize",
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1" }
            }),
        );
        assert_eq!(init["result"]["serverInfo"]["name"], "hakomcp");
        assert!(init["result"]["instructions"].is_string());

        let put = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "put",
                "arguments": {
                    "collection": "notes",
                    "id": "n1",
                    "doc": { "title": "hello", "n": 1 }
                }
            }),
        );
        assert!(put["result"].get("isError").is_none(), "{put}");
        assert_eq!(tool_json(&put)["id"], "n1");

        let get = request(
            &server,
            3,
            "tools/call",
            json!({
                "name": "get",
                "arguments": { "collection": "notes", "id": "n1" }
            }),
        );
        assert_eq!(tool_json(&get)["document"]["title"], "hello");

        let list = request(&server, 4, "resources/list", json!({}));
        assert!(list["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["uri"] == "hakodb://collections/notes"));

        let read = request(
            &server,
            5,
            "resources/read",
            json!({ "uri": "hakodb://collections/notes" }),
        );
        let payload: JsonValue =
            serde_json::from_str(read["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(payload["documents"][0]["id"], "n1");

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn vector_and_query_tools() {
        let (server, dir) = temp_server("vector", ServerConfig::default());
        initialize(&server);
        let index = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "create_index",
                "arguments": {
                    "collection": "docs",
                    "field": "embedding",
                    "kind": "vector",
                    "dim": 2,
                    "metric": "cosine"
                }
            }),
        );
        assert!(index["result"].get("isError").is_none(), "{index}");

        let put = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "put",
                "arguments": {
                    "collection": "docs",
                    "id": "d1",
                    "doc": { "title": "alpha" },
                    "vectors": { "embedding": [1.0, 0.0] }
                }
            }),
        );
        assert!(put["result"].get("isError").is_none(), "{put}");
        assert!(server
            .db
            .await_quiescent(std::time::Duration::from_secs(30)));

        let near = request(
            &server,
            3,
            "tools/call",
            json!({
                "name": "find_near",
                "arguments": {
                    "collection": "docs",
                    "field": "embedding",
                    "vector": [1.0, 0.0],
                    "k": 1
                }
            }),
        );
        let hits = tool_json(&near);
        assert_eq!(hits["hits"][0]["id"], "d1");

        let query = request(
            &server,
            4,
            "tools/call",
            json!({
                "name": "query",
                "arguments": {
                    "collection": "docs",
                    "filters": [{ "field": "title", "op": "eq", "value": "alpha" }],
                    "limit": 5
                }
            }),
        );
        assert_eq!(tool_json(&query)["count"], 1);

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_profile_hides_and_rejects_writes() {
        let (server, dir) = temp_server(
            "read-profile",
            ServerConfig {
                profile: Profile::Read,
                ..ServerConfig::default()
            },
        );
        initialize(&server);
        let tools = request(&server, 1, "tools/list", json!({}));
        assert!(!tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "put"));

        let call = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "put",
                "arguments": { "collection": "notes", "doc": { "title": "nope" } }
            }),
        );
        assert_eq!(call["error"]["code"].as_i64(), Some(-32602));

        let resources = request(&server, 3, "resources/list", json!({}));
        assert!(!resources["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["uri"]
                .as_str()
                .unwrap_or("")
                .starts_with("hakodb://sync/")));

        let sync = request(
            &server,
            4,
            "resources/read",
            json!({ "uri": "hakodb://sync/versions" }),
        );
        assert_eq!(sync["error"]["code"].as_i64(), Some(-32002));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn sync_versions_respect_collection_policy() {
        let config = ServerConfig {
            profile: Profile::Sync,
            allow_network: true,
            collection_deny: vec!["blocked".to_string()],
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("sync-policy", config);
        initialize(&server);
        server.db.put("allowed", "a1", &HakoDoc::default()).unwrap();
        server.db.put("blocked", "b1", &HakoDoc::default()).unwrap();

        let versions = tool_json(&request(
            &server,
            1,
            "tools/call",
            json!({ "name": "sync_versions", "arguments": {} }),
        ));
        assert!(versions["versions"]["allowed"].is_number());
        assert!(versions["versions"].get("blocked").is_none());

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn collection_policy_filters_resources() {
        let config = ServerConfig {
            collection_deny: vec!["blocked".to_string()],
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("policy", config);
        initialize(&server);
        server.db.put("allowed", "a1", &HakoDoc::default()).unwrap();
        server.db.put("blocked", "b1", &HakoDoc::default()).unwrap();

        let list = request(&server, 1, "resources/list", json!({}));
        let names: Vec<&str> = list["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|resource| resource["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"allowed"));
        assert!(!names.contains(&"blocked"));

        let read = request(
            &server,
            2,
            "resources/read",
            json!({ "uri": "hakodb://collections/blocked" }),
        );
        assert_eq!(read["error"]["code"].as_i64(), Some(-32002));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn query_limit_and_result_caps() {
        let config = ServerConfig {
            max_query_rows: 2,
            max_result_bytes: 4096,
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("caps", config);
        initialize(&server);
        for index in 0..5 {
            server
                .db
                .put("docs", &format!("d{index}"), &HakoDoc::default())
                .unwrap();
        }
        let query = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "query",
                "arguments": { "collection": "docs", "limit": 50 }
            }),
        );
        let payload = tool_json(&query);
        assert_eq!(payload["limit"].as_u64(), Some(2));
        assert_eq!(payload["count"].as_u64(), Some(2));

        let status = request(
            &server,
            2,
            "tools/call",
            json!({ "name": "server_status", "arguments": {} }),
        );
        let status = tool_json(&status);
        assert_eq!(status["limits"]["maxQueryRows"].as_u64(), Some(2));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn result_bytes_cap_returns_tool_error() {
        let config = ServerConfig {
            max_result_bytes: 64,
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("bytes-cap", config);
        initialize(&server);
        let mut doc = HakoDoc::default();
        doc.insert("body".to_string(), Value::String("x".repeat(512)));
        server.db.put("docs", "d1", &doc).unwrap();

        let query = request(
            &server,
            1,
            "tools/call",
            json!({ "name": "query", "arguments": { "collection": "docs" } }),
        );
        assert_eq!(query["result"]["isError"].as_bool(), Some(true));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn schema_set_describe_and_clear() {
        let (server, dir) = temp_server("schema", ServerConfig::default());
        initialize(&server);

        let set = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "schema_set",
                "arguments": {
                    "collection": "notes",
                    "schema": {
                        "fields": [{ "name": "title", "kind": "string", "required": true }]
                    }
                }
            }),
        );
        assert!(set["result"].get("isError").is_none(), "{set}");

        let missing = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "document_put",
                "arguments": { "collection": "notes", "id": "n1", "doc": { "body": "no title" } }
            }),
        );
        assert_eq!(missing["result"]["isError"].as_bool(), Some(true));

        let ok = request(
            &server,
            3,
            "tools/call",
            json!({
                "name": "document_put",
                "arguments": { "collection": "notes", "id": "n1", "doc": { "title": "hello" } }
            }),
        );
        assert!(ok["result"].get("isError").is_none(), "{ok}");

        let describe = request(
            &server,
            4,
            "tools/call",
            json!({ "name": "collection_describe", "arguments": { "collection": "notes" } }),
        );
        let describe = tool_json(&describe);
        assert_eq!(describe["schema"]["fields"][0]["name"], "title");
        assert_eq!(describe["stats"]["doc_count"].as_u64(), Some(1));

        let schema_resource = request(
            &server,
            5,
            "resources/read",
            json!({ "uri": "hakodb://collections/notes/schema" }),
        );
        let text = schema_resource["result"]["contents"][0]["text"]
            .as_str()
            .unwrap();
        assert!(text.contains("title"), "{text}");

        let clear = request(
            &server,
            6,
            "tools/call",
            json!({ "name": "schema_clear", "arguments": { "collection": "notes" } }),
        );
        assert!(clear["result"].get("isError").is_none(), "{clear}");
        let after = request(
            &server,
            7,
            "tools/call",
            json!({ "name": "collection_describe", "arguments": { "collection": "notes" } }),
        );
        assert!(tool_json(&after)["schema"].is_null());

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn patch_delete_batch_and_atomicity() {
        let (server, dir) = temp_server("batch", ServerConfig::default());
        initialize(&server);

        let put = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "put",
                "arguments": { "collection": "docs", "id": "d1", "doc": { "n": 1 } }
            }),
        );
        assert!(put["result"].get("isError").is_none(), "{put}");

        let patch = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "document_patch",
                "arguments": { "collection": "docs", "id": "d1", "updates": { "n": 2 } }
            }),
        );
        assert!(patch["result"].get("isError").is_none(), "{patch}");
        let after_patch = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "get", "arguments": { "collection": "docs", "id": "d1" } }),
        );
        assert_eq!(tool_json(&after_patch)["document"]["n"], 2);

        let batch = request(
            &server,
            4,
            "tools/call",
            json!({
                "name": "document_batch",
                "arguments": {
                    "mutations": [
                        { "op": "put", "collection": "docs", "id": "d2", "doc": { "n": 5 } },
                        { "op": "patch", "collection": "docs", "id": "d1", "updates": { "n": 3 } },
                        { "op": "delete", "collection": "docs", "id": "d2" }
                    ]
                }
            }),
        );
        assert!(batch["result"].get("isError").is_none(), "{batch}");
        let after_batch = request(
            &server,
            5,
            "tools/call",
            json!({ "name": "get", "arguments": { "collection": "docs", "id": "d1" } }),
        );
        assert_eq!(tool_json(&after_batch)["document"]["n"], 3);
        let deleted = request(
            &server,
            6,
            "tools/call",
            json!({ "name": "document_get", "arguments": { "collection": "docs", "id": "d2" } }),
        );
        assert_eq!(tool_json(&deleted)["found"], false);

        let invalid = request(
            &server,
            7,
            "tools/call",
            json!({
                "name": "document_batch",
                "arguments": {
                    "mutations": [
                        { "op": "put", "collection": "docs", "id": "d3", "doc": { "n": 9 } },
                        { "op": "nope", "collection": "docs" }
                    ]
                }
            }),
        );
        assert_eq!(invalid["result"]["isError"].as_bool(), Some(true));
        let not_written = request(
            &server,
            8,
            "tools/call",
            json!({ "name": "document_get", "arguments": { "collection": "docs", "id": "d3" } }),
        );
        assert_eq!(tool_json(&not_written)["found"], false);

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn aggregate_and_explain() {
        let (server, dir) = temp_server("aggregate", ServerConfig::default());
        initialize(&server);
        for (id, n) in [("d1", 1), ("d2", 2), ("d3", 3)] {
            let put = request(
                &server,
                0,
                "tools/call",
                json!({
                    "name": "document_put",
                    "arguments": { "collection": "docs", "id": id, "doc": { "n": n } }
                }),
            );
            assert!(put["result"].get("isError").is_none(), "{put}");
        }

        let count = request(
            &server,
            1,
            "tools/call",
            json!({ "name": "aggregate", "arguments": { "collection": "docs", "op": "count" } }),
        );
        assert_eq!(tool_json(&count)["results"]["count"].as_f64(), Some(3.0));

        let sum = request(
            &server,
            2,
            "tools/call",
            json!({ "name": "aggregate", "arguments": { "collection": "docs", "op": "sum", "field": "n" } }),
        );
        assert_eq!(tool_json(&sum)["results"]["sum_n"].as_f64(), Some(6.0));

        let avg = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "aggregate", "arguments": { "collection": "docs", "op": "avg", "field": "n" } }),
        );
        assert_eq!(tool_json(&avg)["results"]["avg_n"].as_f64(), Some(2.0));

        let explain = request(
            &server,
            4,
            "tools/call",
            json!({ "name": "explain", "arguments": { "collection": "docs" } }),
        );
        let explain = tool_json(&explain);
        assert_eq!(explain["plan"]["collection"], "docs");

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn index_create_and_list() {
        let (server, dir) = temp_server("indexes", ServerConfig::default());
        initialize(&server);
        let secondary = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "create_index",
                "arguments": { "collection": "docs", "field": "n", "kind": "secondary" }
            }),
        );
        assert!(secondary["result"].get("isError").is_none(), "{secondary}");

        let composite = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "index_create",
                "arguments": {
                    "collection": "docs",
                    "kind": "composite",
                    "fields": [{ "field": "n", "direction": "asc" }]
                }
            }),
        );
        assert!(composite["result"].get("isError").is_none(), "{composite}");

        let list = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "index_list", "arguments": { "collection": "docs" } }),
        );
        let list = tool_json(&list);
        assert!(list["secondary"]["docs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "n"));
        assert!(list["composite"]
            .as_array()
            .unwrap()
            .iter()
            .any(|index| index["collection"] == "docs"));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn query_cursor_pagination() {
        let (server, dir) = temp_server("cursor", ServerConfig::default());
        initialize(&server);
        for index in 0..5 {
            let put = request(
                &server,
                0,
                "tools/call",
                json!({
                    "name": "document_put",
                    "arguments": {
                        "collection": "docs",
                        "id": format!("d{index}"),
                        "doc": { "n": index }
                    }
                }),
            );
            assert!(put["result"].get("isError").is_none(), "{put}");
        }

        let first = request(
            &server,
            1,
            "tools/call",
            json!({ "name": "query", "arguments": { "collection": "docs", "limit": 2 } }),
        );
        let first = tool_json(&first);
        assert_eq!(first["count"].as_u64(), Some(2));
        assert_eq!(first["next_cursor"].as_str(), Some("2"));

        let second = request(
            &server,
            2,
            "tools/call",
            json!({ "name": "query", "arguments": { "collection": "docs", "limit": 2, "cursor": "2" } }),
        );
        let second = tool_json(&second);
        assert_eq!(second["count"].as_u64(), Some(2));
        assert_eq!(second["next_cursor"].as_str(), Some("4"));

        let third = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "query", "arguments": { "collection": "docs", "limit": 2, "cursor": "4" } }),
        );
        let third = tool_json(&third);
        assert_eq!(third["count"].as_u64(), Some(1));
        assert!(third["next_cursor"].is_null());

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn jobs_and_delete_where_guards() {
        let config = ServerConfig {
            profile: Profile::Admin,
            allow_destructive: false,
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("jobs", config);
        initialize(&server);
        for index in 0..3 {
            let put = request(
                &server,
                0,
                "tools/call",
                json!({
                    "name": "document_put",
                    "arguments": { "collection": "docs", "id": format!("d{index}"), "doc": { "n": index } }
                }),
            );
            assert!(put["result"].get("isError").is_none(), "{put}");
        }

        let dry = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "documents_delete_where",
                "arguments": { "collection": "docs", "dry_run": true, "filters": [{ "field": "n", "op": "gte", "value": 1 }] }
            }),
        );
        assert_eq!(tool_json(&dry)["count"].as_u64(), Some(2));

        let blocked = request(
            &server,
            2,
            "tools/call",
            json!({
                "name": "documents_delete_where",
                "arguments": { "collection": "docs", "dry_run": false, "filters": [{ "field": "n", "op": "gte", "value": 1 }] }
            }),
        );
        assert_eq!(blocked["result"]["isError"].as_bool(), Some(true));

        let compact = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "compact", "arguments": {} }),
        );
        let compact = tool_json(&compact);
        let job_id = compact["job_id"].as_str().unwrap().to_string();

        let mut state = String::from("queued");
        for _ in 0..40 {
            let job = request(
                &server,
                4,
                "tools/call",
                json!({ "name": "job_get", "arguments": { "id": job_id } }),
            );
            state = tool_json(&job)["state"].as_str().unwrap().to_string();
            if state == "succeeded" || state == "failed" {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert_eq!(state, "succeeded");

        let cancel = request(
            &server,
            5,
            "tools/call",
            json!({ "name": "job_cancel", "arguments": { "id": job_id } }),
        );
        assert_eq!(cancel["result"]["isError"].as_bool(), Some(true));

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn subscriptions_require_allowlist() {
        let config = ServerConfig {
            subscribe_to: vec!["control".to_string()],
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("subscribe", config);
        initialize(&server);

        let allowed = request(
            &server,
            1,
            "resources/subscribe",
            json!({ "uri": "hakodb://collections/control" }),
        );
        assert!(allowed["result"].is_object(), "{allowed}");

        let denied = request(
            &server,
            2,
            "resources/subscribe",
            json!({ "uri": "hakodb://collections/other" }),
        );
        assert_eq!(denied["error"]["code"].as_i64(), Some(-32002));

        let watch = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "watch_create", "arguments": { "collection": "control", "filters": [] } }),
        );
        assert!(watch["result"].get("isError").is_none(), "{watch}");

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    #[ignore = "manual baseline; run with --release --ignored --nocapture"]
    fn performance_baseline() {
        let (server, dir) = temp_server("perf", ServerConfig::default());
        initialize(&server);

        let mutations: Vec<JsonValue> = (0..1000)
            .map(|index| {
                json!({
                    "op": "put",
                    "collection": "bench",
                    "id": format!("b{index}"),
                    "doc": { "n": index, "body": "x".repeat(256) }
                })
            })
            .collect();
        let started = Instant::now();
        request(
            &server,
            1,
            "tools/call",
            json!({ "name": "document_batch", "arguments": { "mutations": mutations } }),
        );
        eprintln!("batch put 1000: {:?}", started.elapsed());

        let started = Instant::now();
        for id in 0..1000 {
            request(
                &server,
                2 + id,
                "tools/call",
                json!({ "name": "document_get", "arguments": { "collection": "bench", "id": format!("b{id}") } }),
            );
        }
        eprintln!("get 1000: {:?}", started.elapsed());

        let started = Instant::now();
        request(
            &server,
            2000,
            "resources/read",
            json!({ "uri": "hakodb://collections/bench" }),
        );
        eprintln!("resource sample: {:?}", started.elapsed());

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn concurrent_tool_calls_stay_consistent() {
        let (server, dir) = temp_server("concurrent", ServerConfig::default());
        initialize(&server);

        let mut workers = Vec::new();
        for worker in 0..4 {
            let server = server.clone();
            workers.push(std::thread::spawn(move || {
                for index in 0..25 {
                    let id = worker * 25 + index;
                    let response = server
                        .handle(&json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "method": "tools/call",
                            "params": {
                                "name": "document_put",
                                "arguments": {
                                    "collection": "notes",
                                    "id": format!("n{id}"),
                                    "doc": { "worker": worker, "index": index }
                                }
                            }
                        }))
                        .unwrap();
                    assert!(response["result"].get("isError").is_none(), "{response}");
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        let query = tool_json(&request(
            &server,
            100,
            "tools/call",
            json!({
                "name": "query",
                "arguments": { "collection": "notes", "limit": 200 }
            }),
        ));
        assert_eq!(query["count"], 100);

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn calls_before_initialize_are_rejected() {
        let (server, dir) = temp_server("lifecycle", ServerConfig::default());
        let call = request(&server, 1, "tools/list", json!({}));
        assert_eq!(call["error"]["code"].as_i64(), Some(-32002));
        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn sync_start_requires_network_permission() {
        let config = ServerConfig {
            profile: Profile::Admin,
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("sync-network", config);
        initialize(&server);
        let start = request(
            &server,
            1,
            "tools/call",
            json!({
                "name": "sync_start",
                "arguments": { "mode": "mesh", "node_id": "a", "room": "r", "port": 7001 }
            }),
        );
        assert_eq!(start["result"]["isError"].as_bool(), Some(true), "{start}");
        assert!(start["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("--allow-network"));
        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn sync_status_is_inactive_and_secret_free_without_a_controller() {
        let config = ServerConfig {
            profile: Profile::Sync,
            allow_network: true,
            ..ServerConfig::default()
        };
        let (server, dir) = temp_server("sync-status", config);
        initialize(&server);

        let status = request(
            &server,
            1,
            "tools/call",
            json!({ "name": "sync_status", "arguments": {} }),
        );
        let status = tool_json(&status);
        assert_eq!(status["active"], false);
        assert!(status.get("room_key").is_none());
        assert!(status.get("auth_token").is_none());

        let read = request(
            &server,
            2,
            "resources/read",
            json!({ "uri": "hakodb://sync/status" }),
        );
        let payload: JsonValue =
            serde_json::from_str(read["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(payload["active"], false);
        assert!(payload.get("room_key").is_none());

        let stop = request(
            &server,
            3,
            "tools/call",
            json!({ "name": "sync_stop", "arguments": {} }),
        );
        assert_eq!(tool_json(&stop)["active"], false);

        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }
}
