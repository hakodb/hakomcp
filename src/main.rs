use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::traverse::{TraverseDir, TraverseSpec};
use hakodb::index::vector::{decode_f32s, distance, encode_f32s, Metric};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;
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
            "get" | "query" | "find_near" | "find_paths" | "server_status" => true,
            "put" | "create_index" => matches!(self, Self::Data | Self::Sync | Self::Admin),
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

struct Server {
    db: Arc<Hako>,
    config: ServerConfig,
    initialized: AtomicBool,
}

impl Server {
    fn new(db: Arc<Hako>, config: ServerConfig) -> Self {
        if config.profile == Profile::Read {
            db.set_read_only(true);
        }
        Self {
            db,
            config,
            initialized: AtomicBool::new(false),
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
            "resources/subscribe" | "resources/unsubscribe" => {
                Err(RpcError::invalid(if self.config.subscriptions_enabled() {
                    "subscriptions are not implemented yet"
                } else {
                    "subscriptions are disabled; set --subscribe-to to opt in"
                }))
            }
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
                "resources": { "subscribe": false, "listChanged": false }
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
            "Change subscriptions are configured but not active in this build."
        } else {
            "Change notifications are disabled."
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
                "name": "get",
                "description": "Read one document by collection and id.",
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
                "name": "put",
                "description": "Insert or replace a document. Fields hold ordinary JSON values; `vectors` holds vector-index fields (LE f32 arrays) and overrides same-named doc fields.",
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
                "name": "create_index",
                "description": "Create a secondary, full-text, or vector index. Vector indexes also need `dim`; `metric` defaults to cosine.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "field": { "type": "string" },
                        "kind": { "type": "string", "enum": ["secondary", "fts", "vector"] },
                        "dim": { "type": "integer", "minimum": 1 },
                        "metric": { "type": "string", "enum": ["cosine", "l2"] }
                    },
                    "required": ["collection", "field", "kind"]
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
        let name = params
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing tool name"))?;

        let known = matches!(
            name,
            "get" | "put" | "query" | "create_index" | "find_near" | "find_paths" | "server_status"
        );
        if !known {
            return Err(RpcError::invalid(format!("unknown tool: {name}")));
        }
        if !self.config.profile.allows_tool(name) {
            return Err(RpcError::invalid(format!(
                "tool `{name}` is not available in profile `{}`",
                self.config.profile.as_str()
            )));
        }

        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let result = match name {
            "get" => self.tool_get(&args),
            "put" => self.tool_put(&args),
            "query" => self.tool_query(&args),
            "create_index" => self.tool_create_index(&args),
            "find_near" => self.tool_find_near(&args),
            "find_paths" => self.tool_find_paths(&args),
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

    fn tool_query(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let mut query = Query::new(collection);

        if let Some(filters) = args.get("filters") {
            let filters = filters
                .as_array()
                .ok_or_else(|| anyhow!("`filters` must be an array"))?;
            for filter in filters {
                let (field, op, value) = parse_filter(filter)?;
                query = query.where_filter(&field, op, value);
            }
        }
        if let Some(filters) = args.get("any") {
            let filters = filters
                .as_array()
                .ok_or_else(|| anyhow!("`any` must be an array"))?;
            for filter in filters {
                let (field, op, value) = parse_filter(filter)?;
                query = query.or_where(&field, op, value);
            }
        }
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

        let requested_limit = opt_usize(args, "limit", 50)?;
        let limit = requested_limit.min(self.config.max_query_rows);
        query = query.limit(limit).offset(opt_usize(args, "offset", 0)?);

        let rows = self.db.query(query)?;
        let documents: Vec<JsonValue> = rows
            .iter()
            .map(|(id, doc)| doc.to_json_with_id(id))
            .collect();
        Ok(json!({
            "collection": collection,
            "count": documents.len(),
            "limit": limit,
            "documents": documents
        }))
    }

    fn tool_create_index(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
        self.ensure_collection(collection)?;
        let field = required_str(args, "field")?;
        let kind = required_str(args, "kind")?;
        match kind {
            "secondary" => self.db.create_index(collection, field)?,
            "fts" => self.db.create_fts_index(collection, field)?,
            "vector" => {
                let dim = required_u32(args, "dim")?;
                let metric = match opt_str(args, "metric")?.unwrap_or("cosine") {
                    "cosine" => Metric::Cosine,
                    "l2" => Metric::L2,
                    other => bail!("`metric` must be cosine or l2, got `{other}`"),
                };
                self.db
                    .create_vector_index(collection, field, dim, metric)?;
            }
            other => bail!("`kind` must be secondary, fts, or vector, got `{other}`"),
        }
        Ok(json!({
            "ok": true,
            "collection": collection,
            "field": field,
            "kind": kind
        }))
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
                "active": false,
                "collections": self.config.subscribe_to
            },
            "network": { "enabled": self.config.allow_network },
            "destructive": { "enabled": self.config.allow_destructive }
        }))
    }

    fn resources_list(&self) -> RpcResult<JsonValue> {
        let collections = self.db.list_collections().map_err(RpcError::internal)?;
        let stats = self.db.get_stats();
        let resources: Vec<JsonValue> = collections
            .into_iter()
            .filter(|collection| self.config.collection_allowed(collection))
            .map(|collection| {
                let count = stats
                    .get(&format!("{collection}_count"))
                    .copied()
                    .unwrap_or(0);
                json!({
                    "uri": format!("{RESOURCE_PREFIX}{collection}"),
                    "name": collection,
                    "description": format!("HakoDB collection ({count} documents)"),
                    "mimeType": "application/json"
                })
            })
            .collect();
        Ok(json!({ "resources": resources }))
    }

    fn resources_read(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        let uri = params
            .get("uri")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing resource `uri`"))?;
        let collection = uri
            .strip_prefix(RESOURCE_PREFIX)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| RpcError::resource_not_found(uri))?;

        if !self.config.collection_allowed(collection) {
            return Err(RpcError::resource_not_found(uri));
        }

        let collections = self.db.list_collections().map_err(RpcError::internal)?;
        if !collections.iter().any(|name| name == collection) {
            return Err(RpcError::resource_not_found(uri));
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

fn parse_filter(filter: &JsonValue) -> Result<(String, Operator, Value)> {
    let field = required_str(filter, "field")?.to_string();
    let op = parse_operator(required_str(filter, "op")?)?;
    let value = filter
        .get("value")
        .ok_or_else(|| anyhow!("filter needs `value`"))?;
    let value = Value::from_json(value.clone()).map_err(|error| anyhow!(error))?;
    Ok((field, op, value))
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
    println!("  --subscribe-to LIST        collection allowlist for future change notifications");
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
    let server = Arc::new(Server::new(db, cli.config.clone()));
    server.log(&format!("database {}", cli.db.display()));
    server.log(&format!("profile {}", cli.config.profile.as_str()));

    let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<String>(64);
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<JsonValue>(64);

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
    writer.await.context("writer task join")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_server(tag: &str, config: ServerConfig) -> (Arc<Server>, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("hakomcp-{tag}-{}-{nanos}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Arc::new(Hako::open(&dir, HakoConfig::default()).unwrap());
        (Arc::new(Server::new(db, config)), dir)
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
    fn calls_before_initialize_are_rejected() {
        let (server, dir) = temp_server("lifecycle", ServerConfig::default());
        let call = request(&server, 1, "tools/list", json!({}));
        assert_eq!(call["error"]["code"].as_i64(), Some(-32002));
        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }
}
