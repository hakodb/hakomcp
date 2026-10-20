use std::io::{self, BufRead, Write};
use std::path::PathBuf;
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

const RESOURCE_PREFIX: &str = "hakodb://collections/";
const PROTOCOL_VERSION: &str = "2024-11-05";

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

struct McpServer {
    db: Hako,
}

impl McpServer {
    fn handle(&self, message: &JsonValue) -> Option<JsonValue> {
        let id = message.get("id").cloned();
        let method = message.get("method").and_then(JsonValue::as_str);

        // Notifications (no id) need no response.
        if id.is_none() {
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

        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        let result: RpcResult<JsonValue> = match method {
            "initialize" => Ok(self.initialize()),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(self.tools_list()),
            "tools/call" => self.tools_call(&params),
            "resources/list" => self.resources_list(),
            "resources/read" => self.resources_read(&params),
            "resources/templates/list" => Ok(json!({ "resourceTemplates": [] })),
            _ => Err(RpcError::method_not_found(method)),
        };

        Some(match result {
            Ok(value) => success_response(id, value),
            Err(error) => error_response(id, error.code, error.message),
        })
    }

    fn initialize(&self) -> JsonValue {
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": {},
                "resources": {}
            },
            "serverInfo": {
                "name": "hakomcp",
                "version": env!("CARGO_PKG_VERSION")
            }
        })
    }

    fn tools_list(&self) -> JsonValue {
        json!({
            "tools": [
                {
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
                },
                {
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
                },
                {
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
                },
                {
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
                },
                {
                    "name": "find_near",
                    "description": "Exact KNN search over a vector index. `vector` is the query embedding and `k` is the hit count.",
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
                },
                {
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
                }
            ]
        })
    }

    fn tools_call(&self, params: &JsonValue) -> RpcResult<JsonValue> {
        let name = params
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| RpcError::invalid("missing tool name"))?;
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
            _ => return Err(RpcError::invalid(format!("unknown tool: {name}"))),
        };

        Ok(match result {
            Ok(value) => tool_success(value),
            Err(error) => tool_error(error.to_string()),
        })
    }

    fn tool_get(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
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
        query = query
            .limit(opt_usize(args, "limit", 50)?)
            .offset(opt_usize(args, "offset", 0)?);

        let rows = self.db.query(query)?;
        let documents: Vec<JsonValue> = rows
            .iter()
            .map(|(id, doc)| doc.to_json_with_id(id))
            .collect();
        Ok(json!({
            "collection": collection,
            "count": documents.len(),
            "documents": documents
        }))
    }

    fn tool_create_index(&self, args: &JsonValue) -> Result<JsonValue> {
        let collection = required_str(args, "collection")?;
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
        let field = required_str(args, "field")?;
        let vector = args
            .get("vector")
            .ok_or_else(|| anyhow!("missing `vector`"))?;
        let query = json_to_f32s(vector)?;
        let k = opt_usize(args, "k", 10)?;
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
            limit: opt_usize(args, "limit", 100)?,
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

    fn resources_list(&self) -> RpcResult<JsonValue> {
        let collections = self
            .db
            .list_collections()
            .map_err(|error| RpcError::internal(error))?;
        let stats = self.db.get_stats();
        let resources: Vec<JsonValue> = collections
            .into_iter()
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

        let collections = self
            .db
            .list_collections()
            .map_err(|error| RpcError::internal(error))?;
        if !collections.iter().any(|name| name == collection) {
            return Err(RpcError::resource_not_found(uri));
        }

        let count = self
            .db
            .get_stats()
            .get(&format!("{collection}_count"))
            .copied()
            .unwrap_or(0);
        let rows = self
            .db
            .query(Query::new(collection).limit(100))
            .map_err(|error| RpcError::internal(error))?;
        let documents: Vec<JsonValue> = rows
            .iter()
            .map(|(id, doc)| doc.to_json_with_id(id))
            .collect();
        let payload = json!({
            "collection": collection,
            "count": count,
            "returned": documents.len(),
            "truncated": count > documents.len(),
            "documents": documents
        });
        let text =
            serde_json::to_string_pretty(&payload).map_err(|error| RpcError::internal(error))?;
        Ok(json!({
            "contents": [{
                "uri": uri,
                "mimeType": "application/json",
                "text": text
            }]
        }))
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

fn tool_success(value: JsonValue) -> JsonValue {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string());
    json!({ "content": [{ "type": "text", "text": text }] })
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

fn write_response(out: &mut impl Write, value: &JsonValue) -> Result<()> {
    let text = serde_json::to_string(value)?;
    writeln!(out, "{text}")?;
    out.flush()?;
    Ok(())
}

fn parse_args() -> Result<PathBuf> {
    let mut db = PathBuf::from("hakomcp-data");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                db = PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("`--db` requires a path"))?,
                );
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("hakomcp {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => bail!("unknown argument `{other}` (try --help)"),
        }
    }
    Ok(db)
}

fn print_help() {
    println!("hakomcp {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Thin MCP server for HakoDB over stdio.");
    println!();
    println!("Usage: hakomcp [--db PATH]");
    println!();
    println!("  --db PATH   database directory (default: ./hakomcp-data)");
    println!("  -h, --help  show this help");
    println!("  -V, --version");
}

fn run() -> Result<()> {
    let db_path = parse_args()?;
    let db = Hako::open(&db_path, HakoConfig::default())
        .with_context(|| format!("open database at {}", db_path.display()))?;
    eprintln!("[hakomcp] database {}", db_path.display());
    let server = McpServer { db };

    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line).context("read stdin")? == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let message = match serde_json::from_str::<JsonValue>(&line) {
            Ok(message) => message,
            Err(error) => {
                let response =
                    error_response(JsonValue::Null, -32700, format!("Parse error: {error}"));
                write_response(&mut output, &response)?;
                continue;
            }
        };
        if let Some(response) = server.handle(&message) {
            write_response(&mut output, &response)?;
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("[hakomcp] {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn temp_server(tag: &str) -> (McpServer, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("hakomcp-{tag}-{}-{nanos}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        (McpServer { db }, dir)
    }

    fn request(server: &McpServer, id: u64, method: &str, params: JsonValue) -> JsonValue {
        server
            .handle(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }))
            .unwrap()
    }

    fn tool_json(response: &JsonValue) -> JsonValue {
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("missing tool text: {response}"));
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn handshake_tool_and_resource_roundtrip() {
        let (server, dir) = temp_server("roundtrip");
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
        let (server, dir) = temp_server("vector");
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
        assert!(server.db.await_quiescent(Duration::from_secs(30)));

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
}
