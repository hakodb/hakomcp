# hakomcp

> Part of [**HakoDB**](https://github.com/hakodb/hakodb) — embedded document DB with native vector search and multi-plane sync, written in Rust. The engine + C ABI live in `hakodb/hakodb`; this repo holds the MCP server.

Thin [Model Context Protocol](https://modelcontextprotocol.io) server over stdio. It opens one local HakoDB database and exposes the engine's document, query, index, vector, and traversal operations as MCP tools. Collections are MCP resources.

No engine changes: this is a JSON adapter over the public `hakodb` crate.

## Compatibility

| hakomcp | hako core |
|---|---|
| 0.1.0 | `hakodb 0.16+` (crates.io) |
| 0.2.0 | `hakodb 0.19+` (`min`/`max` aggregates, `document_exists`, per-collection `compact`, serialized stats) |

## Build

```sh
cargo build --release
```

The binary lands at `target/release/hakomcp`.

## Run

```sh
hakomcp --db ./my-data
```

`--db` is the database directory (default `./hakomcp-data`). The process speaks newline-delimited JSON-RPC on stdin/stdout. Logs go to stderr; stdout carries protocol messages only.

Example MCP client entry:

```json
{
  "mcpServers": {
    "hako": {
      "command": "hakomcp",
      "args": ["--db", "/absolute/path/to/my-data"]
    }
  }
}
```

## Tools

| Tool | Arguments | Result |
|---|---|---|
| `get` | `collection`, `id` | `{found, document}` |
| `document_exists` | `collection`, `id` | `{collection, id, exists}` |
| `put` | `collection`, `doc`, `id?`, `vectors?` | `{ok, collection, id}` |
| `query` | `collection`, `filters?`, `any?`, `order_by?`, `limit?`, `offset?`, `select?` | `{collection, count, documents}` |
| `aggregate` | `collection`, `op`, `filters?`, `any?`, `field?` | `{collection, op, results}` |
| `document_delete` | `collection`, `id`, `force?` (hard delete; soft is the default) | `{ok, collection, id}` |
| `document_restore` | `collection`, `id` | `{ok, collection, id, document}` |
| `document_restore_available` | `collection?` (omitted = every collection) | retention records |
| `documents_delete_queue` | `collection?` (omitted = every collection) | records due for sweep |
| `sweep` | - | `{purged: count}` (admin profile) |
| `compact` | `collection?` | `{ok, job_id}` |
| `create_index` | `collection`, `field`, `kind`, `dim?`, `metric?` | `{ok, collection, field, kind}` |
| `find_near` | `collection`, `field`, `vector`, `k?` | `{collection, field, count, hits}` |
| `find_paths` | `collection`, `id`, `direction?`, `edge_field?`, `max_depth?`, `limit?`, `via_edge?`, `weight_field?` | `{collection, id, count, hits}` |

`kind` is `secondary`, `fts`, or `vector`. Vector indexes need `dim`; `metric` is `cosine` (default) or `l2`.

### Documents and vectors

`doc` holds ordinary JSON values. A vector field is a `Value::Binary` LE `f32[]` in the engine, which JSON cannot express directly, so `put` takes vectors in a separate map and encodes them:

```json
{
  "name": "put",
  "arguments": {
    "collection": "products",
    "id": "kopi-1",
    "doc": { "name": "Kopi", "category": "drink" },
    "vectors": { "embedding": [0.12, -0.03, 0.88] }
  }
}
```

`vectors` entries override same-named `doc` fields. Create the vector index before or after writing; the engine backfills existing documents on index creation. `find_near` returns nearest-first hits with a `score` that is the metric distance (lower is closer).

### Filters

`filters` are AND-combined. Each entry is `{field, op, value}` with `op` one of:

`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `match`, `match_prefix`, `contains`, `starts_with`, `in`, `array_contains`, `array_contains_any`, `array_all_eq`, `not_in`.

`any` entries are OR-combined. `order_by` entries are `{field, direction}` with `direction` `asc` (default) or `desc`. `limit` defaults to 50 and `select` projects only the named fields.

## Resources

`resources/list` returns one resource per visible collection:

```
hakodb://collections/<name>
```

`resources/read` returns a JSON sample: collection name, document count, and up to 100 documents (with `truncated` when more exist).

## Scope

MCP and vector are separate layers: `find_near` is the retrieval primitive, so an agent can build RAG by writing embeddings with `put` and searching them with `find_near`. Out of scope: prompt management, HTTP transport, embedding models, and auth beyond the local engine.

## License

MIT
