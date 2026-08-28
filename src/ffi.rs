//! C ABI bindings for airlayer.
//!
//! Exposes the same functionality as the WASM module but as plain `extern "C"`
//! symbols suitable for consumption from any language with FFI support
//! (Dart, Swift, Kotlin/JNI, C/C++, …). Used by the official Dart SDK in
//! `sdk-dart/` to drive airlayer from Flutter mobile apps.
//!
//! ## ABI shape
//!
//! Every entry point takes a single null-terminated UTF-8 JSON string and
//! returns a heap-allocated null-terminated UTF-8 JSON string of the form:
//!
//! ```jsonc
//! { "ok": <result> }        // success
//! { "error": "<message>" }  // failure
//! ```
//!
//! The caller MUST free the returned pointer with `airlayer_free` once they
//! have copied the data out. Passing a null pointer to `airlayer_free` is a
//! no-op. Functions return null only if the input pointer itself is null
//! (i.e. you couldn't even build a string to report the error in).
//!
//! ## Why JSON in / JSON out?
//!
//! - Avoids cross-language array marshaling (variable-length lists of YAML
//!   strings would otherwise need pointer-of-pointer-plus-length plumbing).
//! - One return shape across all entry points → trivial Dart wrapper.
//! - The serialization cost is negligible compared to the SQL compilation
//!   inside airlayer itself.
//!
//! ## Surface
//!
//! Mirrors `src/wasm.rs`:
//!
//! | Capability                  | WASM                       | FFI                              |
//! |-----------------------------|----------------------------|----------------------------------|
//! | Compile a query to SQL      | `compile`                  | `airlayer_compile`               |
//! | Validate schema             | `validate`                 | `airlayer_validate`              |
//! | List semantic objects       | `catalog_list`             | `airlayer_catalog`               |
//! | Resolve cached rollup       | `cache_resolve`            | `airlayer_cache_resolve`         |
//! | Build manifest from rows    | `cache_build_manifest`     | `airlayer_cache_build_manifest`  |
//! | Cache key helper            | `cache_key`                | `airlayer_cache_key`             |
//! | Live cache keys (eviction)  | `cache_live_keys`          | `airlayer_cache_live_keys`       |
//! | Resolve warehouse rollup    | `cache_resolve_warehouse`  | `airlayer_cache_resolve_warehouse` |
//! | Version string              | (via package.json)         | `airlayer_version`               |
//! | Free returned C string      | (GC)                       | `airlayer_free`                  |
//!
//! ## Build
//!
//! ```sh
//! # native dev (host-only dylib)
//! cargo build --release --no-default-features --features ffi
//!
//! # android (requires cargo-ndk and the relevant Rust targets installed):
//! cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 \
//!     build --release --no-default-features --features ffi
//! ```
//!
//! See `sdk-dart/README.md` for the full cross-compile recipe.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{self, AssertUnwindSafe};

use serde_json::json;

use crate::dialect::Dialect;
use crate::engine::catalog;
use crate::engine::preagg;
use crate::engine::query::QueryRequest;
use crate::engine::{DatasourceDialectMap, SemanticEngine};
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;
use crate::schema::validator::SchemaValidator;

// ---- Public entry points ---------------------------------------------------

/// Compile a semantic query to SQL.
///
/// Args JSON shape:
/// ```jsonc
/// {
///   "views":   ["<view yaml 1>", "<view yaml 2>", ...],
///   "query":   { /* QueryRequest */ },
///   "dialect": "duckdb",
///   "topics":  ["<topic yaml>", ...]?,
///   "motifs":  ["<motif yaml>", ...]?,
///   "queries": ["<saved query yaml>", ...]?
/// }
/// ```
/// Returns `{ "ok": { "sql": "...", "params": [...], "columns": [...] } }`.
#[no_mangle]
pub extern "C" fn airlayer_compile(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CompileArgs| {
        let layer = build_layer(&args.views, &args.topics, &args.motifs, &args.queries)?;
        let resolved = Dialect::from_str(&args.dialect)
            .ok_or_else(|| format!("Unknown dialect: {}", args.dialect))?;
        let mut dialect_map = DatasourceDialectMap::new();
        dialect_map.set_default(resolved);

        let engine =
            SemanticEngine::from_semantic_layer(layer, dialect_map).map_err(|e| e.to_string())?;
        let request: QueryRequest =
            serde_json::from_value(args.query).map_err(|e| format!("Invalid query JSON: {e}"))?;
        let result = engine.compile_query(&request).map_err(|e| e.to_string())?;
        serde_json::to_value(result).map_err(|e| e.to_string())
    })
}

/// Validate view (and optional topic/motif/query) YAMLs without compiling.
///
/// Args JSON shape:
/// ```jsonc
/// { "views": [...], "topics": [...]?, "motifs": [...]?, "queries": [...]? }
/// ```
/// Returns `{ "ok": true }` on success; `{ "error": "..." }` on failure.
#[no_mangle]
pub extern "C" fn airlayer_validate(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: ValidateArgs| {
        let layer = build_layer(&args.views, &args.topics, &args.motifs, &args.queries)?;
        SchemaValidator::validate(&layer).map_err(|e| e.to_string())?;
        Ok(json!(true))
    })
}

/// List all semantic objects (views, dimensions, measures, motifs, …).
///
/// Args JSON shape matches `airlayer_compile` minus `query` and `dialect`.
/// Returns `{ "ok": [...catalog entries...] }`.
#[no_mangle]
pub extern "C" fn airlayer_catalog(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CatalogArgs| {
        let layer = build_layer(&args.views, &args.topics, &args.motifs, &args.queries)?;
        let entries = catalog::catalog(&layer);
        serde_json::to_value(entries).map_err(|e| e.to_string())
    })
}

/// Check if a cached rollup covers a query and return re-aggregation SQL.
///
/// Mirrors WASM `cache_resolve`. The browser/Flutter caller is expected to
/// load the cached parquet/data into a table named `__cache` and execute the
/// returned `reagg_sql`.
///
/// Args JSON shape:
/// ```jsonc
/// {
///   "manifest": { /* LocalManifest */ },
///   "query":    { /* QueryRequest */ },
///   "views":    [ "<.view.yml contents>", ... ]   // optional
/// }
/// ```
/// Pass `views` wherever the caller has the schema: a rollup's hash covers the
/// *definition* of its members, so an edited `expr:` or `type:` moves it, and
/// a manifest row the schema no longer declares is then declined rather than
/// answered from. Omit it and the match is on member names alone — the old
/// behaviour, reported as `stale_checked: false`. An empty array is not the
/// same thing: it is a schema declaring nothing, so every row is stale.
///
/// Returns `{ "ok": { "reagg_sql", "cache_key", "entry", "stale_checked" } }`
/// on a hit, `{ "ok": null }` if no rollup covers the query.
#[no_mangle]
pub extern "C" fn airlayer_cache_resolve(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheResolveArgs| {
        let manifest: preagg::LocalManifest = serde_json::from_value(args.manifest)
            .map_err(|e| format!("Invalid manifest JSON: {e}"))?;
        let request: QueryRequest =
            serde_json::from_value(args.query).map_err(|e| format!("Invalid query JSON: {e}"))?;
        let live = live_rollups_from(&args.views)?;
        match preagg::resolve_cached(&request, &manifest, live.as_ref()) {
            Some(resolution) => serde_json::to_value(resolution).map_err(|e| e.to_string()),
            None => Ok(serde_json::Value::Null),
        }
    })
}

/// Parse warehouse `__manifest` rows into a `LocalManifest` JSON object.
///
/// Mirrors WASM `cache_build_manifest`. Useful for warehouse → browser/mobile
/// hand-off: the caller queries the warehouse manifest, passes the rows here,
/// and stores the returned manifest locally (IndexedDB / sqflite / file).
///
/// Args JSON shape:
/// ```jsonc
/// {
///   "rows":            [ { /* warehouse manifest row */ }, ... ],
///   "source_database": "<name>"
/// }
/// ```
/// Returns `{ "ok": { /* LocalManifest */ } }`.
#[no_mangle]
pub extern "C" fn airlayer_cache_build_manifest(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheBuildManifestArgs| {
        let warehouse_entries = preagg::parse_manifest_rows(&args.rows);
        let local_entries: Vec<preagg::LocalRollupEntry> = warehouse_entries
            .iter()
            .map(|e| {
                let mut local = e.to_local_entry();
                local.file = format!("{}__{}", e.view_name, e.rollup_hash);
                local
            })
            .collect();

        let manifest = preagg::LocalManifest {
            pulled_at: String::new(),
            source_database: args.source_database,
            rollups: local_entries,
        };
        serde_json::to_value(manifest).map_err(|e| e.to_string())
    })
}

/// Compute the cache key for a `(view_name, rollup_hash)` pair.
///
/// Mirrors WASM `cache_key`. Trivial — exposed so callers don't have to
/// re-implement the `"view__hash"` format and risk drifting.
///
/// Args JSON shape:
/// ```jsonc
/// { "view_name": "<name>", "rollup_hash": "<hash>" }
/// ```
/// Returns `{ "ok": "<view>__<hash>" }`.
#[no_mangle]
pub extern "C" fn airlayer_cache_key(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheKeyArgs| {
        Ok(json!(format!("{}__{}", args.view_name, args.rollup_hash)))
    })
}

/// Resolve a query against warehouse rollup rows (Layer 2 cache).
///
/// Mirrors WASM `cache_resolve_warehouse`. The caller is expected to execute
/// the returned `reagg_sql` against the warehouse.
///
/// Args JSON shape:
/// ```jsonc
/// {
///   "rows":    [ { /* warehouse manifest row */ }, ... ],
///   "query":   { /* QueryRequest */ },
///   "schema":  "<preagg schema>",
/// The cache keys the current schema declares — the local store's retain-set.
///
/// Mirrors WASM `cache_live_keys`. Declining a stale manifest row stops it
/// being read; it does not evict the blob stored under the old key, and once
/// the row is gone nothing can name that key again. Keep what this returns,
/// delete the rest.
///
/// Args JSON shape:
/// ```jsonc
/// { "views": [ "<.view.yml contents>", ... ] }
/// ```
/// Returns `{ "ok": ["<view>__<hash>", ...] }`.
#[no_mangle]
pub extern "C" fn airlayer_cache_live_keys(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheLiveKeysArgs| {
        let parser = SchemaParser::new();
        let views =
            parse_yaml_strings(&args.views, "views", |y, src| parser.parse_view_str(y, src))?;
        let refs: Vec<&crate::schema::models::View> = views.iter().collect();
        Ok(json!(preagg::live_rollup_keys(&refs)))
    })
}

///   "dialect": "<dialect name>",
///   "views":   [ "<.view.yml contents>", ... ]   // optional, see `airlayer_cache_resolve`
/// }
/// ```
/// Returns `{ "ok": { "reagg_sql", "table_name", "stale_checked" } }` on a hit,
/// `{ "ok": null }` if no rollup covers the query.
#[no_mangle]
pub extern "C" fn airlayer_cache_resolve_warehouse(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheResolveWarehouseArgs| {
        let request: QueryRequest =
            serde_json::from_value(args.query).map_err(|e| format!("Invalid query JSON: {e}"))?;
        let dialect = Dialect::from_str(&args.dialect)
            .ok_or_else(|| format!("Unknown dialect: {}", args.dialect))?;
        let entries = preagg::parse_manifest_rows(&args.rows);
        let live = live_rollups_from(&args.views)?;
        match preagg::resolve_warehouse(&request, &entries, &args.schema, &dialect, live.as_ref()) {
            Some(preagg::PreaggResolution::WarehouseRollup {
                reagg_sql,
                table_name,
            }) => Ok(json!({
                "reagg_sql": reagg_sql,
                "table_name": table_name,
            })),
            _ => Ok(serde_json::Value::Null),
        }
    })
}

/// Returns `{ "ok": "<semver>" }`. Used to verify the binding is loaded and
/// to surface the airlayer version in client-side error messages.
#[no_mangle]
pub extern "C" fn airlayer_version() -> *mut c_char {
    let v = env!("CARGO_PKG_VERSION");
    new_c_string(&json!({ "ok": v }).to_string())
}

/// Frees a string previously returned by an `airlayer_*` function. Passing
/// null is a no-op. Calling this on a pointer not produced by airlayer is
/// undefined behavior.
///
/// # Safety
/// `ptr` must be either null or a pointer returned by one of the
                "stale_checked": live.is_some(),
/// `airlayer_*` entry points in this module.
#[no_mangle]
pub unsafe extern "C" fn airlayer_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: ptr was produced by `CString::into_raw` inside this module.
    drop(CString::from_raw(ptr));
}

// ---- Args structs ----------------------------------------------------------

#[derive(serde::Deserialize)]
struct CompileArgs {
    views: Vec<String>,
    query: serde_json::Value,
    dialect: String,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    motifs: Option<Vec<String>>,
    #[serde(default)]
    queries: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct ValidateArgs {
    views: Vec<String>,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    motifs: Option<Vec<String>>,
    #[serde(default)]
    queries: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct CatalogArgs {
    views: Vec<String>,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    motifs: Option<Vec<String>>,
    #[serde(default)]
    queries: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct CacheResolveArgs {
    manifest: serde_json::Value,
    query: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct CacheBuildManifestArgs {
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    source_database: String,
}

#[derive(serde::Deserialize)]
struct CacheKeyArgs {
    view_name: String,
    rollup_hash: String,
}

#[derive(serde::Deserialize)]
struct CacheResolveWarehouseArgs {
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
    query: serde_json::Value,
    schema: String,
    /// Optional .view.yml contents. Absent means no staleness check — the old
    /// behaviour, reported back as `stale_checked: false`. Present but empty
    /// is a checked, empty schema: nothing is live and every row is declined.
    #[serde(default)]
    views: Option<Vec<String>>,
    dialect: String,
}

// ---- Internal helpers ------------------------------------------------------

/// Build a `SemanticLayer` from the four parser-input slices used by
/// `compile`, `validate`, and `catalog`. Centralized so motifs/queries
/// support stays consistent across entry points.
fn build_layer(
    views: &[String],
    topics: &Option<Vec<String>>,
    motifs: &Option<Vec<String>>,
    queries: &Option<Vec<String>>,
) -> Result<SemanticLayer, String> {
    let parser = SchemaParser::new();
    let parsed_views = parse_yaml_strings(views, "views", |y, src| parser.parse_view_str(y, src))?;
    let parsed_topics = parse_optional(topics, "topics", |y, src| parser.parse_topic_str(y, src))?;
    let parsed_motifs = parse_optional(motifs, "motifs", |y, src| parser.parse_motif_str(y, src))?;
    let parsed_queries = parse_optional(queries, "queries", |y, src| {
        parser.parse_saved_query_str(y, src)
    })?;
    #[serde(default)]
    views: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct CacheLiveKeysArgs {
    views: Vec<String>,
    Ok(SemanticLayer::with_motifs_and_queries(
        parsed_views,
        parsed_topics,
        parsed_motifs,
        parsed_queries,
    ))
}

/// Common scaffolding for every entry point:
///   1. Parse args JSON.
///   2. Run the inner closure, catching panics so a bug in airlayer doesn't
///      tear down the host process.
///   3. Wrap the result as `{ "ok": ... }` or `{ "error": "..." }`.
///   4. Hand back a heap-allocated C string.
fn handle_call<A, F>(args_json: *const c_char, f: F) -> *mut c_char
where
    A: serde::de::DeserializeOwned,
    F: FnOnce(A) -> Result<serde_json::Value, String>,
{
    let result: Result<serde_json::Value, String> = (|| -> Result<serde_json::Value, String> {
        let json_str = unsafe { read_c_str(args_json) }
            .ok_or_else(|| "null or non-UTF-8 args pointer".to_string())?;
        let args: A =
            serde_json::from_str(&json_str).map_err(|e| format!("Invalid args JSON: {e}"))?;

        panic::catch_unwind(AssertUnwindSafe(|| f(args)))
            .map_err(panic_message)
            .and_then(|inner| inner)
/// Parse view YAML into the `(view, hash)` pairs the schema declares now.
///
/// `None` only when the field is absent: resolution then falls back to the
/// name-only match every caller got before this field existed. An empty array
/// is a schema that declares nothing, not a missing one — it yields an empty
/// live set, which declines every manifest row. Collapsing the two would let a
/// host whose views all vanished keep serving from rows nothing declares, with
/// `stale_checked: false` the only, easily missed, signal.
fn live_rollups_from(views: &Option<Vec<String>>) -> Result<Option<preagg::LiveRollups>, String> {
    match views {
        Some(arr) => {
            let parser = SchemaParser::new();
            let parsed = parse_yaml_strings(arr, "views", |y, src| parser.parse_view_str(y, src))?;
            let refs: Vec<&crate::schema::models::View> = parsed.iter().collect();
            Ok(Some(preagg::live_rollups(&refs)))
        }
        None => Ok(None),
    }
}

    })();

    let body = match result {
        Ok(value) => json!({ "ok": value }),
        Err(msg) => json!({ "error": msg }),
    };
    new_c_string(&body.to_string())
}

unsafe fn read_c_str(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr).to_str().ok().map(|s| s.to_string())
}

fn new_c_string(s: &str) -> *mut c_char {
    // CString::new rejects interior nulls; JSON output from serde_json can't
    // contain raw nulls so this is infallible in practice, but fall back to
    // an error JSON rather than abort if it ever does.
    CString::new(s)
        .unwrap_or_else(|_| CString::new("{\"error\":\"output contained NUL byte\"}").unwrap())
        .into_raw()
}

fn parse_yaml_strings<T>(
    items: &[String],
    label: &str,
    mut parse_fn: impl FnMut(&str, &str) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    items
        .iter()
        .enumerate()
        .map(|(i, y)| parse_fn(y, &format!("<{label}_{i}>")))
        .collect()
}

fn parse_optional<T>(
    items: &Option<Vec<String>>,
    label: &str,
    parse_fn: impl FnMut(&str, &str) -> Result<T, String>,
) -> Result<Option<Vec<T>>, String> {
    match items {
        // Treat both missing and empty arrays as "no value" — keeps the args
        // shape forgiving for callers that always send the field.
        Some(arr) if !arr.is_empty() => Ok(Some(parse_yaml_strings(arr, label, parse_fn)?)),
        _ => Ok(None),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        format!("airlayer panicked: {s}")
    } else if let Some(s) = payload.downcast_ref::<String>() {
        format!("airlayer panicked: {s}")
    } else {
        "airlayer panicked: (no message)".to_string()
    }
}

// ---- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn call(f: extern "C" fn(*const c_char) -> *mut c_char, json: &str) -> serde_json::Value {
        let c = CString::new(json).unwrap();
        let ptr = f(c.as_ptr());
        assert!(!ptr.is_null());
        let out = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
        unsafe { airlayer_free(ptr) };
        serde_json::from_str(&out).expect("non-JSON output")
    }

    const VIEW: &str = r#"
name: orders
table: orders
datasource: local
dialect: duckdb
dimensions:
  - name: status
    type: string
    expr: status
measures:
  - name: count
    type: count
"#;

    #[test]
    fn validate_ok() {
        let args = json!({ "views": [VIEW] }).to_string();
        let res = call(airlayer_validate, &args);
        assert_eq!(res, json!({ "ok": true }));
    }

    #[test]
    fn validate_bad_yaml_returns_error() {
        let args = json!({ "views": ["not yaml: ["] }).to_string();
        let res = call(airlayer_validate, &args);
        assert!(res.get("error").is_some(), "expected error, got {res}");
    }

    #[test]
    fn compile_basic() {
        let args = json!({
            "views": [VIEW],
            "dialect": "duckdb",
            "query": { "measures": ["orders.count"], "dimensions": ["orders.status"] }
        })
        .to_string();
        let res = call(airlayer_compile, &args);
        let ok = res.get("ok").expect("expected ok");
        assert!(ok
            .get("sql")
            .and_then(|v| v.as_str())
            .unwrap()
            .contains("orders"));
    }

    #[test]
    fn compile_unknown_measure_returns_error() {
        let args = json!({
            "views": [VIEW],
            "dialect": "duckdb",
            "query": { "measures": ["orders.does_not_exist"], "dimensions": ["orders.status"] }
        })
        .to_string();
        let res = call(airlayer_compile, &args);
        assert!(res.get("error").is_some(), "expected error, got {res}");
    }

    #[test]
    fn catalog_lists_view() {
        let args = json!({ "views": [VIEW] }).to_string();
        let res = call(airlayer_catalog, &args);
        let entries = res.get("ok").and_then(|v| v.as_array()).expect("array");
        assert!(!entries.is_empty());
    }

    #[test]
    fn version_returns_semver() {
        let ptr = airlayer_version();
        let out = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
        unsafe { airlayer_free(ptr) };
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("ok").and_then(|s| s.as_str()).unwrap().contains('.'));
    }

    #[test]
    fn null_pointer_returns_error_not_segfault() {
        let ptr = airlayer_compile(std::ptr::null());
        let out = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
        unsafe { airlayer_free(ptr) };
        assert!(out.contains("error"));
    }

    #[test]
    fn cache_key_format() {
        let args = json!({ "view_name": "orders", "rollup_hash": "abc123" }).to_string();
        let res = call(airlayer_cache_key, &args);
        assert_eq!(res, json!({ "ok": "orders__abc123" }));
    }

    #[test]
    fn cache_build_manifest_roundtrip() {
        let rows = json!([{
            "view_name": "orders",
            "rollup_name": "by_day",
            "rollup_hash": "abc123",
            "table_name": "airlayer.orders__abc123",
            "dimensions": "[\"status\"]",
            "measures": "[{\"name\":\"count\",\"measure_type\":\"count\",\"expr\":null,\"columns\":[\"count__count\"]}]",
            "time_dimension": "created_at",
            "granularity": "day",
            "build_date": "2026-01-01",
        }]);
        let args = json!({ "rows": rows, "source_database": "warehouse" }).to_string();
        let res = call(airlayer_cache_build_manifest, &args);
        let manifest = res.get("ok").expect("expected ok manifest");
        assert_eq!(
            manifest.get("source_database").and_then(|v| v.as_str()),
            Some("warehouse")
        );
        let rollups = manifest
            .get("rollups")
            .and_then(|v| v.as_array())
            .expect("rollups array");
        assert_eq!(rollups.len(), 1);
        assert_eq!(
            rollups[0].get("file").and_then(|v| v.as_str()),
            Some("orders__abc123")
        );
    }

    #[test]
    fn cache_resolve_returns_null_when_no_coverage() {
        let manifest = json!({
            "pulled_at": "",
            "source_database": "warehouse",
            "rollups": []
        });
        let args = json!({
            "manifest": manifest,
            "query": { "measures": ["orders.count"], "dimensions": ["orders.status"] }
        })
        .to_string();
        let res = call(airlayer_cache_resolve, &args);
        assert_eq!(res, json!({ "ok": serde_json::Value::Null }));
    }

    #[test]
    fn cache_resolve_warehouse_returns_null_when_no_coverage() {
        let args = json!({
            "rows": [],
            "query": { "measures": ["orders.count"], "dimensions": ["orders.status"] },
            "schema": "airlayer",
            "dialect": "postgres",
        })
        .to_string();
        let res = call(airlayer_cache_resolve_warehouse, &args);
        assert_eq!(res, json!({ "ok": serde_json::Value::Null }));
    }
}

    /// A view whose only rollup is `by_status`. Its hash covers the member
    /// definitions, so editing `expr:` below would move it — which is the
    /// whole point of passing views to the resolve calls.
    const ORDERS_VIEW: &str = r#"
name: orders
table: orders
dimensions:
  - name: status
    type: string
    expr: status
measures:
  - name: total
    type: sum
    expr: amount
pre_aggregations:
  - name: by_status
    dimensions: [status]
    measures: [total]
"#;

    /// The hash the schema declares for `orders.by_status` right now. Read it
    /// out of the API rather than hardcoding it — the fingerprint is an
    /// implementation detail and any change to it should move this test's
    /// fixtures with it, not break them.
    fn live_hash() -> String {
        let args = json!({ "views": [ORDERS_VIEW] }).to_string();
        let res = call(airlayer_cache_live_keys, &args);
        let keys = res.get("ok").and_then(|v| v.as_array()).expect("ok array");
        assert_eq!(keys.len(), 1, "one rollup declared");
        keys[0]
            .as_str()
            .unwrap()
            .strip_prefix("orders__")
            .expect("key is view__hash")
            .to_string()
    }

    fn manifest_with_hash(hash: &str) -> serde_json::Value {
        json!({
            "pulled_at": "",
            "source_database": "warehouse",
            "rollups": [{
                "view_name": "orders",
                "rollup_name": "by_status",
                "rollup_hash": hash,
                "file": format!("orders__{hash}"),
                "dimensions": ["status"],
                "measures": [{"name": "total", "type": "sum", "columns": ["total__sum"]}],
                "time_dimension": null,
                "granularity": null,
                "build_date": "2026-01-01",
            }]
        })
    }

    fn status_query() -> serde_json::Value {
        json!({ "measures": ["orders.total"], "dimensions": ["orders.status"] })
    }

    fn warehouse_rows(hash: &str) -> serde_json::Value {
        json!([{
            "view_name": "orders",
            "rollup_name": "by_status",
            "rollup_hash": hash,
            "table_name": format!("airlayer.orders__{hash}"),
            "dimensions": "[\"status\"]",
            // `"type"`, not `"measure_type"` — this is the shape
            // `build_manifest_entry` writes and `generate_reagg_sql` reads.
            "measures": "[{\"name\":\"total\",\"type\":\"sum\",\"columns\":[\"total__sum\"]}]",
            "time_dimension": "",
            "granularity": "",
            "build_date": "2026-01-01",
        }])
    }

    #[test]
    fn cache_live_keys_lists_the_schemas_rollups() {
        let args = json!({ "views": [ORDERS_VIEW] }).to_string();
        let res = call(airlayer_cache_live_keys, &args);
        let keys = res.get("ok").and_then(|v| v.as_array()).expect("ok array");
        assert_eq!(keys.len(), 1);
        assert!(keys[0].as_str().unwrap().starts_with("orders__"));
    }

    #[test]
    fn cache_resolve_serves_a_rollup_the_schema_still_declares() {
        let args = json!({
            "manifest": manifest_with_hash(&live_hash()),
            "query": status_query(),
            "views": [ORDERS_VIEW],
        })
        .to_string();
        let res = call(airlayer_cache_resolve, &args);
        let ok = res.get("ok").expect("ok field");
        assert!(!ok.is_null(), "the live rollup covers this query: {res}");
        assert_eq!(
            ok.get("stale_checked").and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn cache_resolve_declines_a_rollup_the_schema_no_longer_declares() {
        // A definition edit moves the hash. `covers()` matches member *names*,
        // so without the schema this row still looks like a hit and the host
        // serves pre-edit numbers under the pre-aggregated badge.
        let args = json!({
            "manifest": manifest_with_hash("deadbeef"),
            "query": status_query(),
            "views": [ORDERS_VIEW],
        })
        .to_string();
        let res = call(airlayer_cache_resolve, &args);
        assert_eq!(res, json!({ "ok": serde_json::Value::Null }));
    }

    #[test]
    fn cache_resolve_without_views_keeps_the_unchecked_behaviour() {
        // Back-compat: a caller that sends no `views` gets exactly what it got
        // before the field existed — a name-only match — and `stale_checked`
        // says so rather than the hit implying a guarantee it never made.
        let args = json!({
            "manifest": manifest_with_hash("deadbeef"),
            "query": status_query(),
        })
        .to_string();
        let res = call(airlayer_cache_resolve, &args);
        let ok = res.get("ok").expect("ok field");
        assert!(!ok.is_null(), "unchecked resolution still answers: {res}");
        assert_eq!(
            ok.get("stale_checked").and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn cache_resolve_declines_everything_when_the_schema_is_empty() {
        // `views: []` is a schema that declares nothing, not a missing schema:
        // the caller asked for the check and no rollup is live, so every row is
        // stale. Downgrading it to the unchecked path would answer from rows
        // nothing declares any more, with `stale_checked: false` the only hint.
        let args = json!({
            "manifest": manifest_with_hash(&live_hash()),
            "query": status_query(),
            "views": [],
        })
        .to_string();
        let res = call(airlayer_cache_resolve, &args);
        assert_eq!(res, json!({ "ok": serde_json::Value::Null }));
    }

    #[test]
    fn cache_resolve_warehouse_declines_everything_when_the_schema_is_empty() {
        let args = json!({
            "rows": warehouse_rows(&live_hash()),
            "query": status_query(),
            "schema": "airlayer",
            "dialect": "postgres",
            "views": [],
        })
        .to_string();
        let res = call(airlayer_cache_resolve_warehouse, &args);
        assert_eq!(res, json!({ "ok": serde_json::Value::Null }));
    }

    #[test]
    fn cache_resolve_warehouse_honours_the_live_set() {
        let hash = live_hash();
        let live_args = json!({
            "rows": warehouse_rows(&hash),
            "query": status_query(),
            "schema": "airlayer",
            "dialect": "postgres",
            "views": [ORDERS_VIEW],
        })
        .to_string();
        let res = call(airlayer_cache_resolve_warehouse, &live_args);
        let ok = res.get("ok").expect("ok field");
        assert!(!ok.is_null(), "the live rollup covers this query: {res}");
        assert_eq!(
            ok.get("stale_checked").and_then(|v| v.as_bool()),
            Some(true)
        );
        // Guards the fixture as much as the check: a measure the entry does not
        // describe compiles to `NULL AS ...`, which is still a non-null
        // resolution and would pass the assertions above.
        assert!(
            ok.get("reagg_sql")
                .and_then(|v| v.as_str())
                .unwrap()
                .contains("total__sum"),
            "reads the stored column: {res}"
        );

        let stale_args = json!({
            "rows": warehouse_rows("deadbeef"),
            "query": status_query(),
            "schema": "airlayer",
            "dialect": "postgres",
            "views": [ORDERS_VIEW],
        })
        .to_string();
        let stale = call(airlayer_cache_resolve_warehouse, &stale_args);
        assert_eq!(stale, json!({ "ok": serde_json::Value::Null }));
    }
