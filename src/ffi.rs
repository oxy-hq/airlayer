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
///   "query":    { /* QueryRequest */ }
/// }
/// ```
/// Returns `{ "ok": { "reagg_sql", "cache_key", "entry" } }` on a hit,
/// `{ "ok": null }` if no rollup covers the query.
#[no_mangle]
pub extern "C" fn airlayer_cache_resolve(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheResolveArgs| {
        let manifest: preagg::LocalManifest = serde_json::from_value(args.manifest)
            .map_err(|e| format!("Invalid manifest JSON: {e}"))?;
        let request: QueryRequest =
            serde_json::from_value(args.query).map_err(|e| format!("Invalid query JSON: {e}"))?;
        match preagg::resolve_cached(&request, &manifest, None) {
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
///   "dialect": "<dialect name>"
/// }
/// ```
/// Returns `{ "ok": { "reagg_sql", "table_name" } }` on a hit,
/// `{ "ok": null }` if no rollup covers the query.
#[no_mangle]
pub extern "C" fn airlayer_cache_resolve_warehouse(args_json: *const c_char) -> *mut c_char {
    handle_call(args_json, |args: CacheResolveWarehouseArgs| {
        let request: QueryRequest =
            serde_json::from_value(args.query).map_err(|e| format!("Invalid query JSON: {e}"))?;
        let dialect = Dialect::from_str(&args.dialect)
            .ok_or_else(|| format!("Unknown dialect: {}", args.dialect))?;
        let entries = preagg::parse_manifest_rows(&args.rows);
        match preagg::resolve_warehouse(&request, &entries, &args.schema, &dialect, None) {
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
