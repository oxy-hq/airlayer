//! DuckDB query executor (in-process).

use super::{DuckDbConnection, ExecutionResult};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &DuckDbConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let conn = match &config.path {
        Some(p) if !p.is_empty() => duckdb::Connection::open(p),
        _ => duckdb::Connection::open_in_memory(),
    }
    .map_err(|e| EngineError::QueryError(format!("Failed to open DuckDB: {}", e)))?;

    // If file_search_path is set, load files as tables
    if let Some(ref fsp) = config.file_search_path {
        load_files(&conn, fsp)?;
    }

    // DuckDB uses ? params, not $1. Rewrite.
    let rewritten = rewrite_params(sql);

    let mut stmt = conn
        .prepare(&rewritten)
        .map_err(|e| EngineError::QueryError(format!("DuckDB prepare failed: {}", e)))?;

    let param_refs: Vec<&dyn duckdb::ToSql> =
        params.iter().map(|p| p as &dyn duckdb::ToSql).collect();

    let mut rows_result = stmt
        .query(param_refs.as_slice())
        .map_err(|e| EngineError::QueryError(format!("DuckDB query failed: {}", e)))?;

    // Get column names from the result set (after execution, not before)
    let columns: Vec<String> = rows_result
        .as_ref()
        .ok_or_else(|| {
            EngineError::QueryError("DuckDB: failed to get result set reference".to_string())
        })?
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::new();
    while let Some(row) = rows_result
        .next()
        .map_err(|e| EngineError::QueryError(format!("DuckDB row iteration failed: {}", e)))?
    {
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = duckdb_value_to_json(row, i);
            obj.insert(col_name.clone(), val);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

pub(crate) fn rewrite_params(sql: &str) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\$(\d+)").unwrap());
    re.replace_all(sql, "?").to_string()
}

pub(crate) fn duckdb_value_to_json(row: &duckdb::Row, idx: usize) -> JsonValue {
    use duckdb::types::Value;

    match row.get::<_, Value>(idx) {
        Ok(val) => match val {
            Value::Null => JsonValue::Null,
            Value::Boolean(b) => JsonValue::Bool(b),
            Value::TinyInt(n) => JsonValue::Number(n.into()),
            Value::SmallInt(n) => JsonValue::Number(n.into()),
            Value::Int(n) => JsonValue::Number(n.into()),
            Value::BigInt(n) => JsonValue::Number(n.into()),
            Value::HugeInt(n) => {
                // HugeInt is i128, serde_json only supports i64/u64/f64
                if let Ok(n64) = i64::try_from(n) {
                    JsonValue::Number(n64.into())
                } else {
                    JsonValue::String(n.to_string())
                }
            }
            Value::UTinyInt(n) => JsonValue::Number(n.into()),
            Value::USmallInt(n) => JsonValue::Number(n.into()),
            Value::UInt(n) => JsonValue::Number(n.into()),
            Value::UBigInt(n) => JsonValue::Number(n.into()),
            Value::Float(f) => serde_json::Number::from_f64(f as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Double(f) => serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Text(s) => JsonValue::String(s),
            Value::Blob(b) => JsonValue::String(format!("<blob {} bytes>", b.len())),
            Value::Timestamp(unit, val_inner) => {
                // Convert to ISO 8601 via chrono
                use duckdb::types::TimeUnit;
                let micros = match unit {
                    TimeUnit::Second => val_inner * 1_000_000,
                    TimeUnit::Millisecond => val_inner * 1_000,
                    TimeUnit::Microsecond => val_inner,
                    TimeUnit::Nanosecond => val_inner / 1_000,
                };
                match chrono::DateTime::from_timestamp_micros(micros) {
                    Some(dt) => JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
                    None => JsonValue::String(format!("{:?}", (unit, val_inner))),
                }
            }
            Value::Date32(d) => {
                // Date32 is days since Unix epoch (1970-01-01)
                match chrono::NaiveDate::from_num_days_from_ce_opt(d + 719_163) {
                    Some(date) => JsonValue::String(date.format("%Y-%m-%d").to_string()),
                    None => JsonValue::String(format!("{}", d)),
                }
            }
            Value::Time64(_, t) => JsonValue::String(format!("{}", t)),
            _ => JsonValue::String(format!("{:?}", val)),
        },
        Err(_) => JsonValue::Null,
    }
}

fn load_files(conn: &duckdb::Connection, dir: &str) -> Result<(), EngineError> {
    let path = std::path::Path::new(dir);
    if !path.is_dir() {
        return Err(EngineError::QueryError(format!(
            "file_search_path '{}' is not a directory",
            dir
        )));
    }

    let entries = std::fs::read_dir(path).map_err(|e| {
        EngineError::QueryError(format!("Failed to read directory '{}': {}", dir, e))
    })?;

    for entry in entries.flatten() {
        let file_path = entry.path();
        let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");

        if !matches!(ext, "csv" | "parquet" | "json" | "jsonl") {
            continue;
        }

        let table_name = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        let escaped_table = table_name.replace('"', "\"\"");
        let escaped_path = file_path.display().to_string().replace('\'', "''");
        let sql = format!(
            "CREATE TEMPORARY TABLE \"{}\" AS FROM '{}'",
            escaped_table, escaped_path
        );

        conn.execute_batch(&sql).map_err(|e| {
            EngineError::QueryError(format!(
                "Failed to load '{}' as table '{}': {}",
                file_path.display(),
                table_name,
                e
            ))
        })?;
    }

    Ok(())
}
