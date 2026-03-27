//! MotherDuck query executor — cloud-hosted DuckDB via the `md:` connection protocol.
//!
//! MotherDuck uses the same DuckDB driver with a `md:<database>?motherduck_token=<token>`
//! connection string. SQL dialect is identical to DuckDB.

use super::{ExecutionResult, MotherDuckConnection};
use crate::engine::EngineError;

pub fn execute(
    config: &MotherDuckConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let conn_str = config.connection_string()?;

    let conn = duckdb::Connection::open(&conn_str)
        .map_err(|e| EngineError::QueryError(format!("Failed to connect to MotherDuck: {}", e)))?;

    // Reuse DuckDB's param rewriting and value conversion
    let rewritten = super::duckdb::rewrite_params(sql);

    let mut stmt = conn.prepare(&rewritten).map_err(|e| {
        EngineError::QueryError(format!("MotherDuck prepare failed: {}", e))
    })?;

    let param_refs: Vec<&dyn duckdb::ToSql> = params
        .iter()
        .map(|p| p as &dyn duckdb::ToSql)
        .collect();

    let mut rows_result = stmt.query(param_refs.as_slice()).map_err(|e| {
        EngineError::QueryError(format!("MotherDuck query failed: {}", e))
    })?;

    let columns: Vec<String> = rows_result
        .as_ref()
        .ok_or_else(|| EngineError::QueryError("MotherDuck: failed to get result set reference".to_string()))?
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::new();
    while let Some(row) = rows_result.next().map_err(|e| {
        EngineError::QueryError(format!("MotherDuck row iteration failed: {}", e))
    })? {
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = super::duckdb::duckdb_value_to_json(row, i);
            obj.insert(col_name.clone(), val);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}
