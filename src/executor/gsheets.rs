//! Google Sheets executor — in-process DuckDB with the `gsheets` community extension.
//!
//! Sheets has no SQL endpoint of its own, so this executor opens an in-memory
//! DuckDB connection, installs/loads the `gsheets` community extension, creates
//! an auth secret, and registers each configured spreadsheet as a view named after
//! its table. The compiled SQL (DuckDB dialect) then runs unchanged.
//!
//! The first query on a machine downloads the extension binary (network required);
//! subsequent loads come from the local extension cache (`~/.duckdb/extensions`).

use super::{ExecutionResult, GSheetsConnection};
use crate::engine::EngineError;

pub fn execute(
    config: &GSheetsConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| EngineError::QueryError(format!("Failed to open DuckDB: {}", e)))?;

    for stmt in config.init_statements()? {
        conn.execute_batch(&stmt).map_err(|e| {
            // Redact the statement on error — the CREATE SECRET statement embeds the token
            let msg = e.to_string();
            let redacted = if stmt.contains("CREATE SECRET") {
                "Google Sheets setup failed creating auth secret (statement redacted)".to_string()
            } else {
                format!("Google Sheets setup failed: {}", msg)
            };
            EngineError::QueryError(redacted)
        })?;
    }

    super::duckdb::execute_on_connection(&conn, sql, params)
}
