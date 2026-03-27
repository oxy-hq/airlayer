//! Database executors — run compiled SQL against real databases and return JSON results.
//!
//! Gated behind `exec-*` feature flags so the core semantic engine stays dependency-free.
//! Enable individual drivers or `exec` (all) as needed.

#[cfg(feature = "exec-postgres")]
pub mod postgres;
#[cfg(feature = "exec-mysql")]
pub mod mysql;
#[cfg(feature = "exec-snowflake")]
pub mod snowflake;
#[cfg(feature = "exec-bigquery")]
pub mod bigquery;
#[cfg(feature = "exec-clickhouse")]
pub mod clickhouse;
#[cfg(feature = "exec-databricks")]
pub mod databricks;
#[cfg(feature = "exec-duckdb")]
pub mod duckdb;
#[cfg(feature = "exec-sqlite")]
pub mod sqlite;
#[cfg(feature = "exec-domo")]
pub mod domo;
#[cfg(feature = "exec-motherduck")]
pub mod motherduck;

pub mod introspect;

use crate::engine::query::{ColumnKind, ColumnMeta};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

/// Maximum number of rows to include in the envelope `data` field.
/// Keeps output manageable for LLM consumption. `row_count` always reflects the true total.
const MAX_DATA_ROWS: usize = 50;

/// Structured output envelope for query execution.
/// Designed for machine consumption — an LLM can inspect `status` + `error` to diagnose
/// failures, read `sql` to understand what the semantic layer compiled, and iterate on
/// `.view.yml` files informed by `views_used`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryEnvelope {
    /// "success", "parse_error", "compile_error", or "execution_error"
    pub status: String,
    /// The generated SQL (present for compile_error and execution_error too, null for parse_error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    /// Column metadata: name (SQL alias), member path, kind (dimension/measure).
    pub columns: Vec<EnvelopeColumn>,
    /// Result rows (capped at 50). Each row is a JSON object keyed by column name.
    pub data: Vec<serde_json::Map<String, JsonValue>>,
    /// Total number of rows returned by the database (may exceed `data.len()`).
    pub row_count: usize,
    /// Which .view.yml view names were referenced by this query.
    pub views_used: Vec<String>,
    /// Error message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EnvelopeColumn {
    /// The SQL alias used in the SELECT (e.g., "events__platform").
    pub name: String,
    /// The semantic member path (e.g., "events.platform").
    pub member: String,
    /// "dimension", "measure", or "time_dimension".
    pub kind: String,
}

impl EnvelopeColumn {
    pub fn from_meta(meta: &ColumnMeta) -> Self {
        Self {
            name: meta.alias.clone(),
            member: meta.member.clone(),
            kind: match meta.kind {
                ColumnKind::Dimension => "dimension".to_string(),
                ColumnKind::Measure => "measure".to_string(),
                ColumnKind::TimeDimension => "time_dimension".to_string(),
            },
        }
    }
}

impl QueryEnvelope {
    /// Build a success envelope from execution results + compilation metadata.
    pub fn success(
        sql: String,
        columns: &[ColumnMeta],
        exec_result: ExecutionResult,
        views_used: Vec<String>,
    ) -> Self {
        let row_count = exec_result.rows.len();
        let data: Vec<_> = exec_result.rows.into_iter().take(MAX_DATA_ROWS).collect();
        Self {
            status: "success".to_string(),
            sql: Some(sql),
            columns: columns.iter().map(EnvelopeColumn::from_meta).collect(),
            data,
            row_count,
            views_used,
            error: None,
        }
    }

    /// Build an error envelope. `sql` is included if compilation succeeded before the error.
    pub fn error(
        status: &str,
        error: String,
        sql: Option<String>,
        columns: &[ColumnMeta],
        views_used: Vec<String>,
    ) -> Self {
        Self {
            status: status.to_string(),
            sql,
            columns: columns.iter().map(EnvelopeColumn::from_meta).collect(),
            data: vec![],
            row_count: 0,
            views_used,
            error: Some(error),
        }
    }
}

/// The result of executing a query: column names + rows of JSON values.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Map<String, JsonValue>>,
}

/// Execute a SQL query against a database, dispatching based on the database config.
#[allow(unused_variables)]
pub fn execute(
    config: &DatabaseConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    match config {
        #[cfg(feature = "exec-postgres")]
        DatabaseConnection::Postgres(pg) => postgres::execute(pg, sql, params),
        #[cfg(feature = "exec-postgres")]
        DatabaseConnection::Redshift(pg) => postgres::execute(pg, sql, params),
        #[cfg(feature = "exec-mysql")]
        DatabaseConnection::Mysql(my) => mysql::execute(my, sql, params),
        #[cfg(feature = "exec-snowflake")]
        DatabaseConnection::Snowflake(sf) => snowflake::execute(sf, sql, params),
        #[cfg(feature = "exec-bigquery")]
        DatabaseConnection::Bigquery(bq) => bigquery::execute(bq, sql, params),
        #[cfg(feature = "exec-clickhouse")]
        DatabaseConnection::Clickhouse(ch) => clickhouse::execute(ch, sql, params),
        #[cfg(feature = "exec-databricks")]
        DatabaseConnection::Databricks(db) => databricks::execute(db, sql, params),
        #[cfg(feature = "exec-duckdb")]
        DatabaseConnection::DuckDb(duck) => duckdb::execute(duck, sql, params),
        #[cfg(feature = "exec-sqlite")]
        DatabaseConnection::Sqlite(sq) => sqlite::execute(sq, sql, params),
        #[cfg(feature = "exec-domo")]
        DatabaseConnection::Domo(domo) => domo::execute(domo, sql, params),
        #[cfg(feature = "exec-motherduck")]
        DatabaseConnection::MotherDuck(md) => motherduck::execute(md, sql, params),
        // When no exec-* features are enabled, or an unrecognized type is deserialized
        #[allow(unreachable_patterns)]
        _ => Err(EngineError::QueryError(
            "No executor available for this database type. \
             Enable the appropriate feature flag (e.g., exec-postgres, exec-snowflake, exec-duckdb)."
                .to_string(),
        )),
    }
}

/// Database connection configuration — the full connection details needed to execute queries.
/// Parsed from config.yml `databases` entries.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseConnection {
    #[cfg(feature = "exec-postgres")]
    Postgres(PostgresConnection),
    #[cfg(feature = "exec-postgres")]
    Redshift(PostgresConnection),
    #[cfg(feature = "exec-mysql")]
    Mysql(MySqlConnection),
    #[cfg(feature = "exec-snowflake")]
    Snowflake(SnowflakeConnection),
    #[cfg(feature = "exec-bigquery")]
    Bigquery(BigQueryConnection),
    #[cfg(feature = "exec-clickhouse")]
    Clickhouse(ClickHouseConnection),
    #[cfg(feature = "exec-databricks")]
    Databricks(DatabricksConnection),
    #[cfg(feature = "exec-duckdb")]
    #[serde(rename = "duckdb")]
    DuckDb(DuckDbConnection),
    #[cfg(feature = "exec-sqlite")]
    Sqlite(SqliteConnection),
    #[cfg(feature = "exec-domo")]
    Domo(DomoConnection),
    #[cfg(feature = "exec-motherduck")]
    #[serde(rename = "motherduck")]
    MotherDuck(MotherDuckConnection),
}

impl DatabaseConnection {
    /// Get the dialect name for this connection.
    pub fn dialect_str(&self) -> &str {
        match self {
            #[cfg(feature = "exec-postgres")]
            DatabaseConnection::Postgres(_) => "postgres",
            #[cfg(feature = "exec-postgres")]
            DatabaseConnection::Redshift(_) => "redshift",
            #[cfg(feature = "exec-mysql")]
            DatabaseConnection::Mysql(_) => "mysql",
            #[cfg(feature = "exec-snowflake")]
            DatabaseConnection::Snowflake(_) => "snowflake",
            #[cfg(feature = "exec-bigquery")]
            DatabaseConnection::Bigquery(_) => "bigquery",
            #[cfg(feature = "exec-clickhouse")]
            DatabaseConnection::Clickhouse(_) => "clickhouse",
            #[cfg(feature = "exec-databricks")]
            DatabaseConnection::Databricks(_) => "databricks",
            #[cfg(feature = "exec-duckdb")]
            DatabaseConnection::DuckDb(_) => "duckdb",
            #[cfg(feature = "exec-sqlite")]
            DatabaseConnection::Sqlite(_) => "sqlite",
            #[cfg(feature = "exec-domo")]
            DatabaseConnection::Domo(_) => "domo",
            #[cfg(feature = "exec-motherduck")]
            DatabaseConnection::MotherDuck(_) => "motherduck",
            #[allow(unreachable_patterns)]
            _ => "unknown",
        }
    }
}

// ---------------------------------------------------------------------------
// Connection config structs
// ---------------------------------------------------------------------------

#[cfg(feature = "exec-postgres")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PostgresConnection {
    pub name: String,
    #[serde(default = "default_localhost")]
    pub host: Option<String>,
    pub host_var: Option<String>,
    #[serde(default)]
    pub port: Option<String>,
    pub port_var: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    pub user_var: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub password_var: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    pub database_var: Option<String>,
}

#[cfg(feature = "exec-postgres")]
impl PostgresConnection {
    pub fn get_host(&self) -> String {
        resolve_value(&self.host, &self.host_var, "localhost")
    }
    pub fn get_port(&self) -> String {
        resolve_value(&self.port, &self.port_var, "5432")
    }
    pub fn get_user(&self) -> String {
        resolve_value(&self.user, &self.user_var, "postgres")
    }
    pub fn get_password(&self) -> Result<String, EngineError> {
        resolve_required(&self.password, &self.password_var, "password")
    }
    pub fn get_database(&self) -> String {
        resolve_value(&self.database, &self.database_var, "postgres")
    }

    pub fn connection_string(&self) -> Result<String, EngineError> {
        // libpq key=value format requires single-quoting values that contain
        // spaces, backslashes, or single quotes. Escape internal ' as \' and \ as \\.
        fn quote_libpq(val: &str) -> String {
            if val.contains(['\'', '\\', ' ', '=']) {
                let escaped = val.replace('\\', "\\\\").replace('\'', "\\'");
                format!("'{}'", escaped)
            } else {
                val.to_string()
            }
        }
        Ok(format!(
            "host={} port={} user={} password={} dbname={}",
            quote_libpq(&self.get_host()),
            quote_libpq(&self.get_port()),
            quote_libpq(&self.get_user()),
            quote_libpq(&self.get_password()?),
            quote_libpq(&self.get_database()),
        ))
    }
}

#[cfg(feature = "exec-mysql")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MySqlConnection {
    pub name: String,
    #[serde(default = "default_localhost")]
    pub host: Option<String>,
    pub host_var: Option<String>,
    #[serde(default)]
    pub port: Option<String>,
    pub port_var: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    pub user_var: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub password_var: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    pub database_var: Option<String>,
}

#[cfg(feature = "exec-mysql")]
impl MySqlConnection {
    pub fn get_host(&self) -> String {
        resolve_value(&self.host, &self.host_var, "localhost")
    }
    pub fn get_port(&self) -> String {
        resolve_value(&self.port, &self.port_var, "3306")
    }
    pub fn get_user(&self) -> String {
        resolve_value(&self.user, &self.user_var, "root")
    }
    pub fn get_password(&self) -> Result<String, EngineError> {
        resolve_required(&self.password, &self.password_var, "password")
    }
    pub fn get_database(&self) -> String {
        resolve_value(&self.database, &self.database_var, "mysql")
    }
}

#[cfg(feature = "exec-snowflake")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SnowflakeConnection {
    pub name: String,
    pub account: Option<String>,
    pub account_var: Option<String>,
    /// Username for authentication. Also accepts "user" in YAML.
    #[serde(alias = "user")]
    pub username: Option<String>,
    pub username_var: Option<String>,
    pub password: Option<String>,
    pub password_var: Option<String>,
    pub warehouse: Option<String>,
    pub warehouse_var: Option<String>,
    pub database: Option<String>,
    pub database_var: Option<String>,
    pub schema: Option<String>,
    pub schema_var: Option<String>,
    pub role: Option<String>,
}

#[cfg(feature = "exec-snowflake")]
impl SnowflakeConnection {
    pub fn get_account(&self) -> Result<String, EngineError> {
        resolve_required(&self.account, &self.account_var, "account")
    }
    pub fn get_username(&self) -> Result<String, EngineError> {
        resolve_required(&self.username, &self.username_var, "username")
    }
    pub fn get_password(&self) -> Result<String, EngineError> {
        resolve_required(&self.password, &self.password_var, "password")
    }
    pub fn get_warehouse(&self) -> String {
        resolve_value(&self.warehouse, &self.warehouse_var, "COMPUTE_WH")
    }
    pub fn get_database(&self) -> Option<String> {
        resolve_optional(&self.database, &self.database_var)
    }
    pub fn get_schema(&self) -> Option<String> {
        resolve_optional(&self.schema, &self.schema_var)
    }
}

#[cfg(feature = "exec-bigquery")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BigQueryConnection {
    pub name: String,
    /// GCP project ID.
    pub project: Option<String>,
    pub project_var: Option<String>,
    /// OAuth2 access token (e.g., from `gcloud auth print-access-token`).
    pub access_token: Option<String>,
    pub access_token_var: Option<String>,
    /// Default dataset for unqualified table references.
    pub dataset: Option<String>,
}

#[cfg(feature = "exec-bigquery")]
impl BigQueryConnection {
    pub fn get_project(&self) -> Result<String, EngineError> {
        resolve_required(&self.project, &self.project_var, "project")
    }
    pub fn get_access_token(&self) -> Result<String, EngineError> {
        resolve_required(&self.access_token, &self.access_token_var, "access_token")
    }
}

#[cfg(feature = "exec-clickhouse")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClickHouseConnection {
    pub name: String,
    /// HTTP URL (e.g., "http://localhost:8123").
    pub host: Option<String>,
    pub host_var: Option<String>,
    #[serde(default)]
    pub port: Option<String>,
    pub port_var: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    pub user_var: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub password_var: Option<String>,
    pub database: Option<String>,
}

#[cfg(feature = "exec-clickhouse")]
impl ClickHouseConnection {
    pub fn get_host(&self) -> String {
        resolve_value(&self.host, &self.host_var, "http://localhost")
    }
    pub fn get_port(&self) -> String {
        resolve_value(&self.port, &self.port_var, "8123")
    }
    pub fn get_user(&self) -> Option<String> {
        resolve_optional(&self.user, &self.user_var)
    }
    pub fn get_password(&self) -> Option<String> {
        resolve_optional(&self.password, &self.password_var)
    }
}

#[cfg(feature = "exec-databricks")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DatabricksConnection {
    pub name: String,
    /// Databricks workspace host (e.g., "dbc-abc123.cloud.databricks.com").
    pub host: Option<String>,
    pub host_var: Option<String>,
    /// Personal access token.
    pub token: Option<String>,
    pub token_var: Option<String>,
    /// SQL warehouse ID.
    pub warehouse_id: Option<String>,
    pub warehouse_id_var: Option<String>,
    /// Default catalog.
    pub catalog: Option<String>,
    /// Default schema.
    pub schema: Option<String>,
}

#[cfg(feature = "exec-databricks")]
impl DatabricksConnection {
    pub fn get_host(&self) -> Result<String, EngineError> {
        resolve_required(&self.host, &self.host_var, "host")
    }
    pub fn get_token(&self) -> Result<String, EngineError> {
        resolve_required(&self.token, &self.token_var, "token")
    }
    pub fn get_warehouse_id(&self) -> Result<String, EngineError> {
        resolve_required(&self.warehouse_id, &self.warehouse_id_var, "warehouse_id")
    }
}

#[cfg(feature = "exec-duckdb")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DuckDbConnection {
    pub name: String,
    /// Path to a DuckDB file, or empty/omitted for in-memory.
    pub path: Option<String>,
    /// Directory to load files from as tables (like oxy's file_search_path).
    pub file_search_path: Option<String>,
}

#[cfg(feature = "exec-motherduck")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MotherDuckConnection {
    pub name: String,
    /// MotherDuck authentication token.
    pub token: Option<String>,
    pub token_var: Option<String>,
    /// MotherDuck database name (e.g., "my_db"). Omit to use the default cloud database.
    pub database: Option<String>,
}

#[cfg(feature = "exec-motherduck")]
impl MotherDuckConnection {
    pub fn get_token(&self) -> Result<String, EngineError> {
        resolve_required(&self.token, &self.token_var, "token")
    }

    /// Build the `md:` connection string used by the DuckDB driver.
    pub fn connection_string(&self) -> Result<String, EngineError> {
        let token = self.get_token()?;
        let base = match &self.database {
            Some(db) if !db.is_empty() => format!("md:{}", db),
            _ => "md:".to_string(),
        };
        Ok(format!("{}?motherduck_token={}", base, token))
    }
}

#[cfg(feature = "exec-sqlite")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqliteConnection {
    pub name: String,
    /// Path to a SQLite file, or empty/omitted for in-memory.
    pub path: Option<String>,
}

#[cfg(feature = "exec-domo")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DomoConnection {
    pub name: String,
    /// Domo instance name (e.g., "mycompany" → mycompany.domo.com).
    pub instance: String,
    /// Domo developer token (or use developer_token_var).
    pub developer_token: Option<String>,
    pub developer_token_var: Option<String>,
    /// Dataset ID to query.
    pub dataset_id: String,
}

#[cfg(feature = "exec-domo")]
impl DomoConnection {
    pub fn get_developer_token(&self) -> Result<String, EngineError> {
        resolve_required(&self.developer_token, &self.developer_token_var, "developer_token")
    }
}

// --- helpers ---

fn resolve_value(direct: &Option<String>, var: &Option<String>, default: &str) -> String {
    if let Some(v) = direct {
        if !v.is_empty() {
            return v.clone();
        }
    }
    if let Some(var_name) = var {
        if let Ok(v) = std::env::var(var_name) {
            if !v.is_empty() {
                return v;
            }
        }
    }
    default.to_string()
}

fn resolve_optional(direct: &Option<String>, var: &Option<String>) -> Option<String> {
    if let Some(v) = direct {
        if !v.is_empty() {
            return Some(v.clone());
        }
    }
    if let Some(var_name) = var {
        if let Ok(v) = std::env::var(var_name) {
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn resolve_required(
    direct: &Option<String>,
    var: &Option<String>,
    field_name: &str,
) -> Result<String, EngineError> {
    if let Some(v) = direct {
        if !v.is_empty() {
            return Ok(v.clone());
        }
    }
    if let Some(var_name) = var {
        if let Ok(v) = std::env::var(var_name) {
            if !v.is_empty() {
                return Ok(v);
            }
        }
        return Err(EngineError::QueryError(format!(
            "Environment variable '{}' for {} is not set or empty",
            var_name, field_name
        )));
    }
    Err(EngineError::QueryError(format!(
        "No {} configured (provide the value directly or via _var env reference)",
        field_name
    )))
}

fn default_localhost() -> Option<String> {
    Some("localhost".to_string())
}

/// Config file structure for execution — extends PartialConfig with connection details.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub databases: Vec<serde_json::Value>,
}

impl ExecutionConfig {
    /// Find a database connection by name and deserialize it.
    pub fn find_connection(&self, datasource: &str) -> Result<DatabaseConnection, EngineError> {
        for db in &self.databases {
            if db.get("name").and_then(|n| n.as_str()) == Some(datasource) {
                let conn: DatabaseConnection = serde_json::from_value(db.clone()).map_err(|e| {
                    EngineError::QueryError(format!(
                        "Failed to parse connection config for '{}': {}",
                        datasource, e
                    ))
                })?;
                return Ok(conn);
            }
        }
        Err(EngineError::QueryError(format!(
            "No database '{}' found in config",
            datasource
        )))
    }

    /// Get the first database connection (default).
    pub fn first_connection(&self) -> Result<DatabaseConnection, EngineError> {
        let first = self.databases.first().ok_or_else(|| {
            EngineError::QueryError("No databases configured in config.yml".to_string())
        })?;
        let name = first
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        serde_json::from_value(first.clone()).map_err(|e| {
            EngineError::QueryError(format!(
                "Failed to parse connection config for '{}': {}",
                name, e
            ))
        })
    }
}

#[cfg(test)]
#[cfg(feature = "exec-motherduck")]
mod tests {
    use super::*;

    #[test]
    fn test_motherduck_config_deserializes() {
        let json = serde_json::json!({
            "name": "cloud",
            "type": "motherduck",
            "token": "test_token_123",
            "database": "my_db"
        });

        let config: ExecutionConfig = serde_json::from_value(serde_json::json!({
            "databases": [json]
        }))
        .expect("parse config");

        let conn = config.find_connection("cloud").expect("find connection");
        assert_eq!(conn.dialect_str(), "motherduck");
    }

    #[test]
    fn test_motherduck_connection_string_with_database() {
        let conn = MotherDuckConnection {
            name: "test".to_string(),
            token: Some("tok123".to_string()),
            token_var: None,
            database: Some("my_db".to_string()),
        };
        let cs = conn.connection_string().expect("conn string");
        assert_eq!(cs, "md:my_db?motherduck_token=tok123");
    }

    #[test]
    fn test_motherduck_connection_string_without_database() {
        let conn = MotherDuckConnection {
            name: "test".to_string(),
            token: Some("tok123".to_string()),
            token_var: None,
            database: None,
        };
        let cs = conn.connection_string().expect("conn string");
        assert_eq!(cs, "md:?motherduck_token=tok123");
    }

    #[test]
    fn test_motherduck_token_required() {
        let conn = MotherDuckConnection {
            name: "test".to_string(),
            token: None,
            token_var: None,
            database: None,
        };
        assert!(conn.connection_string().is_err());
    }
}
