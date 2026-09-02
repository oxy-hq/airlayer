//! Database executors — run compiled SQL against real databases and return JSON results.
//!
//! Gated behind `exec-*` feature flags so the core semantic engine stays dependency-free.
//! Enable individual drivers or `exec` (all) as needed.

#[cfg(feature = "exec-bigquery")]
pub mod bigquery;
#[cfg(feature = "exec-clickhouse")]
pub mod clickhouse;
#[cfg(feature = "exec-databricks")]
pub mod databricks;
#[cfg(feature = "exec-domo")]
pub mod domo;
#[cfg(feature = "exec-duckdb")]
pub mod duckdb;
#[cfg(feature = "exec-gsheets")]
pub mod gsheets;
#[cfg(feature = "exec-motherduck")]
pub mod motherduck;
#[cfg(feature = "exec-mysql")]
pub mod mysql;
#[cfg(feature = "exec-postgres")]
pub mod postgres;
#[cfg(feature = "exec-presto")]
pub mod presto;
#[cfg(feature = "exec-snowflake")]
pub mod snowflake;
#[cfg(feature = "exec-sqlite")]
pub mod sqlite;

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
                ColumnKind::MotifComputed => "motif_computed".to_string(),
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
        #[cfg(feature = "exec-gsheets")]
        DatabaseConnection::GSheets(gs) => gsheets::execute(gs, sql, params),
        #[cfg(feature = "exec-presto")]
        DatabaseConnection::Presto(pr) => presto::execute(pr, sql, params),
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
    #[cfg(feature = "exec-gsheets")]
    #[serde(rename = "gsheets")]
    GSheets(GSheetsConnection),
    #[cfg(feature = "exec-presto")]
    Presto(PrestoConnection),
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
            #[cfg(feature = "exec-gsheets")]
            DatabaseConnection::GSheets(_) => "gsheets",
            #[cfg(feature = "exec-presto")]
            DatabaseConnection::Presto(_) => "presto",
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
    /// Expires in ~an hour; prefer a service-account key for unattended use.
    pub access_token: Option<String>,
    pub access_token_var: Option<String>,
    /// Path to a service-account JSON key file. Alternative to
    /// `access_token`/`access_token_var`: airlayer mints its own access token
    /// from the key and refreshes it as it nears expiry.
    pub key_file: Option<String>,
    pub key_file_var: Option<String>,
    /// The service-account JSON key inline, for callers that hold the key in
    /// memory or in an environment variable rather than on disk.
    pub key_json: Option<String>,
    pub key_json_var: Option<String>,
    /// Default dataset for unqualified table references.
    pub dataset: Option<String>,
    /// Access token minted from the service-account key, cached until it nears
    /// expiry. Never deserialized from config, never rendered by `Debug`.
    #[serde(skip)]
    token_cache: TokenCache,
}

/// A minted access token and its absolute expiry, shared across clones of a
/// connection so a cloned connection does not re-mint on every query.
///
/// `Debug` is written by hand: `DatabaseConnection` is `Debug` and reaches
/// error paths and logs, and the derived impl would print the bearer token.
#[cfg(feature = "exec-bigquery")]
#[derive(Clone, Default)]
pub struct TokenCache(std::sync::Arc<std::sync::Mutex<Option<(String, i64)>>>);

#[cfg(feature = "exec-bigquery")]
impl std::fmt::Debug for TokenCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenCache(<redacted>)")
    }
}

#[cfg(feature = "exec-bigquery")]
impl BigQueryConnection {
    pub fn get_project(&self) -> Result<String, EngineError> {
        resolve_required(&self.project, &self.project_var, "project")
    }

    /// A bearer token for the BigQuery REST API.
    ///
    /// An explicitly configured `access_token` wins: it is cheaper than a round
    /// trip to Google, and an operator who set both meant the explicit one.
    /// Otherwise mint one from the service-account key (cached until expiry).
    pub fn get_access_token(&self) -> Result<String, EngineError> {
        if let Some(token) = resolve_optional(&self.access_token, &self.access_token_var) {
            return Ok(token);
        }
        if self.has_service_account_key() {
            return bigquery::auth::access_token(self);
        }
        Err(EngineError::QueryError(
            "bigquery connection requires authentication: set `access_token` (or \
             `access_token_var`) to an OAuth2 access token, or `key_file` (or \
             `key_file_var` / `key_json` / `key_json_var`) to a service-account JSON key"
                .to_string(),
        ))
    }

    /// Whether a service-account key is configured in any of its forms.
    pub fn has_service_account_key(&self) -> bool {
        resolve_optional(&self.key_json, &self.key_json_var).is_some()
            || resolve_optional(&self.key_file, &self.key_file_var).is_some()
    }

    /// The service-account key JSON, inline or read from `key_file`.
    ///
    /// The path is named in errors (a path is not a secret); the file contents
    /// never are.
    pub fn service_account_key_json(&self) -> Result<String, EngineError> {
        if let Some(json) = resolve_optional(&self.key_json, &self.key_json_var) {
            return Ok(json);
        }
        let path = resolve_optional(&self.key_file, &self.key_file_var).ok_or_else(|| {
            EngineError::QueryError(
                "bigquery connection has no service-account key configured".to_string(),
            )
        })?;
        std::fs::read_to_string(&path).map_err(|e| {
            EngineError::QueryError(format!(
                "Failed to read BigQuery service-account key file '{}': {}",
                path,
                e.kind()
            ))
        })
    }

    /// The cached token, if one is present and not within the refresh window of
    /// `now` (epoch seconds).
    pub fn cached_token_at(&self, now: i64) -> Option<String> {
        let guard = self.token_cache.0.lock().ok()?;
        let (token, expires_at) = guard.as_ref()?;
        if bigquery::auth::needs_refresh(*expires_at, now) {
            return None;
        }
        Some(token.clone())
    }

    /// Store a freshly minted token and its absolute expiry (epoch seconds).
    pub fn cache_token(&self, token: &str, expires_at: i64) {
        if let Ok(mut guard) = self.token_cache.0.lock() {
            *guard = Some((token.to_string(), expires_at));
        }
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
    /// SQL statements run on each new connection before the query — e.g.
    /// `INSTALL`/`LOAD` extensions, `CREATE SECRET`, or `CREATE VIEW` over
    /// external data sources.
    #[serde(default)]
    pub init_sql: Vec<String>,
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

#[cfg(feature = "exec-gsheets")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct GSheetsConnection {
    pub name: String,
    /// Google OAuth access token (the `access_token` secret provider).
    pub token: Option<String>,
    pub token_var: Option<String>,
    /// Path to a service-account JSON key file (the `key_file` secret provider).
    /// Alternative to `token`/`token_var`.
    pub key_file: Option<String>,
    /// Map of table name → spreadsheet. Each entry is registered as a DuckDB view
    /// so compiled SQL can reference the table name directly.
    #[serde(default)]
    pub sheets: std::collections::BTreeMap<String, GSheetSource>,
}

/// A spreadsheet source: either a bare URL/ID string, or a detailed form
/// selecting a specific tab or range.
#[cfg(feature = "exec-gsheets")]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum GSheetSource {
    Url(String),
    Detailed {
        /// Spreadsheet URL or ID.
        url: String,
        /// Tab name within the spreadsheet (gsheets `sheet` parameter).
        sheet: Option<String>,
        /// A1-notation range, e.g. "A1:D100" (gsheets `range` parameter).
        range: Option<String>,
        /// Whether the first row is a header (gsheets `header` parameter).
        header: Option<bool>,
        /// Skip type inference and read every column as VARCHAR.
        all_varchar: Option<bool>,
    },
}

#[cfg(feature = "exec-gsheets")]
impl GSheetsConnection {
    /// SQL statements that prepare a fresh DuckDB connection for Sheets queries:
    /// install/load the community extension, create the auth secret, and register
    /// one view per configured sheet.
    pub fn init_statements(&self) -> Result<Vec<String>, EngineError> {
        let mut stmts = vec!["INSTALL gsheets FROM community; LOAD gsheets;".to_string()];

        let has_token = self.token.as_deref().is_some_and(|t| !t.is_empty())
            || self.token_var.as_deref().is_some_and(|v| !v.is_empty());
        if has_token {
            let token = resolve_required(&self.token, &self.token_var, "token")?;
            stmts.push(format!(
                "CREATE SECRET (TYPE gsheet, PROVIDER access_token, TOKEN '{}');",
                escape_sql_string(&token)
            ));
        } else if let Some(key_file) = self.key_file.as_deref().filter(|k| !k.is_empty()) {
            stmts.push(format!(
                "CREATE SECRET (TYPE gsheet, PROVIDER key_file, FILEPATH '{}');",
                escape_sql_string(key_file)
            ));
        } else {
            return Err(EngineError::QueryError(
                "gsheets connection requires authentication: set `token` (or `token_var`) \
                 to a Google OAuth access token, or `key_file` to a service-account JSON key path"
                    .to_string(),
            ));
        }

        if self.sheets.is_empty() {
            return Err(EngineError::QueryError(
                "gsheets connection has no `sheets` configured — add a map of \
                 table name → spreadsheet URL/ID"
                    .to_string(),
            ));
        }
        for (table, source) in &self.sheets {
            stmts.push(source.create_view_sql(table));
        }
        Ok(stmts)
    }

    /// Like [`init_statements`](Self::init_statements), but registers only the
    /// sheets whose table name is referenced by `sql`. DuckDB binds views
    /// eagerly, so every `CREATE VIEW ... read_gsheet(...)` costs one Sheets
    /// API read — registering unreferenced sheets burns through Google's
    /// per-minute read quota. Falls back to registering all sheets when no
    /// table name matches (e.g. introspection over information_schema).
    pub fn init_statements_for_sql(&self, sql: &str) -> Result<Vec<String>, EngineError> {
        let all = self.init_statements()?;
        let preamble = all.len() - self.sheets.len();

        let any_referenced = self
            .sheets
            .keys()
            .any(|table| sql_references_table(sql, table));
        if !any_referenced {
            return Ok(all);
        }
        Ok(all
            .into_iter()
            .take(preamble)
            .chain(
                self.sheets
                    .iter()
                    .filter(|(table, _)| sql_references_table(sql, table))
                    .map(|(table, source)| source.create_view_sql(table)),
            )
            .collect())
    }
}

/// Whether `sql` references `table` as a standalone identifier (bare or quoted).
#[cfg(feature = "exec-gsheets")]
fn sql_references_table(sql: &str, table: &str) -> bool {
    let pattern = format!(
        r#"(?i)(?:^|[^A-Za-z0-9_]){}(?:[^A-Za-z0-9_]|$)"#,
        regex::escape(table)
    );
    regex::Regex::new(&pattern)
        .map(|re| re.is_match(sql))
        .unwrap_or(true)
}

#[cfg(feature = "exec-gsheets")]
impl GSheetSource {
    /// `CREATE VIEW "<table>" AS SELECT * FROM read_gsheet(...)` for this source.
    fn create_view_sql(&self, table: &str) -> String {
        let (url, mut args) = match self {
            GSheetSource::Url(url) => (url, Vec::new()),
            GSheetSource::Detailed {
                url,
                sheet,
                range,
                header,
                all_varchar,
            } => {
                let mut args = Vec::new();
                if let Some(s) = sheet {
                    args.push(format!("sheet='{}'", escape_sql_string(s)));
                }
                if let Some(r) = range {
                    args.push(format!("range='{}'", escape_sql_string(r)));
                }
                if let Some(h) = header {
                    args.push(format!("header={}", h));
                }
                if let Some(av) = all_varchar {
                    args.push(format!("all_varchar={}", av));
                }
                (url, args)
            }
        };
        args.insert(0, format!("'{}'", escape_sql_string(url)));
        format!(
            "CREATE VIEW \"{}\" AS SELECT * FROM read_gsheet({});",
            table.replace('"', "\"\""),
            args.join(", ")
        )
    }
}

/// Escape a string literal for SQL (standard doubled single-quote).
#[cfg(feature = "exec-gsheets")]
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(feature = "exec-sqlite")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqliteConnection {
    pub name: String,
    /// Path to a SQLite file, or empty/omitted for in-memory.
    pub path: Option<String>,
}

#[cfg(feature = "exec-presto")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PrestoConnection {
    pub name: String,
    /// Presto/Trino coordinator host (e.g., "http://localhost").
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
    /// Catalog name (e.g., "memory", "hive", "iceberg").
    pub catalog: Option<String>,
    /// Schema/database name.
    pub schema: Option<String>,
}

#[cfg(feature = "exec-presto")]
impl PrestoConnection {
    pub fn get_host(&self) -> String {
        resolve_value(&self.host, &self.host_var, "http://localhost")
    }
    pub fn get_port(&self) -> String {
        resolve_value(&self.port, &self.port_var, "8080")
    }
    pub fn get_user(&self) -> String {
        resolve_value(&self.user, &self.user_var, "presto")
    }
    pub fn get_password(&self) -> Option<String> {
        resolve_optional(&self.password, &self.password_var)
    }
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
        resolve_required(
            &self.developer_token,
            &self.developer_token_var,
            "developer_token",
        )
    }
}

// --- helpers ---

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

/// Build a `DatabaseConnection` from a map of field values + a database type string.
/// This allows constructing a connection from interactive prompt results without
/// going through config.yml serialization/deserialization.
pub fn build_connection_from_fields(
    db_type: &str,
    fields: &std::collections::BTreeMap<String, String>,
) -> Result<DatabaseConnection, EngineError> {
    let mut json_map = serde_json::Map::new();
    json_map.insert(
        "type".to_string(),
        serde_json::Value::String(db_type.to_string()),
    );
    for (k, v) in fields {
        // gsheets prompts collect a flat sheet_url/sheet_table pair; fold them
        // into the nested `sheets` map the connection config expects.
        if db_type == "gsheets" && (k == "sheet_url" || k == "sheet_table") {
            continue;
        }
        json_map.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    if db_type == "gsheets" {
        if let Some(url) = fields.get("sheet_url").filter(|u| !u.is_empty()) {
            let table = fields
                .get("sheet_table")
                .map(|s| s.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("sheet1");
            let mut sheets = serde_json::Map::new();
            sheets.insert(table.to_string(), serde_json::Value::String(url.clone()));
            json_map.insert("sheets".to_string(), serde_json::Value::Object(sheets));
        }
    }
    // Ensure "name" is always set
    if !json_map.contains_key("name") {
        json_map.insert(
            "name".to_string(),
            serde_json::Value::String("warehouse".to_string()),
        );
    }
    let json_value = serde_json::Value::Object(json_map);
    serde_json::from_value(json_value).map_err(|e| {
        EngineError::QueryError(format!(
            "Failed to construct {} connection from fields: {}",
            db_type, e
        ))
    })
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

#[cfg(test)]
#[cfg(feature = "exec-gsheets")]
mod gsheets_tests {
    use super::*;

    fn parse_connection(json: serde_json::Value) -> GSheetsConnection {
        match serde_json::from_value(json).expect("parse connection") {
            DatabaseConnection::GSheets(gs) => gs,
            other => panic!("expected gsheets connection, got {}", other.dialect_str()),
        }
    }

    #[test]
    fn test_gsheets_config_deserializes() {
        let json = serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "ya29.token",
            "sheets": {
                "orders": "https://docs.google.com/spreadsheets/d/abc123",
                "customers": {
                    "url": "abc456",
                    "sheet": "Customers",
                    "range": "A1:F500",
                    "header": true
                }
            }
        });

        let config: ExecutionConfig = serde_json::from_value(serde_json::json!({
            "databases": [json]
        }))
        .expect("parse config");

        let conn = config.find_connection("sheets").expect("find connection");
        assert_eq!(conn.dialect_str(), "gsheets");
    }

    #[test]
    fn test_gsheets_init_statements_with_token() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "ya29.token",
            "sheets": { "orders": "https://docs.google.com/spreadsheets/d/abc123" }
        }));

        let stmts = conn.init_statements().expect("init statements");
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "INSTALL gsheets FROM community; LOAD gsheets;");
        assert_eq!(
            stmts[1],
            "CREATE SECRET (TYPE gsheet, PROVIDER access_token, TOKEN 'ya29.token');"
        );
        assert_eq!(
            stmts[2],
            "CREATE VIEW \"orders\" AS SELECT * FROM read_gsheet('https://docs.google.com/spreadsheets/d/abc123');"
        );
    }

    #[test]
    fn test_gsheets_init_statements_with_key_file() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "key_file": "./service-account.json",
            "sheets": { "orders": "abc123" }
        }));

        let stmts = conn.init_statements().expect("init statements");
        assert_eq!(
            stmts[1],
            "CREATE SECRET (TYPE gsheet, PROVIDER key_file, FILEPATH './service-account.json');"
        );
    }

    #[test]
    fn test_gsheets_detailed_source_renders_named_args() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "t",
            "sheets": {
                "customers": {
                    "url": "abc456",
                    "sheet": "Customers",
                    "range": "A1:F500",
                    "header": true,
                    "all_varchar": false
                }
            }
        }));

        let stmts = conn.init_statements().expect("init statements");
        assert_eq!(
            stmts[2],
            "CREATE VIEW \"customers\" AS SELECT * FROM read_gsheet('abc456', \
             sheet='Customers', range='A1:F500', header=true, all_varchar=false);"
        );
    }

    #[test]
    fn test_gsheets_escapes_quotes() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "tok'en",
            "sheets": { "or\"ders": { "url": "ab'c", "sheet": "O'Brien" } }
        }));

        let stmts = conn.init_statements().expect("init statements");
        assert!(stmts[1].contains("'tok''en'"));
        assert_eq!(
            stmts[2],
            "CREATE VIEW \"or\"\"ders\" AS SELECT * FROM read_gsheet('ab''c', sheet='O''Brien');"
        );
    }

    #[test]
    fn test_gsheets_requires_auth() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "sheets": { "orders": "abc123" }
        }));
        let err = conn.init_statements().unwrap_err();
        assert!(err.to_string().contains("requires authentication"));
    }

    #[test]
    fn test_gsheets_requires_sheets() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "t"
        }));
        let err = conn.init_statements().unwrap_err();
        assert!(err.to_string().contains("no `sheets` configured"));
    }

    #[test]
    fn test_gsheets_token_var_resolves_from_env() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token_var": "AIRLAYER_TEST_GSHEET_TOKEN",
            "sheets": { "orders": "abc123" }
        }));
        std::env::set_var("AIRLAYER_TEST_GSHEET_TOKEN", "env_token");
        let stmts = conn.init_statements().expect("init statements");
        std::env::remove_var("AIRLAYER_TEST_GSHEET_TOKEN");
        assert!(stmts[1].contains("TOKEN 'env_token'"));
    }

    #[test]
    fn test_gsheets_init_for_sql_registers_only_referenced_sheets() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "t",
            "sheets": { "orders": "a", "customers": "b", "order_items": "c" }
        }));

        let stmts = conn
            .init_statements_for_sql("SELECT * FROM orders AS \"orders\" GROUP BY 1")
            .expect("init statements");
        // install/load + secret + only the `orders` view — not customers,
        // and not order_items despite `orders` being a substring of it
        assert_eq!(stmts.len(), 3);
        assert!(stmts[2].contains("CREATE VIEW \"orders\""));

        let stmts = conn
            .init_statements_for_sql("SELECT * FROM \"customers\" JOIN order_items ON 1=1")
            .expect("init statements");
        assert_eq!(stmts.len(), 4);
        assert!(stmts[2].contains("CREATE VIEW \"customers\""));
        assert!(stmts[3].contains("CREATE VIEW \"order_items\""));
    }

    #[test]
    fn test_gsheets_init_for_sql_falls_back_to_all_sheets() {
        let conn = parse_connection(serde_json::json!({
            "name": "sheets",
            "type": "gsheets",
            "token": "t",
            "sheets": { "orders": "a", "customers": "b" }
        }));

        // Introspection SQL references no sheet table — register everything
        let stmts = conn
            .init_statements_for_sql("SELECT * FROM information_schema.columns")
            .expect("init statements");
        assert_eq!(stmts.len(), 4);
    }

    #[test]
    fn test_build_connection_from_fields_folds_sheet_pair() {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("token_var".to_string(), "GSHEET_TOKEN".to_string());
        fields.insert("sheet_url".to_string(), "abc123".to_string());
        fields.insert("sheet_table".to_string(), "orders".to_string());

        let conn = build_connection_from_fields("gsheets", &fields).expect("build connection");
        match conn {
            DatabaseConnection::GSheets(gs) => {
                assert_eq!(gs.sheets.len(), 1);
                assert!(matches!(
                    gs.sheets.get("orders"),
                    Some(GSheetSource::Url(u)) if u == "abc123"
                ));
            }
            other => panic!("expected gsheets connection, got {}", other.dialect_str()),
        }
    }
}
