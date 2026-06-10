//! Google Sheets executor tests.
//!
//! Tier 1: `init_sql` connection setup on the DuckDB executor (no network).
//! Tier 3 (`--ignored`): live checks that require network access to install the
//! `gsheets` community extension into the bundled DuckDB.

/// init_sql statements run on each new connection before the query, so a table
/// created there is visible to the compiled SQL. This is the mechanism the
/// gsheets executor uses to register sheet views.
#[cfg(feature = "exec-duckdb")]
#[test]
fn duckdb_init_sql_runs_before_query() {
    use airlayer::executor::{duckdb, DuckDbConnection};

    let conn = DuckDbConnection {
        name: "test".to_string(),
        path: None,
        file_search_path: None,
        init_sql: vec![
            "CREATE TABLE events (platform VARCHAR, revenue DOUBLE);".to_string(),
            "INSERT INTO events VALUES ('ios', 10.0), ('android', 5.0), ('ios', 2.5);".to_string(),
        ],
    };

    let result = duckdb::execute(
        &conn,
        "SELECT platform, SUM(revenue) AS total FROM events GROUP BY platform ORDER BY total DESC",
        &[],
    )
    .expect("query over init_sql table");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0]["platform"], "ios");
    assert_eq!(result.rows[0]["total"], 12.5);
}

/// A broken init_sql statement surfaces as a clear error, not a confusing
/// failure on the main query.
#[cfg(feature = "exec-duckdb")]
#[test]
fn duckdb_init_sql_error_is_reported() {
    use airlayer::executor::{duckdb, DuckDbConnection};

    let conn = DuckDbConnection {
        name: "test".to_string(),
        path: None,
        file_search_path: None,
        init_sql: vec!["CREATE SYNTAX ERROR".to_string()],
    };

    let err = duckdb::execute(&conn, "SELECT 1", &[]).unwrap_err();
    assert!(err.to_string().contains("init_sql"), "got: {}", err);
}

/// Views registered via init_sql appear in information_schema — the gsheets
/// executor relies on this for `inspect --schema` (sheets are CREATE VIEWs).
#[cfg(feature = "exec-duckdb")]
#[test]
fn duckdb_init_sql_views_are_introspectable() {
    use airlayer::executor::{introspect, DatabaseConnection, DuckDbConnection};

    let conn = DatabaseConnection::DuckDb(DuckDbConnection {
        name: "test".to_string(),
        path: None,
        file_search_path: None,
        init_sql: vec![
            "CREATE TABLE raw (region VARCHAR, revenue DOUBLE);".to_string(),
            "CREATE VIEW orders AS SELECT * FROM raw;".to_string(),
        ],
    });

    let schema = introspect::introspect(&conn).expect("introspect");
    let orders = schema
        .tables
        .iter()
        .find(|t| t.name == "orders")
        .expect("orders view visible in information_schema");
    assert_eq!(orders.columns.len(), 2);
}

/// duckdb connections without init_sql in the YAML still deserialize (field defaults).
#[cfg(feature = "exec-duckdb")]
#[test]
fn duckdb_config_without_init_sql_deserializes() {
    use airlayer::executor::ExecutionConfig;

    let config: ExecutionConfig = serde_yaml::from_str(
        "databases:\n  - name: local\n    type: duckdb\n    path: ./data.duckdb\n",
    )
    .expect("parse config");
    let conn = config.find_connection("local").expect("find connection");
    assert_eq!(conn.dialect_str(), "duckdb");
}

/// Verifies the bundled duckdb crate can install + load the `gsheets` community
/// extension at runtime. Requires network access (downloads the extension binary).
#[test]
#[ignore]
fn duckdb_bundled_can_install_gsheets_community_extension() {
    let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
    conn.execute_batch("INSTALL gsheets FROM community; LOAD gsheets;")
        .expect("INSTALL/LOAD gsheets from community");

    let n: i64 = conn
        .prepare("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'read_gsheet'")
        .unwrap()
        .query_row([], |r| r.get(0))
        .expect("query duckdb_functions");
    assert!(n > 0, "read_gsheet function not registered after LOAD");
}
