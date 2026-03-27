//! Database schema introspection — discover tables, columns, and types from the warehouse.
//!
//! Used by `airlayer inspect --schema` to give agents (or humans) a structured catalog
//! of what's in the database, so they can bootstrap `.view.yml` files.

use super::{DatabaseConnection, ExecutionResult};
use crate::engine::EngineError;
use serde::Serialize;
use std::collections::BTreeMap;

/// A table discovered in the database.
#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    /// Schema/dataset name (e.g., "public", "analytics").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Table name.
    pub name: String,
    /// Columns in ordinal order.
    pub columns: Vec<ColumnInfo>,
}

/// A column discovered in the database.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Database-reported data type (e.g., "varchar", "INT64", "timestamp with time zone").
    #[serde(rename = "type")]
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// 1-based ordinal position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<i64>,
}

/// The full schema introspection result.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaInfo {
    /// Database type (e.g., "postgres", "bigquery").
    pub database_type: String,
    /// Discovered tables with their columns.
    pub tables: Vec<TableInfo>,
}

/// Generate the information_schema SQL for a given database connection.
/// Returns (sql, has_schema_column) — some databases don't have a schema concept.
fn introspection_sql(config: &DatabaseConnection) -> Result<(String, bool), EngineError> {
    match config {
        #[cfg(feature = "exec-postgres")]
        DatabaseConnection::Postgres(_) | DatabaseConnection::Redshift(_) => {
            Ok((
                "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                 CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                    .to_string(),
                true,
            ))
        }
        #[cfg(feature = "exec-mysql")]
        DatabaseConnection::Mysql(_) => {
            Ok((
                "SELECT table_schema, table_name, column_name, column_type AS data_type, ordinal_position, \
                 CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                 ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                    .to_string(),
                true,
            ))
        }
        #[cfg(feature = "exec-snowflake")]
        DatabaseConnection::Snowflake(_) => {
            Ok((
                "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                 CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('INFORMATION_SCHEMA') \
                 ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                    .to_string(),
                true,
            ))
        }
        #[cfg(feature = "exec-bigquery")]
        DatabaseConnection::Bigquery(bq) => {
            // BigQuery requires dataset-scoped INFORMATION_SCHEMA
            let dataset = bq.dataset.as_deref().unwrap_or("*");
            if dataset == "*" {
                // Can't query all datasets at once in BigQuery — need INFORMATION_SCHEMA.COLUMNS
                // from region. Use INFORMATION_SCHEMA.COLUMNS from the project level (requires region).
                // For simplicity, require a dataset.
                return Err(EngineError::QueryError(
                    "BigQuery --schema requires a dataset configured in config.yml \
                     (add 'dataset: your_dataset' to the database entry)"
                        .to_string(),
                ));
            }
            let safe_dataset = dataset.replace('`', "");
            Ok((
                format!(
                    "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                     CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                     FROM `{safe_dataset}`.INFORMATION_SCHEMA.COLUMNS \
                     ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                ),
                true,
            ))
        }
        #[cfg(feature = "exec-clickhouse")]
        DatabaseConnection::Clickhouse(ch) => {
            let db_filter = if let Some(ref db) = ch.database {
                format!("database = '{}'", db.replace('\'', "''"))
            } else {
                "database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')".to_string()
            };
            Ok((
                format!(
                    "SELECT database AS table_schema, table AS table_name, name AS column_name, \
                     type AS data_type, position AS ordinal_position, \
                     0 AS nullable \
                     FROM system.columns \
                     WHERE {} \
                     ORDER BY database, table, position \
                     LIMIT 50000",
                    db_filter
                ),
                true,
            ))
        }
        #[cfg(feature = "exec-databricks")]
        DatabaseConnection::Databricks(db) => {
            let mut conditions = vec![];
            if let Some(ref catalog) = db.catalog {
                conditions.push(format!("table_catalog = '{}'", catalog.replace('\'', "''")));
            }
            if let Some(ref schema) = db.schema {
                conditions.push(format!("table_schema = '{}'", schema.replace('\'', "''")));
            } else {
                conditions.push(
                    "table_schema NOT IN ('information_schema')".to_string(),
                );
            }
            let where_clause = conditions.join(" AND ");
            Ok((
                format!(
                    "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                     CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                     FROM information_schema.columns \
                     WHERE {} \
                     ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000",
                    where_clause
                ),
                true,
            ))
        }
        #[cfg(feature = "exec-duckdb")]
        DatabaseConnection::DuckDb(_) => {
            Ok((
                "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                 CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                    .to_string(),
                true,
            ))
        }
        #[cfg(feature = "exec-motherduck")]
        DatabaseConnection::MotherDuck(_) => {
            Ok((
                "SELECT table_schema, table_name, column_name, data_type, ordinal_position, \
                 CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name, ordinal_position \
                 LIMIT 50000"
                    .to_string(),
                true,
            ))
        }
        #[cfg(feature = "exec-sqlite")]
        DatabaseConnection::Sqlite(_) => {
            // SQLite doesn't have information_schema. We'll use a two-step approach
            // handled specially in introspect().
            Ok((
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                    .to_string(),
                false,
            ))
        }
        #[cfg(feature = "exec-domo")]
        DatabaseConnection::Domo(_) => {
            Err(EngineError::QueryError(
                "Schema introspection is not supported for Domo".to_string(),
            ))
        }
        #[allow(unreachable_patterns)]
        _ => Err(EngineError::QueryError(
            "No executor available for this database type".to_string(),
        )),
    }
}

/// Run schema introspection against the database and return structured results.
pub fn introspect(config: &DatabaseConnection) -> Result<SchemaInfo, EngineError> {
    let db_type = config.dialect_str().to_string();

    // SQLite needs special handling — no information_schema
    #[cfg(feature = "exec-sqlite")]
    if matches!(config, DatabaseConnection::Sqlite(_)) {
        return introspect_sqlite(config, &db_type);
    }

    let (sql, _has_schema) = introspection_sql(config)?;
    let result = super::execute(config, &sql, &[])?;

    Ok(SchemaInfo {
        database_type: db_type,
        tables: rows_to_tables(&result),
    })
}

/// Convert flat information_schema rows into nested TableInfo structures.
fn rows_to_tables(result: &ExecutionResult) -> Vec<TableInfo> {
    // Group rows by (schema, table_name)
    let mut table_map: BTreeMap<(String, String), Vec<ColumnInfo>> = BTreeMap::new();

    for row in &result.rows {
        let schema = row
            .get("table_schema")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let table = row
            .get("table_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let col_name = row
            .get("column_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let data_type = row
            .get("data_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let ordinal = row
            .get("ordinal_position")
            .and_then(|v| v.as_i64());
        let nullable = row
            .get("nullable")
            .map(|v| match v {
                serde_json::Value::Bool(b) => *b,
                serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
                serde_json::Value::String(s) => s == "true" || s == "YES" || s == "1",
                _ => false,
            })
            .unwrap_or(true);

        table_map
            .entry((schema, table))
            .or_default()
            .push(ColumnInfo {
                name: col_name,
                data_type,
                nullable,
                ordinal,
            });
    }

    table_map
        .into_iter()
        .map(|((schema, name), columns)| TableInfo {
            schema: if schema.is_empty() {
                None
            } else {
                Some(schema)
            },
            name,
            columns,
        })
        .collect()
}

/// SQLite-specific introspection using pragma_table_info.
#[cfg(feature = "exec-sqlite")]
fn introspect_sqlite(
    config: &DatabaseConnection,
    db_type: &str,
) -> Result<SchemaInfo, EngineError> {
    // Step 1: get table names
    let tables_sql =
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
    let tables_result = super::execute(config, tables_sql, &[])?;

    let mut tables = Vec::new();
    for row in &tables_result.rows {
        let table_name = row
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if table_name.is_empty() {
            continue;
        }

        // Step 2: get columns for each table
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table_name);
        let cols_result = super::execute(config, &pragma_sql, &[])?;

        let columns: Vec<ColumnInfo> = cols_result
            .rows
            .iter()
            .map(|col_row| {
                let name = col_row
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let data_type = col_row
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("TEXT")
                    .to_string();
                let notnull = col_row
                    .get("notnull")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let cid = col_row.get("cid").and_then(|v| v.as_i64());

                ColumnInfo {
                    name,
                    data_type,
                    nullable: notnull == 0,
                    ordinal: cid.map(|c| c + 1), // pragma cid is 0-based
                }
            })
            .collect();

        tables.push(TableInfo {
            schema: None,
            name: table_name,
            columns,
        });
    }

    Ok(SchemaInfo {
        database_type: db_type.to_string(),
        tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_tables_groups_correctly() {
        let result = ExecutionResult {
            columns: vec![
                "table_schema".into(),
                "table_name".into(),
                "column_name".into(),
                "data_type".into(),
                "ordinal_position".into(),
                "nullable".into(),
            ],
            rows: vec![
                serde_json::from_str(
                    r#"{"table_schema":"public","table_name":"events","column_name":"id","data_type":"integer","ordinal_position":1,"nullable":false}"#,
                ).unwrap(),
                serde_json::from_str(
                    r#"{"table_schema":"public","table_name":"events","column_name":"name","data_type":"varchar","ordinal_position":2,"nullable":true}"#,
                ).unwrap(),
                serde_json::from_str(
                    r#"{"table_schema":"public","table_name":"users","column_name":"id","data_type":"integer","ordinal_position":1,"nullable":false}"#,
                ).unwrap(),
            ],
        };

        let tables = rows_to_tables(&result);
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "events");
        assert_eq!(tables[0].schema, Some("public".to_string()));
        assert_eq!(tables[0].columns.len(), 2);
        assert_eq!(tables[0].columns[0].name, "id");
        assert_eq!(tables[0].columns[0].nullable, false);
        assert_eq!(tables[0].columns[1].name, "name");
        assert_eq!(tables[0].columns[1].nullable, true);
        assert_eq!(tables[1].name, "users");
        assert_eq!(tables[1].columns.len(), 1);
    }

    #[test]
    fn test_rows_to_tables_handles_empty() {
        let result = ExecutionResult {
            columns: vec![],
            rows: vec![],
        };
        let tables = rows_to_tables(&result);
        assert!(tables.is_empty());
    }

    #[test]
    fn test_nullable_parsing_variants() {
        // Test different nullable representations across databases
        let result = ExecutionResult {
            columns: vec![
                "table_schema".into(), "table_name".into(), "column_name".into(),
                "data_type".into(), "ordinal_position".into(), "nullable".into(),
            ],
            rows: vec![
                // Boolean true
                serde_json::from_str(
                    r#"{"table_schema":"s","table_name":"t","column_name":"a","data_type":"int","ordinal_position":1,"nullable":true}"#,
                ).unwrap(),
                // Number 0 (false)
                serde_json::from_str(
                    r#"{"table_schema":"s","table_name":"t","column_name":"b","data_type":"int","ordinal_position":2,"nullable":0}"#,
                ).unwrap(),
                // String "YES"
                serde_json::from_str(
                    r#"{"table_schema":"s","table_name":"t","column_name":"c","data_type":"int","ordinal_position":3,"nullable":"YES"}"#,
                ).unwrap(),
            ],
        };

        let tables = rows_to_tables(&result);
        assert_eq!(tables[0].columns[0].nullable, true);
        assert_eq!(tables[0].columns[1].nullable, false);
        assert_eq!(tables[0].columns[2].nullable, true);
    }
}
