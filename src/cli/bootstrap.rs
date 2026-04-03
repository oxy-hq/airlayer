//! Programmatic view generation from database schema introspection results.
//!
//! Used by `airlayer init` to bootstrap `.view.yml` files after schema discovery.

#[cfg(test)]
use crate::executor::introspect::ColumnInfo;
use crate::executor::introspect::{SchemaInfo, TableInfo};
use convert_case::{Case, Casing};
use std::path::Path;

/// Known system schemas that should be excluded from bootstrapping.
const SYSTEM_SCHEMAS: &[&str] = &[
    // MotherDuck / DuckDB
    "pg_catalog",
    "information_schema",
    // Snowflake
    "INFORMATION_SCHEMA",
    // ClickHouse
    "system",
];

/// Known system tables (MotherDuck metadata tables in `main` schema).
const MOTHERDUCK_SYSTEM_TABLES: &[&str] = &[
    "database_snapshots",
    "databases",
    "owned_shares",
    "query_history",
    "recent_queries",
    "shared_with_me",
    "storage_info",
    "storage_info_history",
];

/// Filter out known system schemas and tables.
pub fn filter_user_tables(schema_info: &SchemaInfo) -> Vec<&TableInfo> {
    schema_info
        .tables
        .iter()
        .filter(|t| {
            // Skip system schemas
            if let Some(ref s) = t.schema {
                if SYSTEM_SCHEMAS.contains(&s.as_str()) {
                    return false;
                }
                // Skip MotherDuck system tables in "main" schema
                if s == "main" && MOTHERDUCK_SYSTEM_TABLES.contains(&t.name.as_str()) {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Map a SQL data type string to a semantic dimension type name.
fn sql_type_to_dim_type(sql_type: &str) -> &'static str {
    let t = sql_type.to_lowercase();
    if t.contains("bool") {
        "boolean"
    } else if t == "date" || (t.contains("date") && !t.contains("time")) {
        "date"
    } else if t.contains("time") || t.contains("timestamp") {
        "datetime"
    } else if t.contains("int")
        || t.contains("float")
        || t.contains("double")
        || t.contains("numeric")
        || t.contains("decimal")
        || t.contains("real")
        || t.contains("number")
        || t == "bigint"
        || t == "ubigint"
    {
        "number"
    } else {
        "string"
    }
}

/// Check if a SQL type is numeric (for generating sum/average measures).
fn is_numeric_type(sql_type: &str) -> bool {
    let t = sql_type.to_lowercase();
    t.contains("int")
        || t.contains("float")
        || t.contains("double")
        || t.contains("numeric")
        || t.contains("decimal")
        || t.contains("real")
        || t.contains("number")
        || t == "bigint"
        || t == "ubigint"
}

/// Sanitize a column name to a snake_case dimension/measure name.
fn sanitize_name(col_name: &str) -> String {
    // Replace special chars with underscores, then convert to snake_case
    let cleaned: String = col_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Remove leading/trailing underscores and collapse multiple underscores
    let collapsed: String = cleaned
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_");

    collapsed.to_case(Case::Snake)
}

/// Produce a YAML-safe expr value for a column name.
/// Columns needing SQL quoting get: `'"My Column"'` (single-quoted YAML containing double-quoted SQL).
/// Plain columns get: `"col_name"` (double-quoted YAML, no SQL quoting).
fn yaml_quote_expr(col_name: &str) -> String {
    if needs_quoting(col_name) {
        // Single-quote the YAML value so the inner SQL double-quotes are literal
        let escaped = col_name.replace('\'', "''"); // escape single quotes for YAML
        format!("'\"{}\"'", escaped)
    } else {
        format!("\"{}\"", col_name)
    }
}

/// Check if a column name needs quoting in SQL (contains spaces, special chars, etc.).
fn needs_quoting(col_name: &str) -> bool {
    col_name.contains(' ')
        || col_name.contains('(')
        || col_name.contains(')')
        || col_name.contains('%')
        || col_name.contains('-')
        || col_name.contains('/')
        || col_name
            .chars()
            .next()
            .map(|c| c.is_numeric())
            .unwrap_or(false)
}

/// Generate a `.view.yml` YAML string for a table.
pub fn generate_view_yaml(table: &TableInfo, datasource: &str, dialect: &str) -> String {
    let view_name = sanitize_name(&table.name);
    let schema_prefix = table
        .schema
        .as_ref()
        .map(|s| format!("{}.", s))
        .unwrap_or_default();
    let table_ref = format!("{}{}", schema_prefix, table.name);

    let mut yaml = String::new();

    // Header
    yaml.push_str(&format!("name: {}\n", view_name));
    yaml.push_str(&format!(
        "description: \"{}\"\n",
        table.name.replace('"', "\\\"")
    ));
    yaml.push_str(&format!("dialect: {}\n", dialect));
    yaml.push_str(&format!("datasource: {}\n", datasource));
    yaml.push_str(&format!("table: {}\n", table_ref));

    // Dimensions
    yaml.push_str("\ndimensions:\n");
    for col in &table.columns {
        // Skip unnamed/generic columns
        if col.name.starts_with("column")
            && col.name.len() <= 10
            && col.name[6..].chars().all(|c| c.is_numeric())
        {
            continue;
        }

        let dim_name = sanitize_name(&col.name);
        let dim_type = sql_type_to_dim_type(&col.data_type);

        yaml.push_str(&format!("  - name: {}\n", dim_name));
        yaml.push_str(&format!("    type: {}\n", dim_type));
        yaml.push_str(&format!("    expr: {}\n", yaml_quote_expr(&col.name)));
    }

    // Measures
    yaml.push_str("\nmeasures:\n");

    // Always add a count measure
    yaml.push_str("  - name: count\n");
    yaml.push_str("    type: count\n");

    // Add sum measures for numeric columns
    for col in &table.columns {
        if col.name.starts_with("column")
            && col.name.len() <= 10
            && col.name[6..].chars().all(|c| c.is_numeric())
        {
            continue;
        }

        if is_numeric_type(&col.data_type) {
            let dim_name = sanitize_name(&col.name);

            yaml.push_str(&format!("  - name: total_{}\n", dim_name));
            yaml.push_str("    type: sum\n");
            yaml.push_str(&format!("    expr: {}\n", yaml_quote_expr(&col.name)));

            yaml.push_str(&format!("  - name: avg_{}\n", dim_name));
            yaml.push_str("    type: average\n");
            yaml.push_str(&format!("    expr: {}\n", yaml_quote_expr(&col.name)));
        }
    }

    yaml
}

/// The dialect to use in views for a given database type.
pub fn dialect_for_db_type(db_type: &str) -> &str {
    match db_type {
        "motherduck" => "duckdb",
        other => other,
    }
}

/// Bootstrap views for all given tables, writing `.view.yml` files to the views directory.
/// Returns the list of files created.
pub fn bootstrap_views(
    tables: &[&TableInfo],
    datasource: &str,
    dialect: &str,
    views_dir: &Path,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(views_dir)?;

    let mut created = Vec::new();
    for table in tables {
        let view_name = sanitize_name(&table.name);
        let filename = format!("{}.view.yml", view_name);
        let filepath = views_dir.join(&filename);

        let yaml = generate_view_yaml(table, datasource, dialect);
        std::fs::write(&filepath, &yaml)?;
        created.push(filename);
    }

    Ok(created)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_name() {
        assert_eq!(sanitize_name("Weight (lbs)"), "weight_lbs");
        assert_eq!(
            sanitize_name("Body Fat % (Caliper - Gut)"),
            "body_fat_caliper_gut"
        );
        assert_eq!(sanitize_name("Date"), "date");
        assert_eq!(
            sanitize_name("Treadmill Speed (mph)"),
            "treadmill_speed_mph"
        );
        assert_eq!(sanitize_name("created_at"), "created_at");
    }

    #[test]
    fn test_sql_type_to_dim_type() {
        assert_eq!(sql_type_to_dim_type("VARCHAR"), "string");
        assert_eq!(sql_type_to_dim_type("INTEGER"), "number");
        assert_eq!(sql_type_to_dim_type("BIGINT"), "number");
        assert_eq!(sql_type_to_dim_type("DOUBLE"), "number");
        assert_eq!(sql_type_to_dim_type("DATE"), "date");
        assert_eq!(sql_type_to_dim_type("TIMESTAMP"), "datetime");
        assert_eq!(sql_type_to_dim_type("BOOLEAN"), "boolean");
        assert_eq!(sql_type_to_dim_type("TIMESTAMP WITH TIME ZONE"), "datetime");
    }

    #[test]
    fn test_needs_quoting() {
        assert!(needs_quoting("Weight (lbs)"));
        assert!(needs_quoting("Body Fat %"));
        assert!(!needs_quoting("date"));
        assert!(!needs_quoting("created_at"));
    }

    #[test]
    fn test_generate_view_yaml() {
        let table = TableInfo {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    ordinal: Some(1),
                },
                ColumnInfo {
                    name: "status".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                    ordinal: Some(2),
                },
                ColumnInfo {
                    name: "amount".to_string(),
                    data_type: "DOUBLE".to_string(),
                    nullable: true,
                    ordinal: Some(3),
                },
            ],
        };

        let yaml = generate_view_yaml(&table, "warehouse", "postgres");
        assert!(yaml.contains("name: orders"));
        assert!(yaml.contains("table: public.orders"));
        assert!(yaml.contains("dialect: postgres"));
        assert!(yaml.contains("  - name: id"));
        assert!(yaml.contains("  - name: status"));
        assert!(yaml.contains("  - name: total_amount"));
        assert!(yaml.contains("  - name: avg_amount"));
        assert!(yaml.contains("  - name: count"));
    }

    #[test]
    fn test_generate_view_yaml_quoted_columns() {
        let table = TableInfo {
            schema: None,
            name: "metrics".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "Weight (lbs)".to_string(),
                    data_type: "DOUBLE".to_string(),
                    nullable: true,
                    ordinal: Some(1),
                },
                ColumnInfo {
                    name: "Body Fat %".to_string(),
                    data_type: "DOUBLE".to_string(),
                    nullable: true,
                    ordinal: Some(2),
                },
                ColumnInfo {
                    name: "normal_col".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                    ordinal: Some(3),
                },
            ],
        };

        let yaml = generate_view_yaml(&table, "warehouse", "duckdb");
        // Quoted columns: single-quoted YAML wrapping double-quoted SQL
        assert!(yaml.contains("expr: '\"Weight (lbs)\"'"), "got: {}", yaml);
        assert!(yaml.contains("expr: '\"Body Fat %\"'"), "got: {}", yaml);
        // Normal column: double-quoted YAML, no SQL quoting
        assert!(yaml.contains("expr: \"normal_col\""), "got: {}", yaml);
        // Measures for quoted numeric columns
        assert!(yaml.contains("name: total_weight_lbs"));
        assert!(yaml.contains("name: total_body_fat"));
    }
}
