//! Interactive prompts for `airlayer init` — collect database connection details from the user.

use console::style;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use std::collections::BTreeMap;

/// All database types supported by airlayer.
pub const DB_TYPES: &[&str] = &[
    "postgres",
    "snowflake",
    "bigquery",
    "duckdb",
    "motherduck",
    "mysql",
    "clickhouse",
    "databricks",
    "redshift",
    "sqlite",
];

/// Interactively select a database type.
pub fn select_database_type() -> Result<String, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let selection = Select::with_theme(&theme)
        .with_prompt("Database type")
        .items(DB_TYPES)
        .default(0)
        .interact()?;
    Ok(DB_TYPES[selection].to_string())
}

/// Prompt for connection fields based on database type.
pub fn prompt_connection_fields(
    db_type: &str,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    println!();
    println!(
        "  {}",
        style(format!("Configure {} connection", db_type)).dim()
    );
    println!();
    match db_type {
        "postgres" | "redshift" => prompt_postgres_fields(db_type),
        "snowflake" => prompt_snowflake_fields(),
        "bigquery" => prompt_bigquery_fields(),
        "duckdb" => prompt_duckdb_fields(),
        "motherduck" => prompt_motherduck_fields(),
        "mysql" => prompt_mysql_fields(),
        "clickhouse" => prompt_clickhouse_fields(),
        "databricks" => prompt_databricks_fields(),
        "sqlite" => prompt_sqlite_fields(),
        _ => Err(format!("Unknown database type: {}", db_type).into()),
    }
}

/// Generate a complete config.yml string from prompted fields.
pub fn generate_config_yml(db_type: &str, fields: &BTreeMap<String, String>) -> String {
    let mut lines = Vec::new();
    lines.push("databases:".to_string());

    let name = fields
        .get("name")
        .map(|s| s.as_str())
        .unwrap_or("warehouse");
    lines.push(format!("  - name: {}", name));
    lines.push(format!("    type: {}", db_type));

    let ordered_keys = field_order(db_type);
    for key in ordered_keys {
        if key == "name" || key == "type" {
            continue;
        }
        if let Some(val) = fields.get(key) {
            if !val.is_empty() {
                lines.push(format!("    {}: {}", key, val));
            }
        }
    }

    lines.join("\n") + "\n"
}

/// Generate a config template for a specific database type (non-interactive).
pub fn config_template_for_type(db_type: &str) -> Option<String> {
    let template = match db_type {
        "postgres" => "\
databases:
  - name: warehouse
    type: postgres
    host: localhost
    port: \"5432\"
    database: mydb
    user: myuser
    password_var: PG_PASSWORD    # reads from environment variable
",
        "redshift" => "\
databases:
  - name: warehouse
    type: redshift
    host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
    port: \"5439\"
    database: mydb
    user: myuser
    password_var: REDSHIFT_PASSWORD
",
        "snowflake" => "\
databases:
  - name: warehouse
    type: snowflake
    account: myaccount
    user: myuser
    password_var: SNOWFLAKE_PASSWORD
    warehouse: COMPUTE_WH
    database: MYDB
    schema: PUBLIC
",
        "bigquery" => "\
databases:
  - name: warehouse
    type: bigquery
    project: my-gcp-project
    dataset: analytics
    access_token_var: BIGQUERY_ACCESS_TOKEN
",
        "duckdb" => "\
databases:
  - name: warehouse
    type: duckdb
    path: ./data/analytics.duckdb
",
        "motherduck" => "\
databases:
  - name: warehouse
    type: motherduck
    token_var: MOTHERDUCK_TOKEN
    database: my_db
",
        "mysql" => "\
databases:
  - name: warehouse
    type: mysql
    host: localhost
    port: \"3306\"
    database: mydb
    user: root
    password_var: MYSQL_PASSWORD
",
        "clickhouse" => "\
databases:
  - name: warehouse
    type: clickhouse
    host: http://localhost
    port: \"8123\"
    database: default
",
        "databricks" => "\
databases:
  - name: warehouse
    type: databricks
    host: dbc-abc123.cloud.databricks.com
    token_var: DATABRICKS_TOKEN
    warehouse_id: my-warehouse-id
",
        "sqlite" => "\
databases:
  - name: warehouse
    type: sqlite
    path: ./data/analytics.db
",
        _ => return None,
    };
    Some(template.to_string())
}

// --- Per-type prompt functions ---

fn prompt_postgres_fields(
    db_type: &str,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let host: String = Input::with_theme(&theme)
        .with_prompt("Host")
        .default("localhost".to_string())
        .interact_text()?;
    fields.insert("host".to_string(), host);

    let default_port = if db_type == "redshift" {
        "5439"
    } else {
        "5432"
    };
    let port: String = Input::with_theme(&theme)
        .with_prompt("Port")
        .default(default_port.to_string())
        .interact_text()?;
    fields.insert("port".to_string(), port);

    let database: String = Input::with_theme(&theme)
        .with_prompt("Database")
        .interact_text()?;
    fields.insert("database".to_string(), database);

    let user: String = Input::with_theme(&theme)
        .with_prompt("User")
        .default("postgres".to_string())
        .interact_text()?;
    fields.insert("user".to_string(), user);

    let password_var: String = Input::with_theme(&theme)
        .with_prompt("Password env var")
        .default("PG_PASSWORD".to_string())
        .interact_text()?;
    fields.insert("password_var".to_string(), password_var);

    Ok(fields)
}

fn prompt_snowflake_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let account: String = Input::with_theme(&theme)
        .with_prompt("Account identifier")
        .interact_text()?;
    fields.insert("account".to_string(), account);

    let user: String = Input::with_theme(&theme)
        .with_prompt("Username")
        .interact_text()?;
    fields.insert("user".to_string(), user);

    let password_var: String = Input::with_theme(&theme)
        .with_prompt("Password env var")
        .default("SNOWFLAKE_PASSWORD".to_string())
        .interact_text()?;
    fields.insert("password_var".to_string(), password_var);

    let warehouse: String = Input::with_theme(&theme)
        .with_prompt("Warehouse")
        .default("COMPUTE_WH".to_string())
        .interact_text()?;
    fields.insert("warehouse".to_string(), warehouse);

    let database: String = Input::with_theme(&theme)
        .with_prompt("Database (optional)")
        .default(String::new())
        .interact_text()?;
    if !database.is_empty() {
        fields.insert("database".to_string(), database);
    }

    let schema: String = Input::with_theme(&theme)
        .with_prompt("Schema (optional)")
        .default(String::new())
        .interact_text()?;
    if !schema.is_empty() {
        fields.insert("schema".to_string(), schema);
    }

    Ok(fields)
}

fn prompt_bigquery_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let project: String = Input::with_theme(&theme)
        .with_prompt("GCP project ID")
        .interact_text()?;
    fields.insert("project".to_string(), project);

    let dataset: String = Input::with_theme(&theme)
        .with_prompt("Default dataset (optional)")
        .default(String::new())
        .interact_text()?;
    if !dataset.is_empty() {
        fields.insert("dataset".to_string(), dataset);
    }

    let access_token_var: String = Input::with_theme(&theme)
        .with_prompt("Access token env var")
        .default("BIGQUERY_ACCESS_TOKEN".to_string())
        .interact_text()?;
    fields.insert("access_token_var".to_string(), access_token_var);

    Ok(fields)
}

fn prompt_duckdb_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let path: String = Input::with_theme(&theme)
        .with_prompt("Path to .duckdb file (empty for in-memory)")
        .default(String::new())
        .interact_text()?;
    if !path.is_empty() {
        fields.insert("path".to_string(), path);
    }

    Ok(fields)
}

fn prompt_motherduck_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let token_var: String = Input::with_theme(&theme)
        .with_prompt("Token env var")
        .default("MOTHERDUCK_TOKEN".to_string())
        .interact_text()?;
    fields.insert("token_var".to_string(), token_var);

    let database: String = Input::with_theme(&theme)
        .with_prompt("Database name (optional)")
        .default(String::new())
        .interact_text()?;
    if !database.is_empty() {
        fields.insert("database".to_string(), database);
    }

    Ok(fields)
}

fn prompt_mysql_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let host: String = Input::with_theme(&theme)
        .with_prompt("Host")
        .default("localhost".to_string())
        .interact_text()?;
    fields.insert("host".to_string(), host);

    let port: String = Input::with_theme(&theme)
        .with_prompt("Port")
        .default("3306".to_string())
        .interact_text()?;
    fields.insert("port".to_string(), port);

    let database: String = Input::with_theme(&theme)
        .with_prompt("Database")
        .interact_text()?;
    fields.insert("database".to_string(), database);

    let user: String = Input::with_theme(&theme)
        .with_prompt("User")
        .default("root".to_string())
        .interact_text()?;
    fields.insert("user".to_string(), user);

    let password_var: String = Input::with_theme(&theme)
        .with_prompt("Password env var")
        .default("MYSQL_PASSWORD".to_string())
        .interact_text()?;
    fields.insert("password_var".to_string(), password_var);

    Ok(fields)
}

fn prompt_clickhouse_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let host: String = Input::with_theme(&theme)
        .with_prompt("Host")
        .default("http://localhost".to_string())
        .interact_text()?;
    fields.insert("host".to_string(), host);

    let port: String = Input::with_theme(&theme)
        .with_prompt("Port")
        .default("8123".to_string())
        .interact_text()?;
    fields.insert("port".to_string(), port);

    let database: String = Input::with_theme(&theme)
        .with_prompt("Database (optional)")
        .default(String::new())
        .interact_text()?;
    if !database.is_empty() {
        fields.insert("database".to_string(), database);
    }

    Ok(fields)
}

fn prompt_databricks_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let host: String = Input::with_theme(&theme)
        .with_prompt("Workspace host")
        .interact_text()?;
    fields.insert("host".to_string(), host);

    let token_var: String = Input::with_theme(&theme)
        .with_prompt("Token env var")
        .default("DATABRICKS_TOKEN".to_string())
        .interact_text()?;
    fields.insert("token_var".to_string(), token_var);

    let warehouse_id: String = Input::with_theme(&theme)
        .with_prompt("SQL warehouse ID")
        .interact_text()?;
    fields.insert("warehouse_id".to_string(), warehouse_id);

    let catalog: String = Input::with_theme(&theme)
        .with_prompt("Catalog (optional)")
        .default(String::new())
        .interact_text()?;
    if !catalog.is_empty() {
        fields.insert("catalog".to_string(), catalog);
    }

    let schema: String = Input::with_theme(&theme)
        .with_prompt("Schema (optional)")
        .default(String::new())
        .interact_text()?;
    if !schema.is_empty() {
        fields.insert("schema".to_string(), schema);
    }

    Ok(fields)
}

fn prompt_sqlite_fields() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), prompt_datasource_name()?);

    let path: String = Input::with_theme(&theme)
        .with_prompt("Path to SQLite file")
        .interact_text()?;
    fields.insert("path".to_string(), path);

    Ok(fields)
}

// --- Helpers ---

fn prompt_datasource_name() -> Result<String, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let name: String = Input::with_theme(&theme)
        .with_prompt("Datasource name")
        .default("warehouse".to_string())
        .interact_text()?;
    Ok(name)
}

/// Field ordering per database type (for YAML output).
fn field_order(db_type: &str) -> Vec<&'static str> {
    match db_type {
        "postgres" | "redshift" => {
            vec!["name", "type", "host", "port", "database", "user", "password_var"]
        }
        "snowflake" => vec![
            "name", "type", "account", "user", "password_var", "warehouse", "database", "schema",
        ],
        "bigquery" => vec!["name", "type", "project", "dataset", "access_token_var"],
        "duckdb" => vec!["name", "type", "path"],
        "motherduck" => vec!["name", "type", "token_var", "database"],
        "mysql" => {
            vec!["name", "type", "host", "port", "database", "user", "password_var"]
        }
        "clickhouse" => vec!["name", "type", "host", "port", "database"],
        "databricks" => vec![
            "name", "type", "host", "token_var", "warehouse_id", "catalog", "schema",
        ],
        "sqlite" => vec!["name", "type", "path"],
        _ => vec!["name", "type"],
    }
}

/// Ask the user whether to bootstrap views from the schema.
pub fn confirm_bootstrap() -> Result<bool, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    Ok(Confirm::with_theme(&theme)
        .with_prompt("Discover tables and generate views?")
        .default(true)
        .interact()?)
}
