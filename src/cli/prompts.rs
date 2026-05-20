//! Interactive prompts for `airlayer init` — collect database connection details from the user.

use console::{style, Key, Term};
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

/// Database types that support listing databases after connecting with just credentials.
pub fn supports_database_listing(db_type: &str) -> bool {
    matches!(
        db_type,
        "postgres"
            | "redshift"
            | "mysql"
            | "snowflake"
            | "bigquery"
            | "clickhouse"
            | "databricks"
            | "motherduck"
    )
}

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

/// Prompt for ONLY the minimum credentials needed to connect.
/// Does NOT prompt for database, schema, or other scoping fields — those come from discovery.
pub fn prompt_credentials(
    db_type: &str,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    println!();
    println!(
        "  {}",
        style(format!("Configure {} connection", db_type)).dim()
    );
    println!();
    match db_type {
        "postgres" | "redshift" => prompt_postgres_credentials(db_type),
        "snowflake" => prompt_snowflake_credentials(),
        "bigquery" => prompt_bigquery_credentials(),
        "duckdb" => prompt_duckdb_credentials(),
        "motherduck" => prompt_motherduck_credentials(),
        "mysql" => prompt_mysql_credentials(),
        "clickhouse" => prompt_clickhouse_credentials(),
        "databricks" => prompt_databricks_credentials(),
        "sqlite" => prompt_sqlite_credentials(),
        _ => Err(format!("Unknown database type: {}", db_type).into()),
    }
}

/// Re-prompt credentials after a connection failure, using existing values as defaults.
/// Only re-prompts fields — the user can press enter to keep the current value.
pub fn reprompt_credentials(
    db_type: &str,
    existing: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    println!(
        "  {}",
        style("Re-enter credentials (press enter to keep current value)").dim()
    );
    println!();

    // Use field_order to iterate in logical prompt order, not alphabetical
    for key in field_order(db_type) {
        if key == "name" || key == "type" {
            continue;
        }
        if let Some(val) = existing.get(key) {
            let new_val: String = Input::with_theme(&theme)
                .with_prompt(key)
                .default(val.clone())
                .interact_text()?;
            if !new_val.is_empty() {
                fields.insert(key.to_string(), new_val);
            }
        }
    }

    Ok(fields)
}

/// AI CLI tool that can be used for view enrichment.
#[derive(Debug, Clone, Copy)]
pub enum AiTool {
    Claude,
    Codex,
}

impl AiTool {
    pub fn display_name(self) -> &'static str {
        match self {
            AiTool::Claude => "Claude Code",
            AiTool::Codex => "Codex",
        }
    }
}

/// Detect which AI CLI tool is available on PATH. Prefers Claude over Codex.
pub fn detect_ai_tool() -> Option<AiTool> {
    if command_exists("claude") {
        Some(AiTool::Claude)
    } else if command_exists("codex") {
        Some(AiTool::Codex)
    } else {
        None
    }
}

fn command_exists(cmd: &str) -> bool {
    // Search PATH directly instead of relying on `which` (not available on all platforms)
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(cmd).is_file()))
        .unwrap_or(false)
}

/// Ask the user if they want to enrich generated views with AI.
/// Shows a warning about --dangerously-skip-permissions and asks for confirmation.
pub fn prompt_enrichment(tool: AiTool) -> Result<bool, Box<dyn std::error::Error>> {
    use console::style;

    println!(
        "  {}  {} will review and improve the generated views.",
        style("~").cyan(),
        tool.display_name(),
    );
    println!(
        "     {}",
        style(format!(
            "This runs {} with --dangerously-skip-permissions.",
            tool.display_name()
        ))
        .dim(),
    );
    println!();

    let theme = ColorfulTheme::default();
    let term = console::Term::stderr();
    let result = Confirm::with_theme(&theme)
        .with_prompt("Continue?")
        .default(true)
        .interact()?;
    // Clear the confirm prompt line so it doesn't duplicate with the session output
    term.clear_last_lines(1)?;
    Ok(result)
}

/// Let the user select a database from a discovered list.
/// Returns the selected database name, or None if the list is empty.
pub fn prompt_database_selection(
    databases: &[String],
    label: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if databases.is_empty() {
        return Ok(None);
    }
    if databases.len() == 1 {
        println!(
            "  {} Using {} {}",
            style("~").green(),
            label,
            style(&databases[0]).cyan()
        );
        println!();
        return Ok(Some(databases[0].clone()));
    }

    let theme = ColorfulTheme::default();
    let selection = Select::with_theme(&theme)
        .with_prompt(format!("Select {}", label))
        .items(databases)
        .default(0)
        .interact()?;
    Ok(Some(databases[selection].clone()))
}

/// Result of table selection prompt.
pub enum TableSelection {
    /// User selected these table indices.
    Selected(Vec<usize>),
    /// User chose to go back (e.g., re-select database).
    Back,
}

/// Custom multi-select for table selection with `b` to go back.
/// None selected by default. Loops until the user makes a valid selection and confirms.
pub fn prompt_table_selection(
    table_labels: &[String],
) -> Result<TableSelection, Box<dyn std::error::Error>> {
    if table_labels.is_empty() {
        return Ok(TableSelection::Selected(vec![]));
    }

    let term = Term::stderr();
    let mut cursor: usize = 0;
    let mut checked: Vec<bool> = vec![false; table_labels.len()];

    // Viewport: show at most max_visible items, scroll when needed
    let term_height = term.size().0 as usize;
    // Reserve 4 lines for prompt, help, and some breathing room
    let max_visible = (term_height.saturating_sub(4))
        .max(5)
        .min(table_labels.len());
    let mut scroll_offset: usize = 0;

    // Lines rendered: 1 (prompt) + visible items + 1 (help)
    let rendered_lines = || max_visible.min(table_labels.len()) + 2;

    // Initial render
    render_table_select(
        &term,
        table_labels,
        &checked,
        cursor,
        scroll_offset,
        max_visible,
        None,
    )?;

    loop {
        match term.read_key()? {
            Key::ArrowUp | Key::Char('k') => {
                if cursor > 0 {
                    cursor -= 1;
                    if cursor < scroll_offset {
                        scroll_offset = cursor;
                    }
                }
                term.clear_last_lines(rendered_lines())?;
                render_table_select(
                    &term,
                    table_labels,
                    &checked,
                    cursor,
                    scroll_offset,
                    max_visible,
                    None,
                )?;
            }
            Key::ArrowDown | Key::Char('j') => {
                if cursor + 1 < table_labels.len() {
                    cursor += 1;
                    if cursor >= scroll_offset + max_visible {
                        scroll_offset = cursor - max_visible + 1;
                    }
                }
                term.clear_last_lines(rendered_lines())?;
                render_table_select(
                    &term,
                    table_labels,
                    &checked,
                    cursor,
                    scroll_offset,
                    max_visible,
                    None,
                )?;
            }
            Key::Char(' ') => {
                checked[cursor] = !checked[cursor];
                term.clear_last_lines(rendered_lines())?;
                render_table_select(
                    &term,
                    table_labels,
                    &checked,
                    cursor,
                    scroll_offset,
                    max_visible,
                    None,
                )?;
            }
            Key::Char('a') | Key::Char('A') => {
                let all_checked = checked.iter().all(|&c| c);
                checked.fill(!all_checked);
                term.clear_last_lines(rendered_lines())?;
                render_table_select(
                    &term,
                    table_labels,
                    &checked,
                    cursor,
                    scroll_offset,
                    max_visible,
                    None,
                )?;
            }
            Key::Enter => {
                let selected: Vec<usize> = checked
                    .iter()
                    .enumerate()
                    .filter(|(_, &c)| c)
                    .map(|(i, _)| i)
                    .collect();

                if selected.is_empty() {
                    term.clear_last_lines(rendered_lines())?;
                    render_table_select(
                        &term,
                        table_labels,
                        &checked,
                        cursor,
                        scroll_offset,
                        max_visible,
                        Some("Select at least one table"),
                    )?;
                    continue;
                }

                // Show confirmation — append 2 lines below the picker
                let count = selected.len();
                eprintln!();
                eprintln!(
                    "  {} {} tables selected. Press {} to confirm, {} to go back",
                    style("~").green(),
                    style(count).bold(),
                    style("enter").cyan().bold(),
                    style("any key").dim(),
                );

                match term.read_key()? {
                    Key::Enter => {
                        return Ok(TableSelection::Selected(selected));
                    }
                    _ => {
                        // Clear confirmation (2 lines) + picker, re-render
                        term.clear_last_lines(rendered_lines() + 2)?;
                        render_table_select(
                            &term,
                            table_labels,
                            &checked,
                            cursor,
                            scroll_offset,
                            max_visible,
                            None,
                        )?;
                    }
                }
            }
            Key::Char('b') | Key::Char('B') => {
                term.clear_last_lines(rendered_lines())?;
                return Ok(TableSelection::Back);
            }
            Key::Escape => {
                term.clear_last_lines(rendered_lines())?;
                return Ok(TableSelection::Back);
            }
            _ => {}
        }
    }
}

/// Render the custom multi-select table picker with viewport scrolling.
fn render_table_select(
    term: &Term,
    items: &[String],
    checked: &[bool],
    cursor: usize,
    scroll_offset: usize,
    max_visible: usize,
    error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let selected_count = checked.iter().filter(|&&c| c).count();
    let visible_end = (scroll_offset + max_visible).min(items.len());
    // Terminal width for truncation — prevent line wrapping that breaks clear_last_lines
    let term_width = term.size().1 as usize;

    // Prompt line with count
    if selected_count > 0 {
        eprintln!(
            "  {} ({} selected)",
            style("Select tables to model").bold(),
            style(selected_count).cyan()
        );
    } else {
        eprintln!("  {}", style("Select tables to model").bold());
    }

    // Items (only visible viewport)
    // Prefix is 6 visible chars: "  › ◉ " or "    ◉ "
    let max_item_width = term_width.saturating_sub(6);
    for i in scroll_offset..visible_end {
        let checkbox = if checked[i] {
            style("◉").cyan().to_string()
        } else {
            style("○").dim().to_string()
        };

        let label = if items[i].len() > max_item_width {
            format!("{}…", &items[i][..max_item_width.saturating_sub(1)])
        } else {
            items[i].clone()
        };

        if i == cursor {
            eprintln!("  {} {} {}", style("›").cyan().bold(), checkbox, label);
        } else {
            eprintln!("    {} {}", checkbox, style(&label).dim());
        }
    }

    // Help line (with scroll indicators if needed)
    let mut help_parts = Vec::new();
    if scroll_offset > 0 {
        help_parts.push(format!("{}", style("↑ more").dim()));
    }
    if visible_end < items.len() {
        help_parts.push(format!("{}", style("↓ more").dim()));
    }
    let scroll_hint = if help_parts.is_empty() {
        String::new()
    } else {
        format!("{}  ", help_parts.join("  "))
    };

    let help = format!(
        "{}{} {} {} {} {} {} {} {}",
        scroll_hint,
        style("space").cyan().bold(),
        style("toggle").dim(),
        style("enter").cyan().bold(),
        style("continue").dim(),
        style("b").cyan().bold(),
        style("back").dim(),
        style("a").cyan().bold(),
        style("all").dim(),
    );
    if let Some(err) = error {
        eprintln!("  {}  {}", style(err).red(), help);
    } else {
        eprintln!("  {}", help);
    }

    let _ = term.flush();
    Ok(())
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
        "postgres" => {
            "\
databases:
  - name: warehouse
    type: postgres
    host: localhost
    port: \"5432\"
    database: mydb
    user: myuser
    password_var: PG_PASSWORD    # reads from environment variable
"
        }
        "redshift" => {
            "\
databases:
  - name: warehouse
    type: redshift
    host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
    port: \"5439\"
    database: mydb
    user: myuser
    password_var: REDSHIFT_PASSWORD
"
        }
        "snowflake" => {
            "\
databases:
  - name: warehouse
    type: snowflake
    account: myaccount
    user: myuser
    password_var: SNOWFLAKE_PASSWORD
    warehouse: COMPUTE_WH
    database: MYDB
    schema: PUBLIC
"
        }
        "bigquery" => {
            "\
databases:
  - name: warehouse
    type: bigquery
    project: my-gcp-project
    dataset: analytics
    access_token_var: BIGQUERY_ACCESS_TOKEN
"
        }
        "duckdb" => {
            "\
databases:
  - name: warehouse
    type: duckdb
    path: ./data/analytics.duckdb
"
        }
        "motherduck" => {
            "\
databases:
  - name: warehouse
    type: motherduck
    token_var: MOTHERDUCK_TOKEN
    database: my_db
"
        }
        "mysql" => {
            "\
databases:
  - name: warehouse
    type: mysql
    host: localhost
    port: \"3306\"
    database: mydb
    user: root
    password_var: MYSQL_PASSWORD
"
        }
        "clickhouse" => {
            "\
databases:
  - name: warehouse
    type: clickhouse
    host: http://localhost
    port: \"8123\"
    database: default
"
        }
        "databricks" => {
            "\
databases:
  - name: warehouse
    type: databricks
    host: dbc-abc123.cloud.databricks.com
    token_var: DATABRICKS_TOKEN
    warehouse_id: my-warehouse-id
"
        }
        "sqlite" => {
            "\
databases:
  - name: warehouse
    type: sqlite
    path: ./data/analytics.db
"
        }
        _ => return None,
    };
    Some(template.to_string())
}

// --- Credential-only prompt functions ---
// These collect ONLY what's needed to establish a connection.
// Database/schema selection happens later via discovery.

fn prompt_postgres_credentials(
    db_type: &str,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

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

fn prompt_snowflake_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

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

    Ok(fields)
}

fn prompt_bigquery_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    let project: String = Input::with_theme(&theme)
        .with_prompt("GCP project ID")
        .interact_text()?;
    fields.insert("project".to_string(), project);

    let access_token_var: String = Input::with_theme(&theme)
        .with_prompt("Access token env var")
        .default("BIGQUERY_ACCESS_TOKEN".to_string())
        .interact_text()?;
    fields.insert("access_token_var".to_string(), access_token_var);

    Ok(fields)
}

fn prompt_duckdb_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    let path: String = Input::with_theme(&theme)
        .with_prompt("Path to .duckdb file (empty for in-memory)")
        .default(String::new())
        .interact_text()?;
    if !path.is_empty() {
        fields.insert("path".to_string(), path);
    }

    Ok(fields)
}

fn prompt_motherduck_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    let token_var: String = Input::with_theme(&theme)
        .with_prompt("Token env var")
        .default("MOTHERDUCK_TOKEN".to_string())
        .interact_text()?;
    fields.insert("token_var".to_string(), token_var);

    Ok(fields)
}

fn prompt_mysql_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

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

fn prompt_clickhouse_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

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

    Ok(fields)
}

fn prompt_databricks_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

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

    Ok(fields)
}

fn prompt_sqlite_credentials() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    let path: String = Input::with_theme(&theme)
        .with_prompt("Path to SQLite file")
        .interact_text()?;
    fields.insert("path".to_string(), path);

    Ok(fields)
}

/// Prompt for credentials with pre-filled defaults from foreign repo extraction.
/// Unlike `prompt_credentials`, this shows all fields with extracted values as defaults.
pub fn prompt_foreign_credentials(
    db_type: &str,
    extracted: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let theme = ColorfulTheme::default();
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), "warehouse".to_string());

    println!();
    println!(
        "  {}",
        style(format!("Configure {} connection", db_type)).dim()
    );
    println!();

    // Get all credential keys for this db type (skip name/type)
    for key in field_order(db_type) {
        if key == "name" || key == "type" {
            continue;
        }
        let default = extracted.get(key).cloned().unwrap_or_default();
        let label = field_label(key);

        // Determine the effective default
        let effective_default = if !default.is_empty() {
            Some(default)
        } else {
            field_default(db_type, key).map(|s| s.to_string())
        };

        let val: String = if let Some(def) = effective_default {
            Input::with_theme(&theme)
                .with_prompt(label)
                .default(def)
                .interact_text()?
        } else {
            Input::with_theme(&theme)
                .with_prompt(label)
                .interact_text()?
        };
        if !val.is_empty() {
            fields.insert(key.to_string(), val);
        }
    }

    Ok(fields)
}

/// Human-readable label for a config field.
fn field_label(key: &str) -> &str {
    match key {
        "host" => "Host",
        "port" => "Port",
        "database" => "Database",
        "user" => "User",
        "password_var" => "Password env var",
        "account" => "Account identifier",
        "warehouse" => "Warehouse",
        "schema" => "Schema",
        "project" => "GCP project ID",
        "dataset" => "Dataset",
        "access_token_var" => "Access token env var",
        "token_var" => "Token env var",
        "warehouse_id" => "SQL warehouse ID",
        "path" => "Path",
        "catalog" => "Catalog",
        _ => key,
    }
}

/// Default value for a field when no extracted value is available.
fn field_default(db_type: &str, key: &str) -> Option<&'static str> {
    match (db_type, key) {
        ("postgres", "host") | ("redshift", "host") | ("mysql", "host") => Some("localhost"),
        ("postgres", "port") => Some("5432"),
        ("redshift", "port") => Some("5439"),
        ("mysql", "port") => Some("3306"),
        ("clickhouse", "host") => Some("http://localhost"),
        ("clickhouse", "port") => Some("8123"),
        ("postgres", "user") => Some("postgres"),
        ("mysql", "user") => Some("root"),
        ("postgres", "password_var") => Some("PG_PASSWORD"),
        ("redshift", "password_var") => Some("REDSHIFT_PASSWORD"),
        ("snowflake", "password_var") => Some("SNOWFLAKE_PASSWORD"),
        ("mysql", "password_var") => Some("MYSQL_PASSWORD"),
        ("bigquery", "access_token_var") => Some("BIGQUERY_ACCESS_TOKEN"),
        ("motherduck", "token_var") => Some("MOTHERDUCK_TOKEN"),
        ("databricks", "token_var") => Some("DATABRICKS_TOKEN"),
        ("snowflake", "warehouse") => Some("COMPUTE_WH"),
        _ => None,
    }
}

/// Extract database connection info from a dbt project (dbt_project.yml → ~/.dbt/profiles.yml).
pub fn extract_dbt_connection(dir: &std::path::Path) -> BTreeMap<String, String> {
    let mut info = BTreeMap::new();

    let project_path = dir.join("dbt_project.yml");
    let content = match std::fs::read_to_string(&project_path) {
        Ok(c) => c,
        Err(_) => return info,
    };
    let val: serde_yaml::Value = match serde_yaml::from_str(&content) {
        Ok(v) => v,
        Err(_) => return info,
    };
    let profile_name = match val.get("profile").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return info,
    };

    // Read ~/.dbt/profiles.yml
    let home = match std::env::var("HOME") {
        Ok(h) => std::path::PathBuf::from(h),
        Err(_) => return info,
    };
    let profiles_path = home.join(".dbt/profiles.yml");
    let profiles_content = match std::fs::read_to_string(&profiles_path) {
        Ok(c) => c,
        Err(_) => return info,
    };
    let profiles: serde_yaml::Value = match serde_yaml::from_str(&profiles_content) {
        Ok(v) => v,
        Err(_) => return info,
    };

    let profile = match profiles.get(&profile_name) {
        Some(p) => p,
        None => return info,
    };
    let target_name = profile
        .get("target")
        .and_then(|v| v.as_str())
        .unwrap_or("dev");
    let target = match profile.get("outputs").and_then(|o| o.get(target_name)) {
        Some(t) => t,
        None => return info,
    };

    // Map adapter type
    if let Some(adapter) = target.get("type").and_then(|v| v.as_str()) {
        info.insert("db_type".to_string(), map_dbt_adapter(adapter).to_string());
    }

    // Extract common fields
    let field_mappings: &[(&str, &str)] = &[
        ("host", "host"),
        ("port", "port"),
        ("user", "user"),
        ("dbname", "database"),
        ("database", "database"),
        ("schema", "schema"),
        ("account", "account"),
        ("warehouse", "warehouse"),
        ("project", "project"),
        ("dataset", "dataset"),
    ];
    for (dbt_key, config_key) in field_mappings {
        if let Some(val) = target.get(dbt_key) {
            let val_str = if let Some(s) = val.as_str() {
                s.to_string()
            } else if let Some(n) = val.as_u64() {
                n.to_string()
            } else if let Some(n) = val.as_i64() {
                n.to_string()
            } else {
                continue;
            };
            info.insert(config_key.to_string(), val_str);
        }
    }

    info
}

/// Map a dbt adapter type to an airlayer database type.
fn map_dbt_adapter(adapter: &str) -> &str {
    match adapter {
        "postgres" => "postgres",
        "redshift" => "redshift",
        "snowflake" => "snowflake",
        "bigquery" => "bigquery",
        "databricks" | "spark" => "databricks",
        "duckdb" => "duckdb",
        "mysql" => "mysql",
        "clickhouse" => "clickhouse",
        _ => adapter,
    }
}

/// Extract database connection info from a Cube.js project (.env file).
pub fn extract_cube_connection(dir: &std::path::Path) -> BTreeMap<String, String> {
    let mut info = BTreeMap::new();

    let env_path = dir.join(".env");
    let content = match std::fs::read_to_string(&env_path) {
        Ok(c) => c,
        Err(_) => return info,
    };

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || !line.contains('=') {
            continue;
        }
        let mut parts = line.splitn(2, '=');
        let key = parts.next().unwrap_or("").trim();
        let val = parts
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches('"')
            .trim_matches('\'');
        if val.is_empty() {
            continue;
        }
        match key {
            "CUBEJS_DB_TYPE" => {
                info.insert("db_type".to_string(), map_cube_db_type(val).to_string());
            }
            "CUBEJS_DB_HOST" => {
                info.insert("host".to_string(), val.to_string());
            }
            "CUBEJS_DB_PORT" => {
                info.insert("port".to_string(), val.to_string());
            }
            "CUBEJS_DB_NAME" => {
                info.insert("database".to_string(), val.to_string());
            }
            "CUBEJS_DB_USER" => {
                info.insert("user".to_string(), val.to_string());
            }
            "CUBEJS_DB_SCHEMA" => {
                info.insert("schema".to_string(), val.to_string());
            }
            // Snowflake-specific
            "CUBEJS_DB_SNOWFLAKE_ACCOUNT" | "CUBEJS_DB_BQ_PROJECT_ID" => {
                if key.contains("SNOWFLAKE") {
                    info.insert("account".to_string(), val.to_string());
                } else {
                    info.insert("project".to_string(), val.to_string());
                }
            }
            "CUBEJS_DB_SNOWFLAKE_WAREHOUSE" => {
                info.insert("warehouse".to_string(), val.to_string());
            }
            // Don't extract passwords/secrets — use env vars instead
            _ => {}
        }
    }

    info
}

/// Map a Cube.js DB type to an airlayer database type.
fn map_cube_db_type(cube_type: &str) -> &str {
    match cube_type.to_lowercase().as_str() {
        "postgres" | "questdb" | "crate" => "postgres",
        "mysql" | "mysqlauroraserverless" => "mysql",
        "bigquery" => "bigquery",
        "snowflake" => "snowflake",
        "clickhouse" => "clickhouse",
        "databricks-jdbc" | "databricks" => "databricks",
        "duckdb" => "duckdb",
        "prestodb" | "trino" => "presto",
        _ => cube_type,
    }
}

/// Field ordering per database type (for YAML output).
fn field_order(db_type: &str) -> Vec<&'static str> {
    match db_type {
        "postgres" | "redshift" => {
            vec![
                "name",
                "type",
                "host",
                "port",
                "database",
                "user",
                "password_var",
            ]
        }
        "snowflake" => vec![
            "name",
            "type",
            "account",
            "user",
            "password_var",
            "warehouse",
            "database",
            "schema",
        ],
        "bigquery" => vec!["name", "type", "project", "dataset", "access_token_var"],
        "duckdb" => vec!["name", "type", "path"],
        "motherduck" => vec!["name", "type", "token_var", "database"],
        "mysql" => {
            vec![
                "name",
                "type",
                "host",
                "port",
                "database",
                "user",
                "password_var",
            ]
        }
        "clickhouse" => vec!["name", "type", "host", "port", "database"],
        "databricks" => vec![
            "name",
            "type",
            "host",
            "token_var",
            "warehouse_id",
            "catalog",
            "schema",
        ],
        "sqlite" => vec!["name", "type", "path"],
        _ => vec!["name", "type"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_map_dbt_adapter() {
        assert_eq!(map_dbt_adapter("postgres"), "postgres");
        assert_eq!(map_dbt_adapter("redshift"), "redshift");
        assert_eq!(map_dbt_adapter("snowflake"), "snowflake");
        assert_eq!(map_dbt_adapter("bigquery"), "bigquery");
        assert_eq!(map_dbt_adapter("databricks"), "databricks");
        assert_eq!(map_dbt_adapter("spark"), "databricks");
        assert_eq!(map_dbt_adapter("duckdb"), "duckdb");
        assert_eq!(map_dbt_adapter("mysql"), "mysql");
        assert_eq!(map_dbt_adapter("clickhouse"), "clickhouse");
        // Unknown adapter passes through
        assert_eq!(map_dbt_adapter("trino"), "trino");
    }

    #[test]
    fn test_map_cube_db_type() {
        assert_eq!(map_cube_db_type("postgres"), "postgres");
        assert_eq!(map_cube_db_type("POSTGRES"), "postgres");
        assert_eq!(map_cube_db_type("mysql"), "mysql");
        assert_eq!(map_cube_db_type("mysqlauroraserverless"), "mysql");
        assert_eq!(map_cube_db_type("bigquery"), "bigquery");
        assert_eq!(map_cube_db_type("snowflake"), "snowflake");
        assert_eq!(map_cube_db_type("clickhouse"), "clickhouse");
        assert_eq!(map_cube_db_type("databricks-jdbc"), "databricks");
        assert_eq!(map_cube_db_type("prestodb"), "presto");
        assert_eq!(map_cube_db_type("trino"), "presto");
        assert_eq!(map_cube_db_type("questdb"), "postgres");
    }

    #[test]
    fn test_extract_cube_connection() {
        let dir = tempfile::tempdir().unwrap();
        let env_content = r#"
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=db.example.com
CUBEJS_DB_PORT=5432
CUBEJS_DB_NAME=analytics
CUBEJS_DB_USER=cube_user
CUBEJS_DB_PASS=secret123
# This comment should be skipped
CUBEJS_DB_SCHEMA=public
"#;
        let env_path = dir.path().join(".env");
        std::fs::write(&env_path, env_content).unwrap();

        let info = extract_cube_connection(dir.path());
        assert_eq!(info.get("db_type").unwrap(), "postgres");
        assert_eq!(info.get("host").unwrap(), "db.example.com");
        assert_eq!(info.get("port").unwrap(), "5432");
        assert_eq!(info.get("database").unwrap(), "analytics");
        assert_eq!(info.get("user").unwrap(), "cube_user");
        assert_eq!(info.get("schema").unwrap(), "public");
        // Password should NOT be extracted
        assert_eq!(info.get("password"), None);
    }

    #[test]
    fn test_extract_cube_connection_quoted_values() {
        let dir = tempfile::tempdir().unwrap();
        let env_content = r#"CUBEJS_DB_TYPE="snowflake"
CUBEJS_DB_SNOWFLAKE_ACCOUNT='myaccount'
CUBEJS_DB_SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
"#;
        let env_path = dir.path().join(".env");
        std::fs::write(&env_path, env_content).unwrap();

        let info = extract_cube_connection(dir.path());
        assert_eq!(info.get("db_type").unwrap(), "snowflake");
        assert_eq!(info.get("account").unwrap(), "myaccount");
        assert_eq!(info.get("warehouse").unwrap(), "COMPUTE_WH");
    }

    #[test]
    fn test_extract_cube_connection_missing_env() {
        let dir = tempfile::tempdir().unwrap();
        // No .env file
        let info = extract_cube_connection(dir.path());
        assert!(info.is_empty());
    }

    #[test]
    fn test_extract_dbt_connection() {
        let dir = tempfile::tempdir().unwrap();

        // Create dbt_project.yml
        let project_content = "name: my_project\nprofile: my_profile\n";
        std::fs::write(dir.path().join("dbt_project.yml"), project_content).unwrap();

        // Create ~/.dbt/profiles.yml in a temp location
        let home_dir = tempfile::tempdir().unwrap();
        let dbt_dir = home_dir.path().join(".dbt");
        std::fs::create_dir_all(&dbt_dir).unwrap();
        let profiles_content = r#"
my_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: myaccount.us-east-1
      user: myuser
      warehouse: COMPUTE_WH
      database: ANALYTICS
      schema: PUBLIC
"#;
        std::fs::write(dbt_dir.join("profiles.yml"), profiles_content).unwrap();

        // Override HOME for the test
        let orig_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", home_dir.path());

        let info = extract_dbt_connection(dir.path());

        // Restore HOME
        if let Some(h) = orig_home {
            std::env::set_var("HOME", h);
        }

        assert_eq!(info.get("db_type").unwrap(), "snowflake");
        assert_eq!(info.get("account").unwrap(), "myaccount.us-east-1");
        assert_eq!(info.get("user").unwrap(), "myuser");
        assert_eq!(info.get("warehouse").unwrap(), "COMPUTE_WH");
        assert_eq!(info.get("database").unwrap(), "ANALYTICS");
        assert_eq!(info.get("schema").unwrap(), "PUBLIC");
    }

    #[test]
    fn test_extract_dbt_connection_missing_profile() {
        let dir = tempfile::tempdir().unwrap();

        // Create dbt_project.yml pointing to nonexistent profile
        let project_content = "name: my_project\nprofile: nonexistent\n";
        std::fs::write(dir.path().join("dbt_project.yml"), project_content).unwrap();

        let info = extract_dbt_connection(dir.path());
        // Should return empty — graceful failure
        assert!(info.is_empty() || !info.contains_key("db_type"));
    }

    #[test]
    fn test_extract_dbt_connection_no_project() {
        let dir = tempfile::tempdir().unwrap();
        // No dbt_project.yml
        let info = extract_dbt_connection(dir.path());
        assert!(info.is_empty());
    }

    #[test]
    fn test_field_default() {
        assert_eq!(field_default("postgres", "host"), Some("localhost"));
        assert_eq!(field_default("postgres", "port"), Some("5432"));
        assert_eq!(
            field_default("postgres", "password_var"),
            Some("PG_PASSWORD")
        );
        assert_eq!(
            field_default("snowflake", "password_var"),
            Some("SNOWFLAKE_PASSWORD")
        );
        assert_eq!(
            field_default("bigquery", "access_token_var"),
            Some("BIGQUERY_ACCESS_TOKEN")
        );
        assert_eq!(
            field_default("databricks", "token_var"),
            Some("DATABRICKS_TOKEN")
        );
        assert_eq!(field_default("postgres", "database"), None);
    }

    #[test]
    fn test_field_label() {
        assert_eq!(field_label("host"), "Host");
        assert_eq!(field_label("password_var"), "Password env var");
        assert_eq!(field_label("access_token_var"), "Access token env var");
        assert_eq!(field_label("unknown_field"), "unknown_field");
    }
}
