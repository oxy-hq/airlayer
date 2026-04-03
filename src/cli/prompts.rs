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
                for c in &mut checked {
                    *c = !all_checked;
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
