use crate::dialect::Dialect;
use crate::engine::query::{FilterOperator, QueryFilter, QueryRequest};
use crate::engine::{DatasourceDialectMap, PartialConfig, SemanticEngine};
use crate::schema::globals::GlobalSemantics;
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "airlayer")]
#[command(about = "airlayer — in-process semantic engine for SQL generation")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Compile a query to SQL from .view.yml definitions.
    Query {
        /// Base directory containing views/ and/or topics/ subdirectories. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,

        /// Path to config.yml for datasource→dialect mapping (optional).
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Default SQL dialect (postgres, mysql, bigquery, snowflake, duckdb, clickhouse, databricks, redshift, sqlite, domo).
        #[arg(short, long)]
        dialect: Option<String>,

        /// Query JSON string, or '-' for stdin. Alternative to --dimensions/--measures/--filter flags.
        #[arg(short = 'q', long)]
        query: Option<String>,

        /// Dimensions to select (e.g., orders.status). Can be repeated.
        #[arg(long)]
        dimensions: Vec<String>,

        /// Measures to select (e.g., orders.total_revenue). Can be repeated.
        #[arg(long)]
        measures: Vec<String>,

        /// Filters as member:operator:value (e.g., orders.status:equals:active). Multiple values with commas (orders.status:in:active,pending). Can be repeated.
        #[arg(short, long)]
        filter: Vec<String>,

        /// Order by (e.g., orders.status:asc or orders.total_revenue:desc).
        #[arg(long)]
        order: Vec<String>,

        /// Limit number of rows.
        #[arg(long)]
        limit: Option<u64>,

        /// Offset.
        #[arg(long)]
        offset: Option<u64>,

        /// Segments to apply (e.g., users.active). Can be repeated.
        #[arg(long)]
        segments: Vec<String>,

        /// Entity names to route multi-hop joins through. Can be repeated.
        #[arg(long)]
        through: Vec<String>,

        /// Execute the compiled query against the database and return structured JSON results.
        /// Requires --config with database connection details and an exec-* feature flag.
        #[arg(short = 'x', long)]
        execute: bool,

        /// Which datasource (database name) from config.yml to execute against.
        /// Defaults to the first database in config.yml.
        #[arg(long)]
        datasource: Option<String>,
    },

    /// Validate .view.yml files.
    Validate {
        /// Base directory containing views/ and/or topics/ subdirectories. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,
    },

    /// List all views, dimensions, and measures.
    Inspect {
        /// Base directory containing views/ and/or topics/ subdirectories. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,

        /// Show only a specific view.
        #[arg(long)]
        view: Option<String>,

        /// Output as machine-readable JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
}

/// Parse a filter string like "member:operator:value" into a QueryFilter.
fn parse_filter(s: &str) -> Result<QueryFilter, String> {
    // Split on first two colons: member:operator:value(s)
    let mut parts = s.splitn(3, ':');
    let member = parts
        .next()
        .ok_or_else(|| format!("Invalid filter: '{}'", s))?
        .to_string();
    let op_str = parts
        .next()
        .ok_or_else(|| format!("Invalid filter '{}': expected member:operator:value", s))?;
    let operator = FilterOperator::from_str(op_str)
        .ok_or_else(|| format!("Unknown filter operator '{}' in filter '{}'", op_str, s))?;

    // set/notSet don't require a value part
    let values = if operator == FilterOperator::Set || operator == FilterOperator::NotSet {
        vec![]
    } else {
        let val_str = parts.next().ok_or_else(|| {
            format!(
                "Invalid filter '{}': expected member:operator:value",
                s
            )
        })?;
        val_str.split(',').map(|v| v.to_string()).collect()
    };

    Ok(QueryFilter {
        member: Some(member),
        operator: Some(operator),
        values,
        and: None,
        or: None,
    })
}

/// Parse an order string like "member:asc" or "member:desc" into an OrderBy.
fn parse_order(s: &str) -> Result<crate::engine::query::OrderBy, String> {
    let parts: Vec<&str> = s.rsplitn(2, ':').collect();
    match parts.len() {
        1 => Ok(crate::engine::query::OrderBy {
            id: parts[0].to_string(),
            desc: false,
        }),
        2 => {
            let dir = parts[0]; // rsplitn reverses order
            let id = parts[1];
            let desc = match dir {
                "desc" => true,
                "asc" => false,
                _ => return Err(format!("Invalid order direction '{}' in '{}'. Use :asc or :desc", dir, s)),
            };
            Ok(crate::engine::query::OrderBy {
                id: id.to_string(),
                desc,
            })
        }
        _ => Err(format!("Invalid order: '{}'", s)),
    }
}

/// Build a QueryRequest from shorthand CLI flags.
fn build_query_from_flags(
    dimensions: Vec<String>,
    measures: Vec<String>,
    filters: Vec<String>,
    order: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    segments: Vec<String>,
    through: Vec<String>,
) -> Result<QueryRequest, String> {
    let parsed_filters: Vec<QueryFilter> = filters
        .iter()
        .map(|f| parse_filter(f))
        .collect::<Result<_, _>>()?;

    let parsed_order: Vec<crate::engine::query::OrderBy> = order
        .iter()
        .map(|o| parse_order(o))
        .collect::<Result<_, _>>()?;

    Ok(QueryRequest {
        dimensions,
        measures,
        filters: parsed_filters,
        segments,
        time_dimensions: vec![],
        order: parsed_order,
        limit,
        offset,
        timezone: None,
        ungrouped: false,
        through,
    })
}

/// Build a DatasourceDialectMap from --config and/or --dialect flags.
fn build_dialect_map(
    config: Option<&PathBuf>,
    dialect: Option<&str>,
) -> Result<DatasourceDialectMap, Box<dyn std::error::Error>> {
    let mut map = if let Some(config_path) = config {
        let content = std::fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;
        let partial: PartialConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse config {}: {}", config_path.display(), e))?;
        DatasourceDialectMap::from_config_databases(&partial.databases)
    } else {
        DatasourceDialectMap::new()
    };

    if let Some(d) = dialect {
        let dialect = Dialect::from_str(d)
            .ok_or_else(|| format!("Unknown dialect: {}", d))?;
        map.set_default(dialect);
    }

    if config.is_none() && dialect.is_none() {
        map.set_default(Dialect::Postgres);
    }

    Ok(map)
}

/// Discover views and topics from a base directory.
/// Looks for `views/` and `topics/` subdirectories. At least one must exist.
fn load_from_directory(
    parser: &SchemaParser,
    base_dir: &Path,
) -> Result<SemanticLayer, Box<dyn std::error::Error>> {
    let views_dir = base_dir.join("views");
    let topics_dir = base_dir.join("topics");

    let has_views = views_dir.is_dir();
    let has_topics = topics_dir.is_dir();

    if !has_views && !has_topics {
        return Err(format!(
            "No views/ or topics/ subdirectory found in {}",
            base_dir.display()
        )
        .into());
    }

    let all_views = if has_views {
        parser.parse_views(&views_dir)?
    } else {
        vec![]
    };

    let topics = if has_topics {
        let layer = parser.parse_directory(&topics_dir, Some(&topics_dir))?;
        let t = layer.topics_list().to_vec();
        if t.is_empty() { None } else { Some(t) }
    } else {
        None
    };

    Ok(SemanticLayer::new(all_views, topics))
}

fn make_parser(globals: Option<&PathBuf>) -> Result<SchemaParser, Box<dyn std::error::Error>> {
    if let Some(globals_path) = globals {
        let g = GlobalSemantics::load_from_file(globals_path)?;
        Ok(SchemaParser::with_globals(g))
    } else {
        Ok(SchemaParser::new())
    }
}

/// Resolve the base directory from the --path flag or default to cwd.
fn resolve_base_dir(path: Option<&PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    match path {
        Some(p) => {
            if !p.is_dir() {
                return Err(format!("Path does not exist or is not a directory: {}", p.display()).into());
            }
            Ok(p.clone())
        }
        None => std::env::current_dir().map_err(|e| format!("Failed to get current directory: {}", e).into()),
    }
}

/// Print a QueryEnvelope as pretty JSON to stdout.
fn print_envelope(envelope: &crate::executor::QueryEnvelope) {
    // Errors go to stderr so the JSON on stdout is always clean/parseable
    let json = serde_json::to_string_pretty(envelope).expect("serialize envelope");
    println!("{}", json);
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            path,
            globals,
            config,
            dialect,
            query,
            dimensions,
            measures,
            filter,
            order,
            limit,
            offset,
            segments,
            through,
            execute,
            datasource,
        } => {
            // When --execute is set, ALL output goes through the envelope.
            // Errors at any stage produce an envelope with the appropriate status.
            if execute {
                run_execute(
                    path, globals, config, dialect, query, dimensions, measures,
                    filter, order, limit, offset, segments, through, datasource,
                );
            } else {
                run_compile(
                    path, globals, config, dialect, query, dimensions, measures,
                    filter, order, limit, offset, segments, through,
                )?;
            }
        }

        Commands::Validate {
            path,
            globals,
        } => {
            let base_dir = resolve_base_dir(path.as_ref())?;
            let parser = make_parser(globals.as_ref())?;
            let layer = load_from_directory(&parser, &base_dir)?;

            match crate::schema::validator::SchemaValidator::validate(&layer) {
                Ok(()) => {
                    println!("Schema is valid.");
                    println!(
                        "  {} views, {} topics",
                        layer.views.len(),
                        layer.topics_list().len()
                    );
                }
                Err(e) => {
                    eprintln!("Schema validation errors:\n{}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Inspect {
            path,
            globals,
            view,
            json,
        } => {
            let base_dir = resolve_base_dir(path.as_ref())?;
            let parser = make_parser(globals.as_ref())?;
            let layer = load_from_directory(&parser, &base_dir)?;

            let views_to_show: Vec<&crate::schema::models::View> = if let Some(ref name) = view {
                layer
                    .views
                    .iter()
                    .filter(|v| v.name == *name)
                    .collect()
            } else {
                layer.views.iter().collect()
            };

            if json {
                // Machine-readable JSON output for agent consumption
                let output = inspect_json(&views_to_show);
                println!("{}", serde_json::to_string_pretty(&output).expect("serialize inspect"));
            } else {
                // Human-readable text output
                for v in &views_to_show {
                    println!("view: {}", v.name);
                    if let Some(ref desc) = Some(&v.description) {
                        println!("  description: {}", desc);
                    }
                    if let Some(ref table) = v.table {
                        println!("  table: {}", table);
                    }
                    if let Some(ref sql) = v.sql {
                        println!("  sql: {}", sql);
                    }

                    if !v.entities.is_empty() {
                        println!("  entities:");
                        for e in &v.entities {
                            let kind = match e.entity_type {
                                crate::schema::models::EntityType::Primary => "primary",
                                crate::schema::models::EntityType::Foreign => "foreign",
                            };
                            println!(
                                "    - {} ({}, keys: {:?})",
                                e.name,
                                kind,
                                e.get_keys()
                            );
                        }
                    }

                    println!("  dimensions:");
                    for d in &v.dimensions {
                        println!(
                            "    - {}: {} (expr: {})",
                            d.name, d.dimension_type, d.expr
                        );
                    }

                    if !v.measures_list().is_empty() {
                        println!("  measures:");
                        for m in v.measures_list() {
                            let expr = m.expr.as_deref().unwrap_or("*");
                            println!("    - {}: {} (expr: {})", m.name, m.measure_type, expr);
                        }
                    }
                    println!();
                }
            }

            if views_to_show.is_empty() {
                if let Some(name) = view {
                    eprintln!("View '{}' not found", name);
                    std::process::exit(1);
                } else if !json {
                    println!("No views found.");
                }
            }
        }
    }

    Ok(())
}

/// Build machine-readable JSON for `airlayer inspect --json`.
/// This is the schema introspection surface — an agent discovers the semantic vocabulary here.
fn inspect_json(views: &[&crate::schema::models::View]) -> serde_json::Value {
    let views_json: Vec<serde_json::Value> = views
        .iter()
        .map(|v| {
            let dimensions: Vec<serde_json::Value> = v
                .dimensions
                .iter()
                .map(|d| {
                    let mut obj = serde_json::json!({
                        "name": format!("{}.{}", v.name, d.name),
                        "type": format!("{}", d.dimension_type),
                        "expr": d.expr,
                    });
                    if let Some(ref desc) = d.description {
                        obj["description"] = serde_json::Value::String(desc.clone());
                    }
                    if let Some(ref samples) = d.samples {
                        obj["samples"] = serde_json::json!(samples);
                    }
                    obj
                })
                .collect();

            let measures: Vec<serde_json::Value> = v
                .measures_list()
                .iter()
                .map(|m| {
                    let mut obj = serde_json::json!({
                        "name": format!("{}.{}", v.name, m.name),
                        "type": format!("{}", m.measure_type),
                    });
                    if let Some(ref expr) = m.expr {
                        obj["expr"] = serde_json::Value::String(expr.clone());
                    }
                    if let Some(ref desc) = m.description {
                        obj["description"] = serde_json::Value::String(desc.clone());
                    }
                    obj
                })
                .collect();

            let segments: Vec<serde_json::Value> = v
                .segments
                .iter()
                .map(|s| {
                    let mut obj = serde_json::json!({
                        "name": format!("{}.{}", v.name, s.name),
                        "expr": s.expr,
                    });
                    if let Some(ref desc) = s.description {
                        obj["description"] = serde_json::Value::String(desc.clone());
                    }
                    obj
                })
                .collect();

            let mut view_obj = serde_json::json!({
                "name": v.name,
                "description": v.description,
                "dimensions": dimensions,
                "measures": measures,
            });
            if !segments.is_empty() {
                view_obj["segments"] = serde_json::json!(segments);
            }
            view_obj
        })
        .collect();

    serde_json::json!({ "views": views_json })
}

/// Compile-only path (no --execute). Prints raw SQL to stdout.
fn run_compile(
    path: Option<PathBuf>,
    globals: Option<PathBuf>,
    config: Option<PathBuf>,
    dialect: Option<String>,
    query: Option<String>,
    dimensions: Vec<String>,
    measures: Vec<String>,
    filter: Vec<String>,
    order: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    segments: Vec<String>,
    through: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = resolve_base_dir(path.as_ref())?;
    let dialects = build_dialect_map(config.as_ref(), dialect.as_deref())?;
    let parser = make_parser(globals.as_ref())?;
    let layer = load_from_directory(&parser, &base_dir)?;
    let engine = SemanticEngine::from_semantic_layer(layer, dialects)?;

    let request = parse_query_input(query, dimensions, measures, filter, order, limit, offset, segments, through)?;
    let result = engine.compile_query(&request)?;

    println!("{}", result.sql);
    if !result.params.is_empty() {
        eprintln!("-- params: {:?}", result.params);
    }
    Ok(())
}

/// Execute path (--execute). Always outputs a QueryEnvelope as JSON — even on errors.
/// This function never returns Err; all errors are captured in the envelope.
#[allow(clippy::too_many_arguments)]
fn run_execute(
    path: Option<PathBuf>,
    globals: Option<PathBuf>,
    config: Option<PathBuf>,
    dialect: Option<String>,
    query: Option<String>,
    dimensions: Vec<String>,
    measures: Vec<String>,
    filter: Vec<String>,
    order: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    segments: Vec<String>,
    through: Vec<String>,
    datasource: Option<String>,
) {
    use crate::executor::QueryEnvelope;

    // --- Stage 1: parse views & build engine ---
    let base_dir = match resolve_base_dir(path.as_ref()) {
        Ok(d) => d,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };
    let dialects = match build_dialect_map(config.as_ref(), dialect.as_deref()) {
        Ok(d) => d,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };
    let parser = match make_parser(globals.as_ref()) {
        Ok(p) => p,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };
    let layer = match load_from_directory(&parser, &base_dir) {
        Ok(l) => l,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };
    let engine = match SemanticEngine::from_semantic_layer(layer, dialects) {
        Ok(e) => e,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };

    // --- Stage 2: parse query input ---
    let request = match parse_query_input(query, dimensions, measures, filter, order, limit, offset, segments, through) {
        Ok(r) => r,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("parse_error", e.to_string(), None, &[], vec![]));
            return;
        }
    };

    let views_used = request.referenced_views();

    // --- Stage 3: compile query ---
    let result = match engine.compile_query(&request) {
        Ok(r) => r,
        Err(e) => {
            print_envelope(&QueryEnvelope::error("compile_error", e.to_string(), None, &[], views_used));
            return;
        }
    };

    // --- Stage 4: resolve connection ---
    let config_path = match config.as_ref() {
        Some(p) => p,
        None => {
            print_envelope(&QueryEnvelope::error(
                "execution_error",
                "--execute requires --config with database connection details".to_string(),
                Some(result.sql),
                &result.columns,
                views_used,
            ));
            return;
        }
    };
    let content = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            print_envelope(&QueryEnvelope::error(
                "execution_error",
                format!("Failed to read config {}: {}", config_path.display(), e),
                Some(result.sql),
                &result.columns,
                views_used,
            ));
            return;
        }
    };
    let exec_config: crate::executor::ExecutionConfig = match serde_yaml::from_str(&content) {
        Ok(c) => c,
        Err(e) => {
            print_envelope(&QueryEnvelope::error(
                "execution_error",
                format!("Failed to parse config: {}", e),
                Some(result.sql),
                &result.columns,
                views_used,
            ));
            return;
        }
    };
    let connection = match if let Some(ref ds) = datasource {
        exec_config.find_connection(ds)
    } else {
        exec_config.first_connection()
    } {
        Ok(c) => c,
        Err(e) => {
            print_envelope(&QueryEnvelope::error(
                "execution_error",
                e.to_string(),
                Some(result.sql),
                &result.columns,
                views_used,
            ));
            return;
        }
    };

    // --- Stage 5: execute ---
    match crate::executor::execute(&connection, &result.sql, &result.params) {
        Ok(exec_result) => {
            print_envelope(&QueryEnvelope::success(
                result.sql,
                &result.columns,
                exec_result,
                views_used,
            ));
        }
        Err(e) => {
            print_envelope(&QueryEnvelope::error(
                "execution_error",
                e.to_string(),
                Some(result.sql),
                &result.columns,
                views_used,
            ));
        }
    }
}

/// Parse query input from either -q JSON or --dimensions/--measures flags.
fn parse_query_input(
    query: Option<String>,
    dimensions: Vec<String>,
    measures: Vec<String>,
    filter: Vec<String>,
    order: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    segments: Vec<String>,
    through: Vec<String>,
) -> Result<QueryRequest, Box<dyn std::error::Error>> {
    let has_flags = !dimensions.is_empty() || !measures.is_empty();

    if let Some(q) = query {
        if has_flags {
            return Err("Cannot use both -q/--query and --dimensions/--measures flags".into());
        }
        let query_str = if q == "-" {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
            buf
        } else {
            q
        };
        let request: QueryRequest = serde_json::from_str(&query_str)
            .map_err(|e| format!("Invalid query JSON: {}", e))?;
        Ok(request)
    } else if has_flags {
        Ok(build_query_from_flags(dimensions, measures, filter, order, limit, offset, segments, through)?)
    } else {
        Err("Provide either -q/--query (JSON) or --dimensions/--measures flags".into())
    }
}
