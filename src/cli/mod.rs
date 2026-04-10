mod bootstrap;
mod prompts;

use crate::dialect::Dialect;
use crate::engine::query::{FilterOperator, QueryFilter, QueryRequest};
use crate::engine::{DatasourceDialectMap, PartialConfig, SemanticEngine};
use crate::schema::foreign::ForeignFormat;
use crate::schema::globals::GlobalSemantics;
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;
use clap::{Parser, Subcommand};
use std::io::IsTerminal;
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
    ///
    /// Can run inline queries (--dimension/--measure flags or -q JSON),
    /// or saved queries by filepath (e.g., `airlayer query queries/revenue.query.yml`).
    Query {
        /// Path to a .query.yml file.
        /// When provided, runs the saved query instead of inline flags.
        #[arg(value_name = "FILE")]
        name: Option<String>,

        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,

        /// Path to config.yml for datasource→dialect mapping (optional).
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Default SQL dialect (postgres, mysql, bigquery, snowflake, duckdb, clickhouse, databricks, redshift, sqlite, domo, presto).
        #[arg(short, long)]
        dialect: Option<String>,

        /// Query JSON string, or '-' for stdin. Alternative to --dimension/--measure/--filter flags.
        #[arg(short = 'q', long)]
        query: Option<String>,

        /// Dimension to select (e.g., orders.status). Can be repeated.
        #[arg(long = "dimension", visible_alias = "dimensions")]
        dimensions: Vec<String>,

        /// Measure to select (e.g., orders.total_revenue). Can be repeated.
        #[arg(long = "measure", visible_alias = "measures")]
        measures: Vec<String>,

        /// Filter as member:operator:value (e.g., orders.status:equals:active). Multiple values with commas (orders.status:in:active,pending). Can be repeated.
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

        /// Motif to apply as post-aggregation transform (e.g., yoy, mom, anomaly, contribution).
        #[arg(long)]
        motif: Option<String>,

        /// Motif parameter as key=value (e.g., measure=orders.total_revenue, threshold=3). Can be repeated.
        #[arg(long = "motif-param")]
        motif_param: Vec<String>,

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
        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,
    },

    /// Initialize an airlayer project with config.yml, CLAUDE.md, and Claude Code skills.
    Init {
        /// Target directory to initialize. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Database type — generates a targeted config template and skips the type selection prompt.
        #[arg(long, value_name = "DB_TYPE")]
        r#type: Option<String>,
    },

    /// Update CLAUDE.md and Claude Code skills to the latest version.
    Update {
        /// Target directory to update. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// Test the database connection defined in config.yml.
    #[command(name = "test-connection")]
    TestConnection {
        /// Path to config.yml.
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Which datasource (database name) to test. Defaults to first.
        #[arg(long)]
        datasource: Option<String>,
    },

    /// Convert foreign semantic models (Cube.js, LookML, dbt, Omni) to airlayer .view.yml format.
    Convert {
        /// Source format: cube, lookml, dbt, omni.
        #[arg(long, alias = "from")]
        format: String,

        /// Input file or directory containing foreign model files.
        #[arg(value_name = "PATH")]
        input: PathBuf,

        /// Output directory for generated .view.yml files. Defaults to current directory.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Default SQL dialect for generated views (postgres, bigquery, snowflake, etc.).
        #[arg(short, long)]
        dialect: Option<String>,

        /// Print generated YAML to stdout instead of writing files.
        #[arg(long)]
        stdout: bool,
    },

    /// List all views, dimensions, and measures.
    Inspect {
        /// Path to globals file (optional).
        #[arg(short, long)]
        globals: Option<PathBuf>,

        /// Show only a specific view.
        #[arg(long)]
        view: Option<String>,

        /// Output as machine-readable JSON instead of human-readable text.
        #[arg(long)]
        json: bool,

        /// Profile a dimension or all dimensions in a view. Runs type-aware data profiling
        /// against the database (requires --config). Format: "view.dimension" or "view" (all).
        #[arg(long)]
        profile: Option<String>,

        /// Path to config.yml for database connection (required for --profile).
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Default SQL dialect (used for --profile if no config).
        #[arg(short, long)]
        dialect: Option<String>,

        /// Which datasource (database name) from config.yml to execute against.
        #[arg(long)]
        datasource: Option<String>,

        /// Introspect the database schema (tables, columns, types). Requires --config.
        /// Optionally filter to a specific schema/dataset name.
        #[arg(long)]
        schema: Option<Option<String>>,

        /// List available motifs (builtins + custom .motif.yml files) with params and outputs.
        #[arg(long)]
        motifs: bool,

        /// List saved queries (.query.yml files) with steps.
        #[arg(long)]
        queries: bool,
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
        let val_str = parts
            .next()
            .ok_or_else(|| format!("Invalid filter '{}': expected member:operator:value", s))?;
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
                _ => {
                    return Err(format!(
                        "Invalid order direction '{}' in '{}'. Use :asc or :desc",
                        dir, s
                    ))
                }
            };
            Ok(crate::engine::query::OrderBy {
                id: id.to_string(),
                desc,
            })
        }
        _ => Err(format!("Invalid order: '{}'", s)),
    }
}

/// Parse a `--motif-param key=value` string into a (key, serde_json::Value) pair.
/// Numeric values are parsed as numbers; everything else becomes a string.
fn parse_motif_param(s: &str) -> Result<(String, serde_json::Value), String> {
    let eq_pos = s
        .find('=')
        .ok_or_else(|| format!("Invalid --motif-param '{}'. Expected key=value format.", s))?;
    let key = s[..eq_pos].to_string();
    let val_str = &s[eq_pos + 1..];
    let value = if let Ok(n) = val_str.parse::<f64>() {
        // Preserve integer representation when possible
        if n.fract() == 0.0 && n.abs() < (i64::MAX as f64) {
            serde_json::Value::Number(serde_json::Number::from(n as i64))
        } else {
            serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(val_str.to_string()))
        }
    } else {
        serde_json::Value::String(val_str.to_string())
    };
    Ok((key, value))
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
    motif: Option<String>,
    motif_param: Vec<String>,
) -> Result<QueryRequest, String> {
    let parsed_filters: Vec<QueryFilter> = filters
        .iter()
        .map(|f| parse_filter(f))
        .collect::<Result<_, _>>()?;

    let parsed_order: Vec<crate::engine::query::OrderBy> = order
        .iter()
        .map(|o| parse_order(o))
        .collect::<Result<_, _>>()?;

    let mut motif_params = std::collections::HashMap::new();
    for mp in &motif_param {
        let (key, value) = parse_motif_param(mp)?;
        motif_params.insert(key, value);
    }

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
        motif,
        motif_params,
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
        let dialect = Dialect::from_str(d).ok_or_else(|| format!("Unknown dialect: {}", d))?;
        map.set_default(dialect);
    }

    if config.is_none() && dialect.is_none() {
        map.set_default(Dialect::Postgres);
    }

    Ok(map)
}

/// Discover views, topics, motifs, and saved queries from a base directory.
/// For each type, prefers its conventional subdirectory (e.g. `views/`) when it
/// exists, and falls back to scanning the base directory itself.
fn load_from_directory(
    parser: &SchemaParser,
    base_dir: &Path,
) -> Result<SemanticLayer, Box<dyn std::error::Error>> {
    let views_dir = base_dir.join("views");
    let topics_dir = base_dir.join("topics");
    let motifs_dir = base_dir.join("motifs");
    let queries_dir = base_dir.join("queries");

    let effective_views_dir = if views_dir.is_dir() {
        &views_dir
    } else {
        base_dir
    };
    let all_views = parser.parse_views(effective_views_dir)?;

    let effective_topics_dir = if topics_dir.is_dir() {
        &topics_dir
    } else {
        base_dir
    };
    let t = parser.parse_topics(effective_topics_dir)?;
    let topics = if t.is_empty() { None } else { Some(t) };

    let effective_motifs_dir = if motifs_dir.is_dir() {
        &motifs_dir
    } else {
        base_dir
    };
    let m = parser.parse_motifs(effective_motifs_dir)?;
    let motifs = if m.is_empty() { None } else { Some(m) };

    let effective_queries_dir = if queries_dir.is_dir() {
        &queries_dir
    } else {
        base_dir
    };
    let q = parser.parse_saved_queries(effective_queries_dir)?;
    let saved_queries = if q.is_empty() { None } else { Some(q) };

    if all_views.is_empty() {
        return Err(format!("No .view.yml files found in {}", base_dir.display()).into());
    }

    Ok(SemanticLayer::with_motifs_and_queries(
        all_views,
        topics,
        motifs,
        saved_queries,
    ))
}

fn make_parser(globals: Option<&PathBuf>) -> Result<SchemaParser, Box<dyn std::error::Error>> {
    if let Some(globals_path) = globals {
        let g = GlobalSemantics::load_from_file(globals_path)?;
        Ok(SchemaParser::with_globals(g))
    } else {
        Ok(SchemaParser::new())
    }
}

/// Walk up from a starting directory looking for the project root.
/// A project root is identified by the presence of `config.yml`.
/// Returns `None` if not found before reaching the filesystem root.
fn find_project_root(from: &Path) -> Option<PathBuf> {
    let mut dir = from.to_path_buf();
    loop {
        if dir.join("config.yml").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

/// Resolved project context: base directory and optional config path.
struct ProjectContext {
    base_dir: PathBuf,
    config_path: Option<PathBuf>,
}

/// Resolve the project root and config path from CLI flags, with auto-detection fallback.
///
/// Resolution order:
///  1. Walk up from cwd looking for config.yml
///  2. Auto-detected root provides both base_dir and config_path (if config.yml exists there)
///  3. Final fallback → cwd as base_dir, no config
///
/// `--config` explicit override always wins for config_path.
fn resolve_project_context(
    config: Option<&PathBuf>,
) -> Result<ProjectContext, Box<dyn std::error::Error>> {
    let cwd =
        std::env::current_dir().map_err(|e| format!("Failed to get current directory: {}", e))?;

    let (base_dir, auto_config) = if let Some(root) = find_project_root(&cwd) {
        // Auto-detected project root
        let auto_cfg = root.join("config.yml");
        let auto_cfg = if auto_cfg.is_file() {
            Some(auto_cfg)
        } else {
            None
        };
        (root, auto_cfg)
    } else {
        // Fallback: cwd
        let auto_cfg = cwd.join("config.yml");
        let auto_cfg = if auto_cfg.is_file() {
            Some(auto_cfg)
        } else {
            None
        };
        (cwd, auto_cfg)
    };

    // Explicit --config always wins
    let config_path = if let Some(c) = config {
        Some(c.clone())
    } else {
        auto_config
    };

    Ok(ProjectContext {
        base_dir,
        config_path,
    })
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
            name,
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
            motif,
            motif_param,
            execute,
            datasource,
        } => {
            // Check if this is a saved query file
            let is_named = name.is_some();
            let has_inline = query.is_some() || !dimensions.is_empty() || !measures.is_empty();

            if is_named && has_inline {
                return Err("Cannot use a saved query file with inline query flags (-q/--dimension/--measure)".into());
            }

            if let Some(ref query_name) = name {
                // Saved query file mode
                if execute {
                    run_saved_query_execute(
                        query_name,
                        globals.as_ref(),
                        config.as_ref(),
                        dialect.as_deref(),
                        datasource.as_deref(),
                    );
                } else {
                    run_saved_query_compile(
                        query_name,
                        globals.as_ref(),
                        config.as_ref(),
                        dialect.as_deref(),
                    )?;
                }
            } else if execute {
                run_execute(
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
                    motif,
                    motif_param,
                    datasource,
                );
            } else {
                run_compile(
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
                    motif,
                    motif_param,
                )?;
            }
        }

        Commands::Init { path, r#type } => {
            run_init(path.as_ref(), r#type.as_deref())?;
        }

        Commands::Update { path } => {
            run_update(path.as_ref())?;
        }

        Commands::TestConnection { config, datasource } => {
            run_test_connection(config.as_ref(), datasource.as_deref())?;
        }

        Commands::Convert {
            format,
            input,
            output,
            dialect,
            stdout,
        } => {
            run_convert(&format, &input, output.as_ref(), dialect.as_deref(), stdout)?;
        }

        Commands::Validate { globals } => {
            let ctx = resolve_project_context(None)?;
            let parser = make_parser(globals.as_ref())?;
            let layer = load_from_directory(&parser, &ctx.base_dir)?;

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
            globals,
            view,
            json,
            profile,
            config,
            dialect,
            datasource,
            schema,
            motifs,
            queries,
        } => {
            // --- Schema introspection mode ---
            if let Some(ref schema_filter) = schema {
                let ctx = resolve_project_context(config.as_ref())?;
                run_schema_introspect(
                    ctx.config_path.as_ref(),
                    datasource.as_deref(),
                    schema_filter.as_deref(),
                )?;
                return Ok(());
            }

            let ctx = resolve_project_context(config.as_ref())?;
            let parser = make_parser(globals.as_ref())?;
            let layer = load_from_directory(&parser, &ctx.base_dir)?;

            // --- Profile mode ---
            if let Some(ref profile_target) = profile {
                run_profile(
                    &layer,
                    profile_target,
                    ctx.config_path.as_ref(),
                    dialect.as_deref(),
                    datasource.as_deref(),
                )?;
                return Ok(());
            }

            // --- Motifs mode ---
            if motifs {
                run_inspect_motifs(&layer, json)?;
                return Ok(());
            }

            // --- Queries mode ---
            if queries {
                run_inspect_queries(&layer, json)?;
                return Ok(());
            }

            // --- Normal inspect mode ---
            let views_to_show: Vec<&crate::schema::models::View> = if let Some(ref name) = view {
                layer.views.iter().filter(|v| v.name == *name).collect()
            } else {
                layer.views.iter().collect()
            };

            if json {
                // Machine-readable JSON output for agent consumption
                let output = inspect_json(&views_to_show);
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output).expect("serialize inspect")
                );
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
                            println!("    - {} ({}, keys: {:?})", e.name, kind, e.get_keys());
                        }
                    }

                    println!("  dimensions:");
                    for d in &v.dimensions {
                        println!("    - {}: {} (expr: {})", d.name, d.dimension_type, d.expr);
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

/// Profile mode: run type-aware data profiling for one or all dimensions in a view.
/// Outputs structured JSON to stdout.
fn run_profile(
    layer: &SemanticLayer,
    target: &str,
    config: Option<&PathBuf>,
    dialect: Option<&str>,
    datasource: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    

    // Parse target: "view.dimension" or "view" (all dimensions)
    let (view_name, dim_name) = if let Some(dot) = target.find('.') {
        (&target[..dot], Some(&target[dot + 1..]))
    } else {
        (target, None)
    };

    let view = layer
        .view_by_name(view_name)
        .ok_or_else(|| format!("View '{}' not found", view_name))?;

    // Resolve dialect
    let resolved_dialect = if let Some(d) = dialect {
        Dialect::from_str(d).ok_or_else(|| format!("Unknown dialect: {}", d))?
    } else if let Some(ref d) = view.dialect {
        Dialect::from_str(d).ok_or_else(|| format!("Unknown dialect in view: {}", d))?
    } else {
        Dialect::Postgres // fallback
    };

    // Resolve database connection
    let config_path =
        config.ok_or("--profile requires a config.yml (auto-detected or via --config)")?;
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;

    #[cfg(feature = "exec")]
    {
        let exec_config: crate::executor::ExecutionConfig =
            serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)?
        } else {
            exec_config.first_connection()?
        };

        // Determine which dimensions to profile
        let dims_to_profile: Vec<&crate::schema::models::Dimension> = if let Some(name) = dim_name {
            let d = view
                .dimensions
                .iter()
                .find(|d| d.name == name)
                .ok_or_else(|| format!("Dimension '{}' not found in view '{}'", name, view_name))?;
            vec![d]
        } else {
            view.dimensions.iter().collect()
        };

        let mut profiles = Vec::new();

        for dim in &dims_to_profile {
            let member = format!("{}.{}", view.name, dim.name);
            let plan = profiler::plan_profile(view, &dim.name, &resolved_dialect)?;

            // Execute stats query
            let stats_result = crate::executor::execute(&connection, &plan.stats_sql, &[])?;
            let stats_row = stats_result
                .rows
                .first()
                .ok_or_else(|| format!("No stats returned for {}", member))?;

            // Conditionally execute values query (for strings)
            let values_rows = if let Some(ref values_fn) = plan.values_sql_fn {
                let cardinality = profiler::extract_cardinality(stats_row);
                let values_sql = values_fn(cardinality);
                let values_result = crate::executor::execute(&connection, &values_sql, &[])?;
                Some(values_result.rows)
            } else {
                None
            };

            let profile = profiler::build_profile(
                &member,
                &dim.dimension_type,
                stats_row,
                values_rows.as_deref(),
            );
            profiles.push(profile);
        }

        let output = if profiles.len() == 1 {
            serde_json::to_value(&profiles[0]).expect("serialize profile")
        } else {
            serde_json::to_value(&profiles).expect("serialize profiles")
        };

        println!(
            "{}",
            serde_json::to_string_pretty(&output).expect("format profile")
        );
    }

    #[cfg(not(feature = "exec"))]
    {
        let _ = (content, datasource, resolved_dialect, view, dim_name);
        return Err("--profile requires an exec-* feature flag to be enabled".into());
    }

    Ok(())
}

/// Schema introspection mode: discover tables, columns, and types from the database.
/// Outputs structured JSON to stdout.
fn run_schema_introspect(
    config: Option<&PathBuf>,
    datasource: Option<&str>,
    schema_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_path =
        config.ok_or("--schema requires a config.yml (auto-detected or via --config)")?;
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;

    #[cfg(feature = "exec")]
    {
        let exec_config: crate::executor::ExecutionConfig =
            serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)?
        } else {
            exec_config.first_connection()?
        };

        let mut schema_info = crate::executor::introspect::introspect(&connection)?;

        // Apply optional schema/dataset filter
        if let Some(filter) = schema_filter {
            schema_info
                .tables
                .retain(|t| t.schema.as_deref() == Some(filter));
        }

        let json = serde_json::to_string_pretty(&schema_info).expect("serialize schema");
        println!("{}", json);
    }

    #[cfg(not(feature = "exec"))]
    {
        let _ = (content, datasource, schema_filter);
        return Err("--schema requires an exec-* feature flag to be enabled".into());
    }

    Ok(())
}

/// Inspect motifs: list builtins + custom motifs with params and outputs.
fn run_inspect_motifs(layer: &SemanticLayer, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    use crate::engine::motifs;
    use crate::schema::models::MotifKind;

    let builtins = motifs::builtin_motifs();
    let customs = layer.motifs_list();

    if json {
        let motif_to_json = |m: &crate::schema::models::Motif| -> serde_json::Value {
            let params: serde_json::Value = m
                .params
                .iter()
                .map(|(k, v)| {
                    let mut obj =
                        serde_json::json!({ "type": format!("{:?}", v.param_type).to_lowercase() });
                    if let Some(ref desc) = v.description {
                        obj["description"] = serde_json::Value::String(desc.clone());
                    }
                    if let Some(ref def) = v.default {
                        obj["default"] = def.clone();
                    }
                    if let Some(ref vals) = v.values {
                        obj["values"] = serde_json::json!(vals);
                    }
                    (k.clone(), obj)
                })
                .collect::<serde_json::Map<String, serde_json::Value>>()
                .into();

            let outputs: Vec<serde_json::Value> = m
                .outputs
                .iter()
                .map(|o| serde_json::json!({ "name": o.name, "expr": o.expr }))
                .collect();

            let mut obj = serde_json::json!({
                "name": m.name,
                "kind": match m.motif_kind { MotifKind::Builtin => "builtin", MotifKind::Custom => "custom" },
                "params": params,
                "outputs": outputs,
            });
            if let Some(ref desc) = m.description {
                obj["description"] = serde_json::Value::String(desc.clone());
            }
            obj
        };

        let all: Vec<serde_json::Value> = builtins
            .iter()
            .chain(customs.iter())
            .map(motif_to_json)
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "motifs": all }))
                .expect("serialize motifs")
        );
    } else {
        let print_motif = |m: &crate::schema::models::Motif, kind: &str| {
            println!("motif: {} ({})", m.name, kind);
            if let Some(ref desc) = m.description {
                println!("  description: {}", desc);
            }
            if !m.params.is_empty() {
                println!("  params:");
                for (name, p) in &m.params {
                    let type_str = format!("{:?}", p.param_type).to_lowercase();
                    let desc = p.description.as_deref().unwrap_or("");
                    if let Some(ref def) = p.default {
                        println!("    - {}: {} (default: {}) {}", name, type_str, def, desc);
                    } else {
                        println!("    - {}: {} {}", name, type_str, desc);
                    }
                }
            }
            if !m.outputs.is_empty() {
                println!(
                    "  outputs: {}",
                    m.outputs
                        .iter()
                        .map(|o| o.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            println!();
        };

        for m in &builtins {
            print_motif(m, "builtin");
        }
        for m in customs {
            print_motif(m, "custom");
        }
    }
    Ok(())
}

/// Inspect saved queries: list available queries with steps.
fn run_inspect_queries(
    layer: &SemanticLayer,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = layer.saved_queries_list();
    if queries.is_empty() {
        if json {
            println!("{}", serde_json::json!({ "queries": [] }));
        } else {
            println!("No saved queries found.");
        }
        return Ok(());
    }

    if json {
        let queries_json: Vec<serde_json::Value> = queries
            .iter()
            .map(|s| {
                let steps: Vec<serde_json::Value> = s
                    .effective_steps()
                    .iter()
                    .map(|step| {
                        let mut obj = serde_json::json!({
                            "name": step.name,
                            "query": serde_json::to_value(&step.query).expect("serialize query"),
                        });
                        if let Some(ref desc) = step.description {
                            obj["description"] = serde_json::Value::String(desc.clone());
                        }
                        obj
                    })
                    .collect();

                let params: serde_json::Value = s
                    .params
                    .iter()
                    .map(|(k, v)| {
                        let mut obj = serde_json::json!({ "type": v.param_type });
                        if let Some(ref vals) = v.values {
                            obj["values"] = serde_json::json!(vals);
                        }
                        if let Some(ref def) = v.default {
                            obj["default"] = def.clone();
                        }
                        if let Some(ref desc) = v.description {
                            obj["description"] = serde_json::Value::String(desc.clone());
                        }
                        (k.clone(), obj)
                    })
                    .collect::<serde_json::Map<String, serde_json::Value>>()
                    .into();

                let mut obj = serde_json::json!({
                    "name": s.name,
                    "steps": steps,
                    "params": params,
                });
                if let Some(ref desc) = s.description {
                    obj["description"] = serde_json::Value::String(desc.clone());
                }
                if let Some(ref p) = s.source_path {
                    obj["path"] = serde_json::Value::String(p.display().to_string());
                }
                obj
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "queries": queries_json }))
                .expect("serialize queries")
        );
    } else {
        for s in queries {
            let steps = s.effective_steps();
            let kind = if steps.len() == 1 {
                "single"
            } else {
                "multi-step"
            };
            let path_info = s
                .source_path
                .as_ref()
                .map(|p| format!(" [{}]", p.display()))
                .unwrap_or_default();
            println!("query: {} ({}){}", s.name, kind, path_info);
            if let Some(ref desc) = s.description {
                println!("  description: {}", desc);
            }
            if !s.params.is_empty() {
                println!("  params:");
                for (name, p) in &s.params {
                    let desc = p.description.as_deref().unwrap_or("");
                    if let Some(ref def) = p.default {
                        println!(
                            "    - {}: {} (default: {}) {}",
                            name, p.param_type, def, desc
                        );
                    } else {
                        println!("    - {}: {} {}", name, p.param_type, desc);
                    }
                }
            }
            if steps.len() > 1 {
                println!("  steps:");
                for (i, step) in steps.iter().enumerate() {
                    let desc = step.description.as_deref().unwrap_or("");
                    println!("    {}. {} {}", i + 1, step.name, desc);
                }
            }
            println!();
        }
    }
    Ok(())
}

/// Resolve a saved query by file path.
fn resolve_saved_query(
    file_path: &str,
) -> Result<crate::schema::models::SavedQuery, Box<dyn std::error::Error>> {
    let path = Path::new(file_path);
    if !path.is_file() {
        return Err(format!("Query file not found: {}", file_path).into());
    }
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {}", file_path, e))?;
    let mut query: crate::schema::models::SavedQuery = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse query {}: {}", file_path, e))?;
    query.source_path = Some(path.to_path_buf());
    Ok(query)
}

/// Compile a saved query: compile each step to SQL and print results.
fn run_saved_query_compile(
    name: &str,
    globals: Option<&PathBuf>,
    config: Option<&PathBuf>,
    dialect: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = resolve_project_context(config)?;
    let dialects = build_dialect_map(ctx.config_path.as_ref(), dialect)?;
    let parser = make_parser(globals)?;
    let layer = load_from_directory(&parser, &ctx.base_dir)?;
    let engine = SemanticEngine::from_semantic_layer(layer.clone(), dialects)?;

    let saved_query = resolve_saved_query(name)?;
    let steps = saved_query.effective_steps();

    let mut results = Vec::new();
    for step in &steps {
        let result = engine.compile_query(&step.query)?;
        results.push(serde_json::json!({
            "step": step.name,
            "description": step.description,
            "sql": result.sql,
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "query": name,
            "steps": results,
        }))
        .expect("serialize query results")
    );
    Ok(())
}

/// Execute a saved query: compile and run each step against the database.
/// Always outputs JSON — errors in individual steps produce error envelopes.
fn run_saved_query_execute(
    name: &str,
    globals: Option<&PathBuf>,
    config: Option<&PathBuf>,
    dialect: Option<&str>,
    datasource: Option<&str>,
) {
    use crate::executor::QueryEnvelope;

    fn inner(
        name: &str,
        globals: Option<&PathBuf>,
        config: Option<&PathBuf>,
        dialect: Option<&str>,
        datasource: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let ctx = resolve_project_context(config)?;
        let dialects = build_dialect_map(ctx.config_path.as_ref(), dialect)?;
        let parser = make_parser(globals)?;
        let layer = load_from_directory(&parser, &ctx.base_dir)?;
        let engine = SemanticEngine::from_semantic_layer(layer.clone(), dialects)?;

        let saved_query = resolve_saved_query(name)?;
        let steps = saved_query.effective_steps();

        let config_path = ctx
            .config_path
            .as_ref()
            .ok_or("--execute requires a config.yml (auto-detected or via --config)")?;
        let content = std::fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;
        let exec_config: crate::executor::ExecutionConfig =
            serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)?
        } else {
            exec_config.first_connection()?
        };

        let mut step_results = Vec::new();
        for step in &steps {
            let result = engine.compile_query(&step.query);
            match result {
                Ok(compiled) => {
                    let exec_result =
                        crate::executor::execute(&connection, &compiled.sql, &compiled.params);
                    match exec_result {
                        Ok(data) => {
                            let envelope = QueryEnvelope::success(
                                compiled.sql,
                                &compiled.columns,
                                data,
                                step.query.referenced_views(),
                            );
                            step_results.push(serde_json::json!({
                                "step": step.name,
                                "description": step.description,
                                "result": serde_json::to_value(&envelope).expect("serialize"),
                            }));
                        }
                        Err(e) => {
                            let envelope = QueryEnvelope::error(
                                "execution_error",
                                e.to_string(),
                                Some(compiled.sql),
                                &compiled.columns,
                                step.query.referenced_views(),
                            );
                            step_results.push(serde_json::json!({
                                "step": step.name,
                                "description": step.description,
                                "result": serde_json::to_value(&envelope).expect("serialize"),
                            }));
                        }
                    }
                }
                Err(e) => {
                    let envelope = QueryEnvelope::error(
                        "compile_error",
                        e.to_string(),
                        None,
                        &[],
                        step.query.referenced_views(),
                    );
                    step_results.push(serde_json::json!({
                        "step": step.name,
                        "description": step.description,
                        "result": serde_json::to_value(&envelope).expect("serialize"),
                    }));
                }
            }
        }

        Ok(serde_json::json!({
            "query": name,
            "steps": step_results,
        }))
    }

    match inner(name, globals, config, dialect, datasource) {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output).expect("serialize")
            );
        }
        Err(e) => {
            let envelope = crate::executor::QueryEnvelope::error(
                "parse_error",
                e.to_string(),
                None,
                &[],
                vec![],
            );
            print_envelope(&envelope);
            std::process::exit(1);
        }
    }
}

/// Convert foreign semantic models to airlayer .view.yml format.
fn run_convert(
    format_str: &str,
    input: &Path,
    output: Option<&PathBuf>,
    dialect: Option<&str>,
    to_stdout: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let format = ForeignFormat::from_str(format_str)
        .ok_or_else(|| format!(
            "Unknown format '{}'. Supported: cube, lookml, dbt, omni",
            format_str
        ))?;

    let result = if input.is_dir() {
        crate::schema::foreign::convert_directory(format, input)?
    } else {
        let content = std::fs::read_to_string(input)
            .map_err(|e| format!("Failed to read {}: {}", input.display(), e))?;
        crate::schema::foreign::convert(
            format,
            &content,
            input.to_str().unwrap_or("<unknown>"),
        )?
    };

    // Print warnings
    for warning in &result.warnings {
        eprintln!("warning: {}", warning);
    }

    if result.views.is_empty() {
        return Err("No views were converted.".into());
    }

    let serialize_view = |view: &crate::schema::models::View| -> Result<String, String> {
        let mut yaml_view = serde_yaml::to_value(view)
            .map_err(|e| format!("Failed to serialize view: {}", e))?;
        if let Some(d) = dialect {
            if let serde_yaml::Value::Mapping(ref mut map) = yaml_view {
                map.insert(
                    serde_yaml::Value::String("dialect".to_string()),
                    serde_yaml::Value::String(d.to_string()),
                );
            }
        }
        serde_yaml::to_string(&yaml_view)
            .map_err(|e| format!("Failed to serialize view: {}", e))
    };

    if to_stdout {
        for view in &result.views {
            println!("---");
            print!("{}", serialize_view(view)?);
        }
    } else {
        let output_dir = output.map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(output_dir)
            .map_err(|e| format!("Failed to create output directory: {}", e))?;

        for view in &result.views {
            let filename = format!("{}.view.yml", view.name);
            let filepath = output_dir.join(&filename);
            std::fs::write(&filepath, serialize_view(view)?)
                .map_err(|e| format!("Failed to write {}: {}", filepath.display(), e))?;
            println!("Wrote {}", filepath.display());
        }
    }

    println!(
        "Converted {} views from {} format.",
        result.views.len(),
        format
    );

    Ok(())
}

/// Compile-only path (no --execute). Prints raw SQL to stdout.
fn run_compile(
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
    motif: Option<String>,
    motif_param: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = resolve_project_context(config.as_ref())?;
    let dialects = build_dialect_map(ctx.config_path.as_ref(), dialect.as_deref())?;
    let parser = make_parser(globals.as_ref())?;
    let layer = load_from_directory(&parser, &ctx.base_dir)?;
    let engine = SemanticEngine::from_semantic_layer(layer, dialects)?;

    let request = parse_query_input(
        query,
        dimensions,
        measures,
        filter,
        order,
        limit,
        offset,
        segments,
        through,
        motif,
        motif_param,
    )?;
    let result = engine.compile_query(&request)?;

    println!("{}", result.sql);
    if !result.params.is_empty() {
        eprintln!("-- params: {:?}", result.params);
    }
    Ok(())
}

/// Execute path (--execute). Always outputs a QueryEnvelope as JSON — even on errors.
/// This function never returns Err; all errors are captured in the envelope.
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
fn run_execute(
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
    motif: Option<String>,
    motif_param: Vec<String>,
    datasource: Option<String>,
) {
    use crate::executor::QueryEnvelope;

    /// Inner function returning Result<QueryEnvelope, QueryEnvelope> so we can
    /// use early returns with map_err, keeping the envelope construction in one place.
    #[allow(clippy::too_many_arguments)]
    fn inner(
        globals: Option<&PathBuf>,
        config: Option<&PathBuf>,
        dialect: Option<&str>,
        query: Option<String>,
        dimensions: Vec<String>,
        measures: Vec<String>,
        filter: Vec<String>,
        order: Vec<String>,
        limit: Option<u64>,
        offset: Option<u64>,
        segments: Vec<String>,
        through: Vec<String>,
        motif: Option<String>,
        motif_param: Vec<String>,
        datasource: Option<&str>,
    ) -> Result<QueryEnvelope, QueryEnvelope> {
        let err = |stage, msg: String, sql: Option<String>, columns: &[_], views: Vec<String>| {
            QueryEnvelope::error(stage, msg, sql, columns, views)
        };

        // Stage 1: resolve project context, parse views & build engine
        let ctx = resolve_project_context(config)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let dialects = build_dialect_map(ctx.config_path.as_ref(), dialect)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let parser = make_parser(globals)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let layer = load_from_directory(&parser, &ctx.base_dir)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let engine = SemanticEngine::from_semantic_layer(layer, dialects)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;

        // Stage 2: parse query input
        let request = parse_query_input(
            query,
            dimensions,
            measures,
            filter,
            order,
            limit,
            offset,
            segments,
            through,
            motif,
            motif_param,
        )
        .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let views_used = request.referenced_views();

        // Stage 3: compile query
        let result = engine.compile_query(&request).map_err(|e| {
            err(
                "compile_error",
                e.to_string(),
                None,
                &[],
                views_used.clone(),
            )
        })?;

        // Stage 4: resolve connection & execute
        let config_path = ctx.config_path.as_ref().ok_or_else(|| {
            err(
                "execution_error",
                "--execute requires a config.yml (auto-detected or via --config)".into(),
                Some(result.sql.clone()),
                &result.columns,
                views_used.clone(),
            )
        })?;
        let content = std::fs::read_to_string(config_path).map_err(|e| {
            err(
                "execution_error",
                format!("Failed to read config {}: {}", config_path.display(), e),
                Some(result.sql.clone()),
                &result.columns,
                views_used.clone(),
            )
        })?;
        let exec_config: crate::executor::ExecutionConfig = serde_yaml::from_str(&content)
            .map_err(|e| {
                err(
                    "execution_error",
                    format!("Failed to parse config: {}", e),
                    Some(result.sql.clone()),
                    &result.columns,
                    views_used.clone(),
                )
            })?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)
        } else {
            exec_config.first_connection()
        }
        .map_err(|e| {
            err(
                "execution_error",
                e.to_string(),
                Some(result.sql.clone()),
                &result.columns,
                views_used.clone(),
            )
        })?;

        // Stage 5: execute
        let exec_result = crate::executor::execute(&connection, &result.sql, &result.params)
            .map_err(|e| {
                err(
                    "execution_error",
                    e.to_string(),
                    Some(result.sql.clone()),
                    &result.columns,
                    views_used.clone(),
                )
            })?;

        Ok(QueryEnvelope::success(
            result.sql,
            &result.columns,
            exec_result,
            views_used,
        ))
    }

    let is_error;
    let envelope = match inner(
        globals.as_ref(),
        config.as_ref(),
        dialect.as_deref(),
        query,
        dimensions,
        measures,
        filter,
        order,
        limit,
        offset,
        segments,
        through,
        motif,
        motif_param,
        datasource.as_deref(),
    ) {
        Ok(env) => {
            is_error = false;
            env
        }
        Err(env) => {
            is_error = true;
            env
        }
    };
    print_envelope(&envelope);
    if is_error {
        std::process::exit(1);
    }
}

/// Parse query input from either -q JSON or --dimension/--measure flags.
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
    motif: Option<String>,
    motif_param: Vec<String>,
) -> Result<QueryRequest, Box<dyn std::error::Error>> {
    let has_flags = !dimensions.is_empty() || !measures.is_empty();

    if let Some(q) = query {
        if has_flags {
            return Err("Cannot use both -q/--query and --dimension/--measure flags".into());
        }
        let query_str = if q == "-" {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
            buf
        } else {
            q
        };
        let mut request: QueryRequest =
            serde_json::from_str(&query_str).map_err(|e| format!("Invalid query JSON: {}", e))?;
        // CLI --motif overrides JSON motif
        if motif.is_some() {
            request.motif = motif;
        }
        // CLI --motif-param merges into (and overrides) JSON motif_params
        for mp in &motif_param {
            let (key, value) =
                parse_motif_param(mp).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            request.motif_params.insert(key, value);
        }
        Ok(request)
    } else if has_flags {
        Ok(build_query_from_flags(
            dimensions,
            measures,
            filter,
            order,
            limit,
            offset,
            segments,
            through,
            motif,
            motif_param,
        )?)
    } else {
        Err("Provide either -q/--query (JSON) or --dimension/--measure flags".into())
    }
}

/// Print the airlayer ASCII banner with an animated line-by-line reveal.
fn print_banner() {
    use console::style;
    use std::io::Write;
    use std::time::Duration;

    // Compact geometric wordmark
    let lines = [
        r#"        _      __                    "#,
        r#"  ___ _(_)____/ /__ ___ _____ ____   "#,
        r#" / _ `/ / __/ / _ `/ // / -_) __/   "#,
        r#" \_,_/_/_/ /_/\_,_/\_, /\__/_/      "#,
        r#"                  /___/              "#,
    ];

    let term = console::Term::stderr();

    println!();

    // Animate: reveal each line with a short delay
    for line in &lines {
        let _ = term.write_str(&format!("  {}\n", style(line).cyan()));
        let _ = std::io::stderr().flush();
        std::thread::sleep(Duration::from_millis(40));
    }

    println!();
    println!("  {}", style("  in-process semantic engine").dim());
    println!("  {}", style("─".repeat(40)).dim());
}

/// Initialize an airlayer project directory with config.yml, CLAUDE.md, and Claude Code skills.
/// When stdin is a TTY, runs a discovery-driven interactive flow:
///   credentials → connect → discover databases → select → discover tables → select → generate views.
/// Otherwise, generates a template config.
fn run_init(
    path: Option<&PathBuf>,
    db_type_flag: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    use console::style;

    let target = path.map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));

    // Ensure target directory exists
    if !target.exists() {
        std::fs::create_dir_all(target)?;
    }

    let mut created = Vec::new();
    let mut skipped = Vec::new();

    let config_path = target.join("config.yml");
    let views_dir = target.to_path_buf();
    let is_interactive = std::io::stdin().is_terminal() && !config_path.exists();

    if is_interactive {
        // --- Interactive discovery flow ---
        print_banner();
        println!();

        // Step 1: Select database type
        let db_type = if let Some(t) = db_type_flag {
            if !prompts::DB_TYPES.contains(&t) {
                return Err(format!(
                    "Unknown database type '{}'. Supported: {}",
                    t,
                    prompts::DB_TYPES.join(", ")
                )
                .into());
            }
            println!("  {} {}", style("Database:").bold(), style(t).cyan());
            t.to_string()
        } else {
            prompts::select_database_type()?
        };

        // Step 2: Prompt for ONLY credentials (no database/schema)
        let mut fields = prompts::prompt_credentials(&db_type)?;

        // Step 3: Connect and run discovery
        println!();
        run_init_discovery(&db_type, &mut fields, &views_dir)?;

        // Step 4: Write config.yml with discovered database included
        let config_content = prompts::generate_config_yml(&db_type, &fields);
        std::fs::write(&config_path, &config_content)?;
        created.push("config.yml".to_string());

        // View files are already printed during discovery — don't duplicate in summary

        // views/ directory is optional — view files can live in the project root
    } else {
        // --- Non-interactive flow ---
        if !config_path.exists() {
            let config_content = if let Some(t) = db_type_flag {
                prompts::config_template_for_type(t).unwrap_or_else(|| INIT_CONFIG_YML.to_string())
            } else {
                INIT_CONFIG_YML.to_string()
            };
            std::fs::write(&config_path, &config_content)?;
            created.push("config.yml".to_string());
        } else {
            skipped.push("config.yml".to_string());
        }

        // views/ directory is optional — view files can live in the project root
    }

    // Always write CLAUDE.md and skills
    let claude_md_path = target.join("CLAUDE.md");
    write_if_absent(&claude_md_path, INIT_CLAUDE_MD, &mut created, &mut skipped)?;
    install_agents_and_skills(target, &mut created, &mut skipped)?;

    // Print summary
    println!();
    println!("  {}", style("─".repeat(40)).dim());
    if !created.is_empty() {
        println!();
        for f in &created {
            let desc = file_description(f);
            if desc.is_empty() {
                println!("  {} {}", style("+").green(), style(f).white());
            } else {
                println!(
                    "  {} {}  {}",
                    style("+").green(),
                    style(f).white(),
                    style(desc).dim()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(40));
        }
    }
    if !skipped.is_empty() {
        println!();
        for f in &skipped {
            let desc = file_description(f);
            if desc.is_empty() {
                println!(
                    "  {} {} {}",
                    style("-").dim(),
                    style(f).dim(),
                    style("(exists)").dim()
                );
            } else {
                println!(
                    "  {} {} {}  {}",
                    style("-").dim(),
                    style(f).dim(),
                    style("(exists)").dim(),
                    style(desc).dim()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(40));
        }
    }

    // Offer AI enrichment if views were generated and an AI CLI tool is available
    let has_views = views_dir.is_dir()
        && std::fs::read_dir(&views_dir)
            .map(|entries| {
                entries.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with(".view.yml"))
                })
            })
            .unwrap_or(false);
    if is_interactive && has_views {
        if let Some(tool) = prompts::detect_ai_tool() {
            println!();
            if prompts::prompt_enrichment(tool)? {
                run_ai_enrichment(tool, target)?;
            }
        }
    }

    println!();
    if !is_interactive {
        println!("  {}", style("Next steps:").bold());
        println!(
            "  {}  Edit {} with your database connection",
            style("1.").dim(),
            style("config.yml").bold()
        );
        println!(
            "  {}  Run {} to discover tables",
            style("2.").dim(),
            style("airlayer inspect --schema --config config.yml").cyan()
        );
        println!(
            "  {}  Or use Claude Code: {} to bootstrap, {} to query",
            style("3.").dim(),
            style("/builder").cyan(),
            style("/analyst").cyan()
        );
    } else {
        println!(
            "  {} Use {} to query or {} to customize views.",
            style("Ready!").green().bold(),
            style("@analyst").cyan(),
            style("@builder").cyan()
        );
    }
    println!();

    Ok(())
}

/// Discovery-driven init: connect → list databases → select → discover tables → select → generate views.
/// Mutates `fields` to add the selected database.
#[allow(unused_variables)]
fn run_init_discovery(
    db_type: &str,
    fields: &mut std::collections::BTreeMap<String, String>,
    views_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use console::style;

    #[cfg(feature = "exec")]
    {
        use indicatif::{ProgressBar, ProgressStyle};
        use std::time::Duration;

        let make_spinner = |msg: &str| -> ProgressBar {
            let spinner = ProgressBar::new_spinner();
            spinner.set_style(
                ProgressStyle::with_template("  {spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_strings(&[
                        "   ", ".  ", ".. ", "...", " ..", "  .", "   ", ".  ", ".. ", "...",
                    ]),
            );
            spinner.set_message(msg.to_string());
            spinner.enable_steady_tick(Duration::from_millis(120));
            spinner
        };

        // Build connection and test — retry with re-prompted credentials on failure
        let connection = loop {
            let conn = match crate::executor::build_connection_from_fields(db_type, fields) {
                Ok(c) => c,
                Err(e) => {
                    println!(
                        "  {} {}",
                        style("x").red().bold(),
                        style(format!("{}", e)).red()
                    );
                    println!();
                    *fields = prompts::reprompt_credentials(db_type, fields)?;
                    println!();
                    continue;
                }
            };

            let spinner = make_spinner("Connecting...");
            match crate::executor::execute(&conn, "SELECT 1", &[]) {
                Ok(_) => {
                    spinner.finish_and_clear();
                    println!("  {} Connected", style("~").green());
                    println!();
                    break conn;
                }
                Err(e) => {
                    spinner.finish_and_clear();
                    println!(
                        "  {} {}",
                        style("x").red().bold(),
                        style(format!("Connection failed: {}", e)).red()
                    );
                    println!();
                    *fields = prompts::reprompt_credentials(db_type, fields)?;
                    println!();
                }
            }
        };

        // Discover databases and tables.
        // "← Back" from table selection loops back to database selection.
        let mut connection = connection;
        let databases: Vec<String> = if prompts::supports_database_listing(db_type) {
            let spinner = make_spinner("Discovering databases...");
            match crate::executor::introspect::list_databases(&connection) {
                Ok(dbs) => {
                    spinner.finish_and_clear();
                    dbs
                }
                Err(e) => {
                    spinner.finish_and_clear();
                    println!(
                        "  {} Could not list databases: {}",
                        style("~").yellow(),
                        style(format!("{}", e)).dim()
                    );
                    vec![]
                }
            }
        } else {
            vec![]
        };

        'db_select: loop {
            // Database selection (if applicable)
            if !databases.is_empty() {
                let db_label = if db_type == "bigquery" {
                    "dataset"
                } else {
                    "database"
                };
                if let Some(selected) = prompts::prompt_database_selection(&databases, db_label)? {
                    let field_name = if db_type == "bigquery" {
                        "dataset"
                    } else {
                        "database"
                    };
                    fields.insert(field_name.to_string(), selected);
                    connection = crate::executor::build_connection_from_fields(db_type, fields)?;
                }
            }

            // Discover tables
            let spinner = make_spinner("Discovering tables...");
            let schema_info = match crate::executor::introspect::introspect(&connection) {
                Ok(info) => info,
                Err(e) => {
                    spinner.finish_and_clear();
                    println!(
                        "  {} {}",
                        style("x").red().bold(),
                        style(format!("Schema discovery failed: {}", e)).red()
                    );
                    return Ok(());
                }
            };
            let user_tables = bootstrap::filter_user_tables(&schema_info);
            spinner.finish_and_clear();

            if user_tables.is_empty() {
                println!("  {} No tables found", style("~").yellow());
                return Ok(());
            }

            // Build labels for multi-select
            let table_labels: Vec<String> = user_tables
                .iter()
                .map(|t| {
                    let prefix = t
                        .schema
                        .as_deref()
                        .map(|s| format!("{}.", s))
                        .unwrap_or_default();
                    format!("{}{} ({} cols)", prefix, t.name, t.columns.len())
                })
                .collect();

            println!();
            println!(
                "  {} Found {} tables:",
                style("~").green(),
                style(user_tables.len()).bold()
            );
            println!();

            // Let user select which tables to model
            match prompts::prompt_table_selection(&table_labels)? {
                prompts::TableSelection::Back => {
                    if databases.is_empty() {
                        // Nothing to go back to
                        println!("  {} No tables selected", style("~").yellow());
                        return Ok(());
                    }
                    println!();
                    continue 'db_select;
                }
                prompts::TableSelection::Selected(selected_indices) => {
                    if selected_indices.is_empty() {
                        println!("  {} No tables selected", style("~").yellow());
                        return Ok(());
                    }

                    let selected_tables: Vec<&crate::executor::introspect::TableInfo> =
                        selected_indices.iter().map(|&i| user_tables[i]).collect();

                    let datasource_name = fields
                        .get("name")
                        .map(|s| s.as_str())
                        .unwrap_or("warehouse");
                    let dialect = bootstrap::dialect_for_db_type(db_type);

                    std::fs::create_dir_all(views_dir)?;
                    let view_files = bootstrap::bootstrap_views(
                        &selected_tables,
                        datasource_name,
                        dialect,
                        views_dir,
                    )?;

                    println!();
                    let delay = if view_files.len() <= 100 { 40 } else { 0 };
                    for f in &view_files {
                        println!("  {} {}", style("+").green(), style(f).white());
                        if delay > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(delay));
                        }
                    }

                    return Ok(());
                }
            }
        }
    }

    #[cfg(not(feature = "exec"))]
    {
        println!("  {} Discovery requires exec features", style("~").yellow());
        Ok(())
    }
}

/// Launch a non-interactive AI CLI session to enrich generated view files.
/// Parses stream-json output to show progress messages with a spinner while waiting.
fn run_ai_enrichment(
    tool: prompts::AiTool,
    target: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use console::style;
    use indicatif::{ProgressBar, ProgressStyle};
    use std::io::BufRead;
    use std::os::unix::process::CommandExt;
    use std::time::Duration;

    let prompt = "Review and improve the generated .view.yml files using @builder.";

    // Count total .view.yml files to estimate progress
    let mut total_views = std::fs::read_dir(target)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with(".view.yml"))
                })
                .count()
        })
        .unwrap_or(0);

    let cmd_name = match tool {
        prompts::AiTool::Claude => "claude",
        prompts::AiTool::Codex => "codex",
    };

    let mut cmd = std::process::Command::new(cmd_name);
    // Put child in its own process group so it doesn't receive our SIGINT.
    // When ctrl+c kills us, the pipe breaks and the child gets SIGPIPE.
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    cmd.arg("-p")
        .arg(prompt)
        .arg("--output-format")
        .arg("stream-json")
        .arg("--verbose")
        .arg("--dangerously-skip-permissions")
        .arg("--max-budget-usd")
        .arg("5")
        .current_dir(target)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            println!(
                "  {} Could not launch {}: {}",
                style("~").yellow(),
                tool.display_name(),
                style(e).dim()
            );
            return Ok(());
        }
    };

    let stdout = child.stdout.take().unwrap();
    let reader = std::io::BufReader::new(stdout);

    let start_time = std::time::Instant::now();

    // Show a spinner while waiting for progress
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("  {spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&[
                "   ", ".  ", ".. ", "...", " ..", "  .", "   ", ".  ", ".. ", "...",
            ]),
    );
    spinner.set_message(format!("{} views...", style("Enriching").color256(208)));
    spinner.enable_steady_tick(Duration::from_millis(120));

    // Track which files have been announced (in order)
    let mut enriched_files: Vec<String> = Vec::new();
    let mut seen_files: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut current_file: Option<String> = None;
    let mut current_verb: &str = "Enriching";
    let mut got_result = false;

    const VERBS: &[&str] = &[
        "Enriching",
        "Improving",
        "Refining",
        "Polishing",
        "Enhancing",
        "Tuning",
        "Sharpening",
    ];

    fn pick_verb<'a>(verbs: &'a [&'a str], exclude: &str, counter: usize) -> &'a str {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Mix in a counter so rapid calls don't collide
        counter.hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut hasher);
        let candidates: Vec<&&str> = verbs.iter().filter(|v| **v != exclude).collect();
        if candidates.is_empty() {
            return verbs[0];
        }
        candidates[(hasher.finish() as usize) % candidates.len()]
    }

    /// Format seconds as "Xs" or "Xm Ys".
    fn fmt_duration(secs: u64) -> String {
        if secs < 60 {
            format!("{}s", secs)
        } else {
            format!("{}m {}s", secs / 60, secs % 60)
        }
    }

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        // Update elapsed time on each event
        let elapsed = start_time.elapsed().as_secs();
        let done = enriched_files.len();
        if let Some(ref cur) = current_file {
            let eta = if done > 0 && total_views > done {
                let avg = elapsed as f64 / done as f64;
                let remaining = ((total_views - done) as f64 * avg) as u64;
                format!(", ~{} remaining", fmt_duration(remaining))
            } else {
                String::new()
            };
            spinner.set_message(format!(
                "{} {} {}",
                style(current_verb).color256(208),
                cur,
                style(format!(
                    "({}/{}) {} elapsed{}",
                    done,
                    total_views,
                    fmt_duration(elapsed),
                    eta
                ))
                .dim()
            ));
        } else {
            let time_str = fmt_duration(elapsed);
            if total_views > 0 {
                spinner.set_message(format!(
                    "{} views... ({} views) {} elapsed",
                    style(current_verb).color256(208),
                    total_views,
                    time_str
                ));
            } else {
                spinner.set_message(format!(
                    "{} views... {} elapsed",
                    style(current_verb).color256(208),
                    time_str
                ));
            }
        }

        let event: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");

        match event_type {
            "assistant" => {
                // Look for tool_use blocks (file edits)
                if let Some(content) = event.pointer("/message/content").and_then(|c| c.as_array())
                {
                    for block in content {
                        let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        if block_type == "tool_use" {
                            let tool_name =
                                block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            if matches!(tool_name, "Edit" | "Write") {
                                if let Some(path) =
                                    block.pointer("/input/file_path").and_then(|p| p.as_str())
                                {
                                    let filename = std::path::Path::new(path)
                                        .file_name()
                                        .and_then(|f| f.to_str())
                                        .unwrap_or(path);
                                    if filename.ends_with(".view.yml")
                                        && seen_files.insert(filename.to_string())
                                    {
                                        enriched_files.push(filename.to_string());
                                        let done = enriched_files.len();
                                        let elapsed = start_time.elapsed().as_secs();
                                        // Mark previous file as done
                                        if let Some(prev_f) = current_file.take() {
                                            spinner.println(format!(
                                                "  {} {}",
                                                style("✓").green(),
                                                style(&prev_f).white()
                                            ));
                                        }
                                        current_file = Some(filename.to_string());
                                        current_verb = pick_verb(VERBS, current_verb, done);
                                        // Adjust total if Claude creates more files than expected
                                        if done > total_views {
                                            total_views = done;
                                        }
                                        let eta = if done > 0 && total_views > done {
                                            let avg = elapsed as f64 / done as f64;
                                            let remaining =
                                                ((total_views - done) as f64 * avg) as u64;
                                            format!(", ~{} remaining", fmt_duration(remaining))
                                        } else {
                                            String::new()
                                        };
                                        spinner.set_message(format!(
                                            "{} {} {}",
                                            style(current_verb).color256(208),
                                            filename,
                                            style(format!(
                                                "({}/{}) {} elapsed{}",
                                                done,
                                                total_views,
                                                fmt_duration(elapsed),
                                                eta
                                            ))
                                            .dim()
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "result" => {
                got_result = true;
                spinner.finish_and_clear();
                // Mark the last file as done
                if let Some(prev_f) = current_file.take() {
                    println!("  {} {}", style("✓").green(), style(prev_f).white());
                }
                let elapsed = start_time.elapsed().as_secs();
                let is_error = event
                    .get("is_error")
                    .and_then(|e| e.as_bool())
                    .unwrap_or(false);
                if is_error {
                    let msg = event
                        .get("result")
                        .and_then(|r| r.as_str())
                        .unwrap_or("Unknown error");
                    println!(
                        "  {} Enrichment failed: {}",
                        style("x").red().bold(),
                        style(msg).red()
                    );
                } else {
                    println!();
                    println!(
                        "  {}",
                        style(format!("Views enriched in {}", fmt_duration(elapsed))).green()
                    );
                }
                break;
            }
            _ => {}
        }
    }

    spinner.finish_and_clear();
    let _ = child.wait();

    if !got_result {
        println!("  {} Enrichment session ended", style("~").yellow());
    }

    Ok(())
}

/// Test a database connection from a config file. Returns true if successful.
fn test_connection_from_config(config_path: &Path, datasource: Option<&str>) -> bool {
    use console::style;

    #[cfg(feature = "exec")]
    {
        use indicatif::{ProgressBar, ProgressStyle};
        use std::time::Duration;

        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::with_template("  {spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&[
                    "   ", ".  ", ".. ", "...", " ..", "  .", "   ", ".  ", ".. ", "...",
                ]),
        );
        spinner.set_message("Connecting...");
        spinner.enable_steady_tick(Duration::from_millis(120));

        let content = match std::fs::read_to_string(config_path) {
            Ok(c) => c,
            Err(e) => {
                spinner.finish_and_clear();
                println!(
                    "  {} {}",
                    style("x").red().bold(),
                    style(format!("Failed to read config: {}", e)).red()
                );
                return false;
            }
        };
        let exec_config: crate::executor::ExecutionConfig = match serde_yaml::from_str(&content) {
            Ok(c) => c,
            Err(e) => {
                spinner.finish_and_clear();
                println!(
                    "  {} {}",
                    style("x").red().bold(),
                    style(format!("Failed to parse config: {}", e)).red()
                );
                return false;
            }
        };
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)
        } else {
            exec_config.first_connection()
        };
        let connection = match connection {
            Ok(c) => c,
            Err(e) => {
                spinner.finish_and_clear();
                println!(
                    "  {} {}",
                    style("x").red().bold(),
                    style(format!("{}", e)).red()
                );
                return false;
            }
        };
        match crate::executor::execute(&connection, "SELECT 1", &[]) {
            Ok(_) => {
                spinner.finish_and_clear();
                println!("  {} Connected", style("~").green());
                true
            }
            Err(e) => {
                spinner.finish_and_clear();
                println!(
                    "  {} {}",
                    style("x").red().bold(),
                    style(format!("Connection failed: {}", e)).red()
                );
                println!();
                println!(
                    "  {} Fix {} and run {}",
                    style("hint:").yellow(),
                    style("config.yml").bold(),
                    style("airlayer test-connection").cyan()
                );
                false
            }
        }
    }
    #[cfg(not(feature = "exec"))]
    {
        let _ = (config_path, datasource);
        println!(
            "  {} Connection testing requires exec features",
            style("~").yellow()
        );
        false
    }
}

/// Test the database connection.
fn run_test_connection(
    config: Option<&PathBuf>,
    datasource: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = resolve_project_context(config)?;
    let config_path = ctx
        .config_path
        .ok_or("No config.yml found (auto-detected or via --config)")?;

    if !config_path.exists() {
        return Err(format!("Config file not found: {}", config_path.display()).into());
    }
    let config_path = config_path.as_path();

    println!();
    if test_connection_from_config(config_path, datasource) {
        println!();
        Ok(())
    } else {
        println!();
        std::process::exit(1);
    }
}

/// Update CLAUDE.md and Claude Code skills to the latest bundled version.
fn run_update(path: Option<&PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let target = path.map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));

    if !target.exists() {
        return Err(format!("Directory does not exist: {}", target.display()).into());
    }

    let mut updated = Vec::new();
    let mut unchanged = Vec::new();

    // 1. CLAUDE.md
    let claude_md_path = target.join("CLAUDE.md");
    write_or_update(
        &claude_md_path,
        INIT_CLAUDE_MD,
        &mut updated,
        &mut unchanged,
    )?;

    // 2. Claude Code skills (agents + low-level tools)
    install_agents_and_skills(target, &mut updated, &mut unchanged)?;

    if !updated.is_empty() {
        println!("Updated:");
        for f in &updated {
            println!("  {}", f);
        }
    }
    if !unchanged.is_empty() {
        println!("Already up to date:");
        for f in &unchanged {
            println!("  {}", f);
        }
    }
    if updated.is_empty() {
        println!("\nEverything is already up to date.");
    }

    Ok(())
}

/// Install Claude Code sub-agents and skills into the target directory.
fn install_agents_and_skills(
    target: &Path,
    created: &mut Vec<String>,
    skipped: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sub-agents (.claude/agents/*.md) — run in isolated context with restricted tools
    let agents: &[(&str, &str)] = &[
        ("analyst", include_str!("../../.claude/agents/analyst.md")),
        ("builder", include_str!("../../.claude/agents/builder.md")),
    ];

    let agents_dir = target.join(".claude").join("agents");
    std::fs::create_dir_all(&agents_dir)?;
    for (name, content) in agents {
        let agent_path = agents_dir.join(format!("{}.md", name));
        write_or_update(&agent_path, content, created, skipped)?;
    }

    // Skills (.claude/skills/*/SKILL.md) — preloaded into agents, also usable directly
    let skills: &[(&str, &str)] = &[
        (
            "bootstrap",
            include_str!("../../.claude/skills/bootstrap/SKILL.md"),
        ),
        (
            "profile",
            include_str!("../../.claude/skills/profile/SKILL.md"),
        ),
        ("query", include_str!("../../.claude/skills/query/SKILL.md")),
        (
            "migrate-from-cube",
            include_str!("../../.claude/skills/migrate-from-cube/SKILL.md"),
        ),
    ];

    for (name, content) in skills {
        let skill_dir = target.join(".claude").join("skills").join(name);
        std::fs::create_dir_all(&skill_dir)?;
        let skill_path = skill_dir.join("SKILL.md");
        write_or_update(&skill_path, content, created, skipped)?;
    }

    Ok(())
}

/// Short description of a generated file, shown in dim text after the filename.
fn file_description(path: &str) -> &'static str {
    // Strip any " (updated)" suffix for matching
    let base = path.strip_suffix(" (updated)").unwrap_or(path);
    if base == "config.yml" {
        "database connection"
    } else if base.ends_with("CLAUDE.md") {
        "project instructions for Claude Code"
    } else if base.ends_with("agents/analyst.md") {
        "answers data questions via queries"
    } else if base.ends_with("agents/builder.md") {
        "creates and edits view definitions"
    } else if base.ends_with("skills/bootstrap/SKILL.md") {
        "discover schema and generate views"
    } else if base.ends_with("skills/profile/SKILL.md") {
        "profile dimensions and data values"
    } else if base.ends_with("skills/query/SKILL.md") {
        "run semantic queries"
    } else if base.ends_with("skills/migrate-from-cube/SKILL.md") {
        "migrate Cube.js schema to airlayer views"
    } else {
        ""
    }
}

/// Write a file only if it doesn't already exist.
fn write_if_absent(
    path: &Path,
    content: &str,
    created: &mut Vec<String>,
    skipped: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        skipped.push(path.display().to_string());
    } else {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, content)?;
        created.push(path.display().to_string());
    }
    Ok(())
}

/// Write a file, overwriting if it already exists (for skills that should be updated).
fn write_or_update(
    path: &Path,
    content: &str,
    created: &mut Vec<String>,
    _skipped: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let label = path.display().to_string();
    if path.exists() {
        let existing = std::fs::read_to_string(path)?;
        if existing == content {
            _skipped.push(label);
        } else {
            std::fs::write(path, content)?;
            created.push(format!("{} (updated)", label));
        }
    } else {
        std::fs::write(path, content)?;
        created.push(label);
    }
    Ok(())
}

const INIT_CONFIG_YML: &str = "\
# airlayer database configuration
# Uncomment and fill in the section for your database.
# See: https://github.com/oxy-hq/airlayer/blob/main/docs/agent-execution.md

databases: []

# databases:
#   - name: warehouse
#     type: postgres
#     host: localhost
#     port: 5432
#     database: mydb
#     user: myuser
#     password_var: PG_PASSWORD    # reads from environment variable
#
#   - name: warehouse
#     type: snowflake
#     account: myaccount
#     user: myuser
#     password_var: SNOWFLAKE_PASSWORD
#     warehouse: COMPUTE_WH
#     database: MYDB
#     schema: PUBLIC
#
#   - name: warehouse
#     type: bigquery
#     project: my-gcp-project
#     dataset: analytics
#     access_token_var: BIGQUERY_ACCESS_TOKEN
#
#   - name: warehouse
#     type: duckdb
#     path: ./data/analytics.duckdb
#
#   - name: warehouse
#     type: motherduck
#     token_var: MOTHERDUCK_TOKEN
#     database: my_db
";

const INIT_CLAUDE_MD: &str = "\
# airlayer project

This project uses [airlayer](https://github.com/oxy-hq/airlayer) as its semantic layer.

## Structure

```
config.yml              Database connection configuration
*.view.yml              Semantic layer definitions (can also go in views/)
*.motif.yml             Custom analytical patterns (optional, can also go in motifs/)
*.query.yml             Saved queries (optional, can also go in queries/)
```

## Sub-agents

This project has two Claude Code sub-agents (in `.claude/agents/`):

- **`analyst`** — Answers data questions by querying through the semantic layer. Has read-only tools (Read, Glob, Grep, Bash). Uses motifs for contribution analysis, rankings, anomaly detection, period-over-period comparisons, and more. Never modifies files.
- **`builder`** — Creates and modifies `.view.yml` files. Has full tools (Read, Edit, Write, Glob, Grep, Bash). Bootstraps from database schema, adds dimensions/measures, sets up joins, validates, and profiles. Never answers data questions directly.

Claude will automatically delegate to the right sub-agent based on the user's request. Users can also invoke them explicitly with `@analyst` or `@builder`.

### Skills (preloaded into sub-agents, also usable directly)

- `/bootstrap` — Discover database schema and generate .view.yml files
- `/profile` — Profile dimensions to validate data values and ranges
- `/query` — Run semantic queries against the database
- `/migrate-from-cube` — Convert Cube.js schema files to .view.yml

**Do NOT run `airlayer init` or `airlayer update`** — those are user-facing CLI commands. By the time you are reading this, init has already been run. To update agents and skills, the user runs `airlayer update`.

## Workflow

1. Edit `config.yml` with database connection details
2. `/builder` to bootstrap views from your schema, then profile and validate
3. `/analyst` to answer questions using the semantic layer
4. Back to `/builder` if the analyst needs a missing dimension or measure

## Important: no raw SQL

airlayer does NOT support raw SQL queries. There is no `--raw-sql` flag. All queries go through the semantic layer using `--dimension`, `--measure`, and `--filter` flags (or `-q` with JSON). If you need data that isn't covered by existing views, use `/builder` to create or edit a `.view.yml` file first.

## Key concepts

- **Project root auto-detection**: All commands walk up from cwd looking for `config.yml`. No need to pass `--config` from inside a project.
- **Views** define dimensions (group-by columns) and measures (aggregations)
- **Entities** declare join keys — airlayer auto-generates JOINs when queries span views
- **Datasource** in each view maps to a database `name` in config.yml
- **Motifs** are reusable post-aggregation analytical patterns (yoy, anomaly, contribution, etc.)
- **Saved queries** (`.query.yml` files in `queries/`) define reusable single or multi-step queries — run by filepath: `airlayer query queries/revenue.query.yml`
- All views in a single query must use the same SQL dialect

## Motifs

Motifs are reusable post-aggregation analytical patterns. They wrap a base query as a CTE and add window-function columns. Use `--motif <name>` on the CLI or `\"motif\": \"<name>\"` in JSON queries.

**Builtin motifs (12):**

| Motif | Output columns | Description |
|-------|---------------|-------------|
| `contribution` | `total`, `share` | Each row's share of the total (e.g., \"what % does each region contribute?\") |
| `rank` | `rank` | Rank rows by measure descending (e.g., \"top 10 products by revenue\") |
| `percent_of_total` | `percent_of_total` | 100 * measure / total (similar to contribution but as a percentage) |
| `anomaly` | `mean_value`, `stddev_value`, `z_score`, `is_anomaly` | Z-score anomaly detection (flag outliers). Default threshold: 2 |
| `yoy` | `previous_value`, `growth_rate` | Year-over-year — use with `granularity: year` |
| `qoq` | `previous_value`, `growth_rate` | Quarter-over-quarter — use with `granularity: quarter` |
| `mom` | `previous_value`, `growth_rate` | Month-over-month — use with `granularity: month` |
| `wow` | `previous_value`, `growth_rate` | Week-over-week — use with `granularity: week` |
| `dod` | `previous_value`, `growth_rate` | Day-over-day — use with `granularity: day` |
| `trend` | `row_n`, `slope`, `intercept`, `trend_value` | Linear regression trend line (requires time dimension) |
| `moving_average` | `moving_avg` | Rolling average over a sliding window (requires time dimension). Default: 7-period |
| `cumulative` | `cumulative_value` | Running sum over time (requires time dimension) |

**Important:** Period-over-period motifs (yoy/qoq/mom/wow/dod) use `LAG(1)` — the granularity MUST match the period. For example, `yoy` requires `granularity: year`, `mom` requires `granularity: month`. Using the wrong granularity gives incorrect comparisons.

**Motif params:** Motif params control which measure/dimension a motif operates on. With a single measure, `{{ measure }}` auto-binds. With multiple measures, you MUST specify which one explicitly.

Pass params via `--motif-param key=value` on the CLI or `\"motif_params\"` in JSON queries. **Values for measure/dimension params are semantic member names** (e.g., `orders.total_revenue`), not SQL aliases.

- `anomaly`: `threshold` — z-score threshold (default: 2)
- `moving_average`: `window` — periods preceding (default: 6 meaning 7-period window)

**Custom motifs** can be defined as `.motif.yml` files in a `motifs/` directory. Custom motifs can declare multiple `type: measure` params for different roles (e.g., numerator and denominator). Important: motif expressions run in an outer SELECT over already-aggregated data (wrapped as a CTE), so cross-row computations MUST use `OVER()` window functions — plain `MIN(x)` would collapse all rows, but `MIN(x) OVER ()` computes the global min while keeping every row. Row-level math like `{{ measure }} * 2` doesn't need OVER.

**Examples:**
```bash
# Single measure — auto-binds, no motif-param needed
airlayer query -x \\
  --dimension orders.category \\
  --measure orders.total_revenue \\
  --motif contribution

# Multiple measures — must specify which one the motif operates on
airlayer query -x \\
  --dimension orders.category \\
  --measure orders.total_revenue --measure orders.order_count \\
  --motif rank --motif-param measure=orders.total_revenue

# Period-over-period (granularity must match motif)
airlayer query -x -q '{
  \"measures\": [\"orders.total_revenue\"],
  \"time_dimensions\": [{\"dimension\": \"orders.created_at\", \"granularity\": \"month\"}],
  \"motif\": \"mom\"
}'

# Anomaly detection with custom threshold
airlayer query -x -q '{
  \"measures\": [\"orders.total_revenue\"],
  \"time_dimensions\": [{\"dimension\": \"orders.created_at\", \"granularity\": \"month\"}],
  \"motif\": \"anomaly\",
  \"motif_params\": {\"threshold\": 3}
}'

# Custom motif with two measure params
airlayer query -x -q '{
  \"measures\": [\"orders.total_revenue\", \"orders.order_count\"],
  \"motif\": \"ratio\",
  \"motif_params\": {\"numerator\": \"orders.total_revenue\", \"denominator\": \"orders.order_count\"}
}'
```

## Saved queries

Saved queries are reusable query definitions stored as `.query.yml` files in a `queries/` directory. They can be single-step (inline query fields) or multi-step (a `steps` array grouping related queries for a specific analytical task).

**Single-step** (inline fields):
```yaml
name: revenue_by_region
description: \"Revenue contribution by region\"
measures: [\"orders.total_revenue\"]
dimensions: [\"orders.region\"]
motif: contribution
```

**Multi-step** (`steps` array):
```yaml
name: revenue_investigation
description: \"Investigate revenue trends and anomalies\"
steps:
  - name: overall_trend
    description: \"Identify the overall revenue trend\"
    query:
      measures: [\"orders.total_revenue\"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: trend
  - name: anomaly_check
    description: \"Flag anomalous months\"
    query:
      measures: [\"orders.total_revenue\"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: anomaly
```

**Running saved queries (by filepath):**
```bash
# Compile to SQL (dry run)
airlayer query queries/revenue_investigation.query.yml

# Execute against the database
airlayer query queries/revenue_investigation.query.yml -x
```

## Discovery

Use `inspect` to discover available views, motifs, and saved queries. All commands auto-detect the project root (walks up from cwd looking for `config.yml`), so `--config` is usually not needed.

```bash
# List all views, dimensions, measures
airlayer inspect

# List all motifs (builtins + custom) with params and outputs
airlayer inspect --motifs

# List saved queries with steps
airlayer inspect --queries

# Machine-readable JSON (works with --motifs and --queries too)
airlayer inspect --json
airlayer inspect --motifs --json
airlayer inspect --queries --json
```
";
