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

        /// Motif to apply as post-aggregation transform (e.g., yoy, mom, anomaly, contribution).
        #[arg(long)]
        motif: Option<String>,

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

    /// Initialize an airlayer project with config.yml, CLAUDE.md, and Claude Code skills.
    Init {
        /// Target directory to initialize. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// Update CLAUDE.md and Claude Code skills to the latest version.
    Update {
        /// Target directory to update. Defaults to current directory.
        #[arg(long)]
        path: Option<PathBuf>,
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
    motif: Option<String>,
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
        motif,
        motif_params: std::collections::HashMap::new(),
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
    let motifs_dir = base_dir.join("motifs");
    let sequences_dir = base_dir.join("sequences");

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

    let motifs = if motifs_dir.is_dir() {
        let m = parser.parse_motifs(&motifs_dir)?;
        if m.is_empty() { None } else { Some(m) }
    } else {
        None
    };

    let sequences = if sequences_dir.is_dir() {
        let s = parser.parse_sequences(&sequences_dir)?;
        if s.is_empty() { None } else { Some(s) }
    } else {
        None
    };

    Ok(SemanticLayer::with_motifs_and_sequences(all_views, topics, motifs, sequences))
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
            motif,
            execute,
            datasource,
        } => {
            // When --execute is set, ALL output goes through the envelope.
            // Errors at any stage produce an envelope with the appropriate status.
            if execute {
                run_execute(
                    path, globals, config, dialect, query, dimensions, measures,
                    filter, order, limit, offset, segments, through, motif, datasource,
                );
            } else {
                run_compile(
                    path, globals, config, dialect, query, dimensions, measures,
                    filter, order, limit, offset, segments, through, motif,
                )?;
            }
        }

        Commands::Init { path } => {
            run_init(path.as_ref())?;
        }

        Commands::Update { path } => {
            run_update(path.as_ref())?;
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
            profile,
            config,
            dialect,
            datasource,
            schema,
        } => {
            // --- Schema introspection mode ---
            if let Some(ref schema_filter) = schema {
                run_schema_introspect(config.as_ref(), datasource.as_deref(), schema_filter.as_deref())?;
                return Ok(());
            }

            let base_dir = resolve_base_dir(path.as_ref())?;
            let parser = make_parser(globals.as_ref())?;
            let layer = load_from_directory(&parser, &base_dir)?;

            // --- Profile mode ---
            if let Some(ref profile_target) = profile {
                run_profile(&layer, profile_target, config.as_ref(), dialect.as_deref(), datasource.as_deref())?;
                return Ok(());
            }

            // --- Normal inspect mode ---
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

/// Profile mode: run type-aware data profiling for one or all dimensions in a view.
/// Outputs structured JSON to stdout.
fn run_profile(
    layer: &SemanticLayer,
    target: &str,
    config: Option<&PathBuf>,
    dialect: Option<&str>,
    datasource: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::engine::profiler;

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
    let config_path = config.ok_or("--profile requires --config with database connection details")?;
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;

    #[cfg(feature = "exec")]
    {
        let exec_config: crate::executor::ExecutionConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse config: {}", e))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)?
        } else {
            exec_config.first_connection()?
        };

        // Determine which dimensions to profile
        let dims_to_profile: Vec<&crate::schema::models::Dimension> = if let Some(name) = dim_name {
            let d = view.dimensions.iter().find(|d| d.name == name)
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
            let stats_row = stats_result.rows.first()
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

        println!("{}", serde_json::to_string_pretty(&output).expect("format profile"));
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
    let config_path = config.ok_or("--schema requires --config with database connection details")?;
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config {}: {}", config_path.display(), e))?;

    #[cfg(feature = "exec")]
    {
        let exec_config: crate::executor::ExecutionConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse config: {}", e))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)?
        } else {
            exec_config.first_connection()?
        };

        let mut schema_info = crate::executor::introspect::introspect(&connection)?;

        // Apply optional schema/dataset filter
        if let Some(filter) = schema_filter {
            schema_info.tables.retain(|t| {
                t.schema.as_deref() == Some(filter)
            });
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
    motif: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = resolve_base_dir(path.as_ref())?;
    let dialects = build_dialect_map(config.as_ref(), dialect.as_deref())?;
    let parser = make_parser(globals.as_ref())?;
    let layer = load_from_directory(&parser, &base_dir)?;
    let engine = SemanticEngine::from_semantic_layer(layer, dialects)?;

    let request = parse_query_input(query, dimensions, measures, filter, order, limit, offset, segments, through, motif)?;
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
    motif: Option<String>,
    datasource: Option<String>,
) {
    use crate::executor::QueryEnvelope;

    /// Inner function returning Result<QueryEnvelope, QueryEnvelope> so we can
    /// use early returns with map_err, keeping the envelope construction in one place.
    #[allow(clippy::too_many_arguments)]
    fn inner(
        path: Option<&PathBuf>,
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
        datasource: Option<&str>,
    ) -> Result<QueryEnvelope, QueryEnvelope> {
        let err = |stage, msg: String, sql: Option<String>, columns: &[_], views: Vec<String>|
            QueryEnvelope::error(stage, msg, sql, columns, views);

        // Stage 1: parse views & build engine
        let base_dir = resolve_base_dir(path)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let dialects = build_dialect_map(config, dialect)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let parser = make_parser(globals)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let layer = load_from_directory(&parser, &base_dir)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let engine = SemanticEngine::from_semantic_layer(layer, dialects)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;

        // Stage 2: parse query input
        let request = parse_query_input(query, dimensions, measures, filter, order, limit, offset, segments, through, motif)
            .map_err(|e| err("parse_error", e.to_string(), None, &[], vec![]))?;
        let views_used = request.referenced_views();

        // Stage 3: compile query
        let result = engine.compile_query(&request)
            .map_err(|e| err("compile_error", e.to_string(), None, &[], views_used.clone()))?;

        // Stage 4: resolve connection & execute
        let config_path = config.ok_or_else(||
            err("execution_error", "--execute requires --config with database connection details".into(),
                Some(result.sql.clone()), &result.columns, views_used.clone()))?;
        let content = std::fs::read_to_string(config_path)
            .map_err(|e| err("execution_error", format!("Failed to read config {}: {}", config_path.display(), e),
                Some(result.sql.clone()), &result.columns, views_used.clone()))?;
        let exec_config: crate::executor::ExecutionConfig = serde_yaml::from_str(&content)
            .map_err(|e| err("execution_error", format!("Failed to parse config: {}", e),
                Some(result.sql.clone()), &result.columns, views_used.clone()))?;
        let connection = if let Some(ds) = datasource {
            exec_config.find_connection(ds)
        } else {
            exec_config.first_connection()
        }.map_err(|e| err("execution_error", e.to_string(),
            Some(result.sql.clone()), &result.columns, views_used.clone()))?;

        // Stage 5: execute
        let exec_result = crate::executor::execute(&connection, &result.sql, &result.params)
            .map_err(|e| err("execution_error", e.to_string(),
                Some(result.sql.clone()), &result.columns, views_used.clone()))?;

        Ok(QueryEnvelope::success(result.sql, &result.columns, exec_result, views_used))
    }

    let is_error;
    let envelope = match inner(
        path.as_ref(), globals.as_ref(), config.as_ref(), dialect.as_deref(),
        query, dimensions, measures, filter, order, limit, offset, segments, through, motif,
        datasource.as_deref(),
    ) {
        Ok(env) => { is_error = false; env }
        Err(env) => { is_error = true; env }
    };
    print_envelope(&envelope);
    if is_error {
        std::process::exit(1);
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
    motif: Option<String>,
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
        let mut request: QueryRequest = serde_json::from_str(&query_str)
            .map_err(|e| format!("Invalid query JSON: {}", e))?;
        // CLI --motif overrides JSON motif
        if motif.is_some() {
            request.motif = motif;
        }
        Ok(request)
    } else if has_flags {
        Ok(build_query_from_flags(dimensions, measures, filter, order, limit, offset, segments, through, motif)?)
    } else {
        Err("Provide either -q/--query (JSON) or --dimensions/--measures flags".into())
    }
}

/// Initialize an airlayer project directory with config.yml, CLAUDE.md, and Claude Code skills.
fn run_init(path: Option<&PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let target = path.map(|p| p.as_path()).unwrap_or_else(|| Path::new("."));

    // Ensure target directory exists
    if !target.exists() {
        std::fs::create_dir_all(target)?;
    }

    let mut created = Vec::new();
    let mut skipped = Vec::new();

    // 1. config.yml
    let config_path = target.join("config.yml");
    write_if_absent(&config_path, INIT_CONFIG_YML, &mut created, &mut skipped)?;

    // 2. views/ directory
    let views_dir = target.join("views");
    if !views_dir.exists() {
        std::fs::create_dir_all(&views_dir)?;
        created.push("views/".to_string());
    }

    // 3. CLAUDE.md
    let claude_md_path = target.join("CLAUDE.md");
    write_if_absent(&claude_md_path, INIT_CLAUDE_MD, &mut created, &mut skipped)?;

    // 4. Claude Code skills (agents + low-level tools)
    install_agents_and_skills(target, &mut created, &mut skipped)?;

    // Print summary
    if !created.is_empty() {
        println!("Created:");
        for f in &created {
            println!("  {}", f);
        }
    }
    if !skipped.is_empty() {
        println!("Already exists (skipped):");
        for f in &skipped {
            println!("  {}", f);
        }
    }

    println!("\nNext steps:");
    println!("  1. Edit config.yml with your database connection details");
    println!("  2. Run: airlayer inspect --schema --config config.yml");
    println!("  3. Or use Claude Code: /builder to bootstrap, /analyst to query");

    Ok(())
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
    write_or_update(&claude_md_path, INIT_CLAUDE_MD, &mut updated, &mut unchanged)?;

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
        ("bootstrap", include_str!("../../.claude/skills/bootstrap/SKILL.md")),
        ("profile", include_str!("../../.claude/skills/profile/SKILL.md")),
        ("query", include_str!("../../.claude/skills/query/SKILL.md")),
    ];

    for (name, content) in skills {
        let skill_dir = target.join(".claude").join("skills").join(name);
        std::fs::create_dir_all(&skill_dir)?;
        let skill_path = skill_dir.join("SKILL.md");
        write_or_update(&skill_path, content, created, skipped)?;
    }

    Ok(())
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
config.yml          Database connection configuration
views/              .view.yml semantic layer definitions
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

**Do NOT run `airlayer init` or `airlayer update`** — those are user-facing CLI commands. By the time you are reading this, init has already been run. To update agents and skills, the user runs `airlayer update`.

## Workflow

1. Edit `config.yml` with database connection details
2. `/builder` to bootstrap views from your schema, then profile and validate
3. `/analyst` to answer questions using the semantic layer
4. Back to `/builder` if the analyst needs a missing dimension or measure

## Important: no raw SQL

airlayer does NOT support raw SQL queries. There is no `--raw-sql` flag. All queries go through the semantic layer using `--dimensions`, `--measures`, and `--filter` flags (or `-q` with JSON). If you need data that isn't covered by existing views, use `/builder` to create or edit a `.view.yml` file first.

## Key concepts

- **Views** define dimensions (group-by columns) and measures (aggregations)
- **Entities** declare join keys — airlayer auto-generates JOINs when queries span views
- **Datasource** in each view maps to a database `name` in config.yml
- **Motifs** are reusable post-aggregation analytical patterns (yoy, anomaly, contribution, etc.)
- All views in a single query must use the same SQL dialect

## Motifs

Motifs wrap a base query as a CTE and add analytical columns. Use `--motif <name>` on the CLI or `\"motif\": \"<name>\"` in JSON queries.

**Builtin motifs:** yoy, qoq, mom, wow, dod (period-over-period), anomaly, contribution, trend, moving_average, rank, percent_of_total, cumulative.

Period-over-period motifs (yoy, mom, etc.) require a time dimension with an appropriate granularity. The motif name is a hint — the actual period depends on the granularity you set (e.g., `mom` with `month` granularity for month-over-month).

Example:
```bash
airlayer query --execute --config config.yml --path . \\
  --dimensions orders.category \\
  --measures orders.total_revenue \\
  --motif contribution
```
";
