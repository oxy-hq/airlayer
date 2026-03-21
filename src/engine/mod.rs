pub mod evaluator;
pub mod join_graph;
pub mod member_sql;
pub mod query;
pub mod sql_generator;

mod error;

pub use error::EngineError;

use crate::dialect::Dialect;
use crate::schema::models::{SemanticLayer, View};
use crate::schema::parser::SchemaParser;
use crate::schema::validator::SchemaValidator;
use evaluator::SchemaEvaluator;
use join_graph::JoinGraph;
use query::{QueryRequest, QueryResult};
use sql_generator::SqlGenerator;
use std::collections::HashMap;
use std::path::Path;

/// Maps datasource names to SQL dialects.
/// Built from config.yml `databases` entries or passed explicitly.
#[derive(Debug, Clone, Default)]
pub struct DatasourceDialectMap {
    map: HashMap<String, Dialect>,
    default: Option<Dialect>,
}

impl DatasourceDialectMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            default: None,
        }
    }

    /// Create a map with a single default dialect for all datasources.
    pub fn with_default(dialect: Dialect) -> Self {
        Self {
            map: HashMap::new(),
            default: Some(dialect),
        }
    }

    /// Add a datasource -> dialect mapping.
    pub fn insert(&mut self, datasource: &str, dialect: Dialect) {
        self.map.insert(datasource.to_string(), dialect);
    }

    /// Set the default dialect (used when a view has no datasource or when
    /// the datasource isn't in the map).
    pub fn set_default(&mut self, dialect: Dialect) {
        self.default = Some(dialect);
    }

    /// Resolve the dialect for a given datasource name.
    pub fn resolve(&self, datasource: Option<&str>) -> Result<&Dialect, EngineError> {
        if let Some(ds) = datasource {
            if let Some(d) = self.map.get(ds) {
                return Ok(d);
            }
        }
        self.default.as_ref().ok_or_else(|| {
            let ds_name = datasource.unwrap_or("<none>");
            EngineError::SchemaError(format!(
                "No dialect configured for datasource '{}' and no default dialect set",
                ds_name
            ))
        })
    }

    /// Load from a config.yml databases section.
    pub fn from_config_databases(databases: &[DatabaseConfig]) -> Self {
        let mut m = Self::new();
        for db in databases {
            if let Some(dialect) = Dialect::from_str(&db.db_type) {
                m.insert(&db.name, dialect);
            }
        }
        // Use the first database as default if there is one
        if let Some(first) = databases.first() {
            if let Some(dialect) = Dialect::from_str(&first.db_type) {
                m.set_default(dialect);
            }
        }
        m
    }
}

/// A database entry from config.yml.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub db_type: String,
}

/// Partial config.yml — only the fields we need.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PartialConfig {
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,
}

/// The main semantic engine. Load .view.yml files, compile queries to SQL.
pub struct SemanticEngine {
    semantic_layer: SemanticLayer,
    evaluator: SchemaEvaluator,
    join_graph: JoinGraph,
    dialects: DatasourceDialectMap,
}

impl SemanticEngine {
    /// Load a semantic layer from a directory containing .view.yml and .topic.yml files.
    pub fn load(
        views_dir: &Path,
        topics_dir: Option<&Path>,
        dialects: DatasourceDialectMap,
    ) -> Result<Self, EngineError> {
        let parser = SchemaParser::new();
        let semantic_layer = parser.parse_directory(views_dir, topics_dir)?;
        Self::from_semantic_layer(semantic_layer, dialects)
    }

    /// Build from an already-parsed SemanticLayer.
    pub fn from_semantic_layer(
        semantic_layer: SemanticLayer,
        dialects: DatasourceDialectMap,
    ) -> Result<Self, EngineError> {
        SchemaValidator::validate(&semantic_layer)?;
        let join_graph = JoinGraph::build(&semantic_layer.views)?;
        let evaluator = SchemaEvaluator::new(&semantic_layer, &join_graph)?;
        Ok(Self {
            semantic_layer,
            evaluator,
            join_graph,
            dialects,
        })
    }

    /// Compile a query request into SQL.
    /// The dialect is resolved from the views' datasources.
    pub fn compile_query(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        let dialect = self.resolve_dialect_for_query(request)?;
        let generator = SqlGenerator::new(&self.evaluator, &self.join_graph, dialect);
        generator.generate(request)
    }

    /// Resolve which dialect to use for a query by looking at the datasources
    /// of the referenced views.
    fn resolve_dialect_for_query(&self, request: &QueryRequest) -> Result<&Dialect, EngineError> {
        let views = request.referenced_views();

        // Collect the datasources from all referenced views
        let mut datasources: Vec<Option<&str>> = Vec::new();
        for view_name in &views {
            if let Some(view) = self.semantic_layer.view_by_name(view_name) {
                datasources.push(view.datasource.as_deref());
            }
        }

        // All views in a single query should use the same dialect.
        // Use the first non-None datasource we find.
        let ds = datasources.iter().find_map(|d| *d);
        let dialect = self.dialects.resolve(ds)?;

        // Verify all views agree on the dialect
        for d in &datasources {
            let other = self.dialects.resolve(*d)?;
            if std::mem::discriminant(other) != std::mem::discriminant(dialect) {
                return Err(EngineError::QueryError(format!(
                    "Query spans multiple dialects: datasource {:?} uses {} but {:?} uses {}. \
                     Cross-database queries are not supported.",
                    ds, dialect, d, other
                )));
            }
        }

        Ok(dialect)
    }

    /// List all available views.
    pub fn views(&self) -> &[View] {
        &self.semantic_layer.views
    }

    /// Get a view by name.
    pub fn view(&self, name: &str) -> Option<&View> {
        self.semantic_layer.views.iter().find(|v| v.name == name)
    }

    /// Get the semantic layer.
    pub fn semantic_layer(&self) -> &SemanticLayer {
        &self.semantic_layer
    }

    /// Get the dialect map.
    pub fn dialects(&self) -> &DatasourceDialectMap {
        &self.dialects
    }
}
