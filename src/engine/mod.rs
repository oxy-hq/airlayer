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
    /// Whether the default was explicitly set (via CLI -d flag or config.yml),
    /// as opposed to being inferred from view-level dialect fields.
    explicit_default: bool,
}

impl DatasourceDialectMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            default: None,
            explicit_default: false,
        }
    }

    /// Create a map with a single default dialect for all datasources.
    pub fn with_default(dialect: Dialect) -> Self {
        Self {
            map: HashMap::new(),
            default: Some(dialect),
            explicit_default: true,
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
        self.explicit_default = true;
    }

    /// Set the default dialect inferred from view-level fields (lower priority than explicit).
    fn set_inferred_default(&mut self, dialect: Dialect) {
        self.default = Some(dialect);
        // Don't set explicit_default — this is a soft/inferred default
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

    /// Check whether a datasource name is explicitly mapped in this config.
    pub fn has_datasource(&self, datasource: &str) -> bool {
        self.map.contains_key(datasource)
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
        mut dialects: DatasourceDialectMap,
    ) -> Result<Self, EngineError> {
        SchemaValidator::validate(&semantic_layer)?;

        // If no default dialect is set, try to infer from view-level dialect fields.
        // If all views with a dialect field agree, use that as the default.
        if dialects.default.is_none() {
            let mut view_dialect: Option<Dialect> = None;
            let mut conflict = false;
            for view in &semantic_layer.views {
                // Skip views whose datasource is already mapped
                if let Some(ref ds) = view.datasource {
                    if dialects.has_datasource(ds) {
                        continue;
                    }
                }
                if let Some(ref dialect_str) = view.dialect {
                    if let Some(d) = Dialect::from_str(dialect_str) {
                        if let Some(ref existing) = view_dialect {
                            if std::mem::discriminant(existing) != std::mem::discriminant(&d) {
                                conflict = true;
                                break;
                            }
                        } else {
                            view_dialect = Some(d);
                        }
                    }
                }
            }
            // Only set the default if all views agree (conflict is checked at query time)
            if !conflict {
                if let Some(d) = view_dialect {
                    dialects.set_inferred_default(d);
                }
            }
        }

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
    /// of the referenced views, falling back to view-level `dialect` fields.
    ///
    /// Priority chain (highest to lowest):
    /// 1. CLI `-d` flag (stored as the default on DatasourceDialectMap)
    /// 2. config.yml datasource mapping
    /// 3. View-level `dialect` field in .view.yml (injected as default at construction time)
    /// 4. Default: postgres (set by CLI when neither -d nor -c is given)
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

        // Check for conflicting view-level dialect declarations,
        // but only when the default was NOT explicitly set (via CLI -d or config).
        // When an explicit default is set, it takes priority and view-level dialect is ignored.
        if !self.dialects.explicit_default {
            for view_name in &views {
                if let Some(view) = self.semantic_layer.view_by_name(view_name) {
                    // Skip views whose datasource is explicitly mapped in config
                    if let Some(ref ds_name) = view.datasource {
                        if self.dialects.has_datasource(ds_name) {
                            continue;
                        }
                    }
                    if let Some(ref dialect_str) = view.dialect {
                        if let Some(d) = Dialect::from_str(dialect_str) {
                            if std::mem::discriminant(&d) != std::mem::discriminant(dialect) {
                                return Err(EngineError::QueryError(format!(
                                    "Query spans multiple dialects: view '{}' declares dialect '{}' \
                                     but resolved dialect is '{}'. Cross-database queries are not supported.",
                                    view.name, dialect_str, dialect
                                )));
                            }
                        } else {
                            return Err(EngineError::SchemaError(format!(
                                "Unknown dialect '{}' in view '{}'",
                                dialect_str, view.name
                            )));
                        }
                    }
                }
            }
        }

        // Verify all datasource-based views agree on the dialect
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    fn simple_view_with_dialect(name: &str, dialect: Option<&str>) -> View {
        View {
            name: name.to_string(),
            description: "test".to_string(),
            label: None,
            datasource: None,
            dialect: dialect.map(|s| s.to_string()),
            table: Some(format!("{}", name)),
            sql: None,
            entities: vec![],
            dimensions: vec![Dimension {
                name: "id".to_string(),
                dimension_type: DimensionType::Number,
                description: None,
                expr: "id".to_string(),
                original_expr: None,
                samples: None,
                synonyms: None,
                primary_key: None,
                sub_query: None,
                inherits_from: None,
            }],
            measures: Some(vec![Measure {
                name: "count".to_string(),
                measure_type: MeasureType::Count,
                description: None,
                expr: None,
                original_expr: None,
                filters: None,
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
            }]),
            segments: vec![],
        }
    }

    #[test]
    fn test_view_level_dialect_bigquery() {
        let view = simple_view_with_dialect("orders", Some("bigquery"));
        let layer = SemanticLayer::new(vec![view], None);
        // No default dialect set — view-level dialect should be used
        let dialects = DatasourceDialectMap::new();
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // BigQuery uses backtick quoting
        assert!(
            result.sql.contains('`'),
            "Expected BigQuery backtick quoting, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_view_level_dialect_conflict_error() {
        let view1 = simple_view_with_dialect("orders", Some("bigquery"));
        let mut view2 = simple_view_with_dialect("customers", Some("postgres"));
        // Give view2 a foreign entity pointing at orders so the query can reference both
        view2.entities.push(Entity {
            name: "order".to_string(),
            entity_type: EntityType::Foreign,
            description: None,
            key: Some("id".to_string()),
            keys: None,
            inherits_from: None,
        });
        // Add primary entity to orders
        let mut view1_with_entity = view1;
        view1_with_entity.entities.push(Entity {
            name: "order".to_string(),
            entity_type: EntityType::Primary,
            description: None,
            key: Some("id".to_string()),
            keys: None,
            inherits_from: None,
        });

        let layer = SemanticLayer::new(vec![view1_with_entity, view2], None);
        let dialects = DatasourceDialectMap::new();
        // Construction should still succeed (conflict only checked at query time via default)
        // But since views disagree, the engine won't set a default from views
        let engine = SemanticEngine::from_semantic_layer(layer, dialects);
        // With conflicting view dialects and no default, construction still works
        // but querying across both views should fail
        assert!(engine.is_err() || {
            let eng = engine.unwrap();
            let request = QueryRequest {
                dimensions: vec!["orders.id".to_string(), "customers.id".to_string()],
                measures: vec![],
                ..QueryRequest::new()
            };
            eng.compile_query(&request).is_err()
        });
    }

    #[test]
    fn test_cli_dialect_overrides_view_dialect() {
        let view = simple_view_with_dialect("orders", Some("bigquery"));
        let layer = SemanticLayer::new(vec![view], None);
        // CLI sets postgres as default, which should override view-level bigquery
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // Postgres uses double-quote quoting, not backticks
        assert!(
            !result.sql.contains('`'),
            "Expected Postgres quoting (no backticks), got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_view_without_dialect_uses_default() {
        let view = simple_view_with_dialect("orders", None);
        let layer = SemanticLayer::new(vec![view], None);
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // Should work fine with default postgres
        assert!(
            result.sql.contains("\"orders\""),
            "Expected Postgres quoting, got:\n{}",
            result.sql
        );
    }
}
