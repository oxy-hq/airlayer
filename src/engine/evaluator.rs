use crate::engine::join_graph::JoinGraph;
use crate::engine::EngineError;
use crate::schema::models::*;
use std::collections::HashMap;

/// The schema evaluator: resolves member paths, looks up definitions,
/// and provides the interface the SQL generator needs.
pub struct SchemaEvaluator {
    views: HashMap<String, View>,
    /// view_name.member_name -> MemberKind
    member_index: HashMap<String, MemberKind>,
    /// entity_name -> Vec<(view_name, is_primary)>
    entity_index: HashMap<String, Vec<(String, bool)>>,
    /// Primary keys per view: view_name -> Vec<dimension_name>
    primary_keys: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberKind {
    Dimension,
    Measure,
    Segment,
}

impl SchemaEvaluator {
    pub fn new(layer: &SemanticLayer, _join_graph: &JoinGraph) -> Result<Self, EngineError> {
        let mut views = HashMap::new();
        let mut member_index = HashMap::new();
        let mut entity_index: HashMap<String, Vec<(String, bool)>> = HashMap::new();
        let mut primary_keys = HashMap::new();

        for view in &layer.views {
            views.insert(view.name.clone(), view.clone());

            // Index dimensions
            for dim in &view.dimensions {
                let path = format!("{}.{}", view.name, dim.name);
                member_index.insert(path, MemberKind::Dimension);
            }

            // Index measures
            for measure in view.measures_list() {
                let path = format!("{}.{}", view.name, measure.name);
                member_index.insert(path, MemberKind::Measure);
            }

            // Index segments
            for segment in &view.segments {
                let path = format!("{}.{}", view.name, segment.name);
                member_index.insert(path, MemberKind::Segment);
            }

            // Index entities
            for entity in &view.entities {
                let is_primary = entity.entity_type == EntityType::Primary;
                entity_index
                    .entry(entity.name.clone())
                    .or_default()
                    .push((view.name.clone(), is_primary));
            }

            // Index primary keys
            let pks: Vec<String> = view
                .entities
                .iter()
                .filter(|e| e.entity_type == EntityType::Primary)
                .flat_map(|e| e.get_keys())
                .collect();
            if !pks.is_empty() {
                primary_keys.insert(view.name.clone(), pks);
            }
        }

        Ok(SchemaEvaluator {
            views,
            member_index,
            entity_index,
            primary_keys,
        })
    }

    /// Get a view by name.
    pub fn view(&self, name: &str) -> Option<&View> {
        self.views.get(name)
    }

    /// Get a dimension by path (view.dimension).
    pub fn dimension(&self, view: &str, name: &str) -> Option<&Dimension> {
        self.views
            .get(view)
            .and_then(|v| v.dimensions.iter().find(|d| d.name == name))
    }

    /// Get a measure by path (view.measure).
    pub fn measure(&self, view: &str, name: &str) -> Option<&Measure> {
        self.views
            .get(view)
            .and_then(|v| v.measures_list().iter().find(|m| m.name == name))
    }

    /// Get a segment by path (view.segment).
    pub fn segment(&self, view: &str, name: &str) -> Option<&Segment> {
        self.views
            .get(view)
            .and_then(|v| v.segments.iter().find(|s| s.name == name))
    }

    /// Parse a dotted member path into (view, member).
    pub fn parse_member_path(&self, path: &str) -> Result<(String, String), EngineError> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() != 2 {
            return Err(EngineError::QueryError(format!(
                "Invalid member path: '{}'. Expected format: 'view.member'",
                path
            )));
        }
        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Check what kind of member a path refers to.
    pub fn member_kind(&self, path: &str) -> Option<&MemberKind> {
        self.member_index.get(path)
    }

    /// Is this a measure?
    pub fn is_measure(&self, path: &str) -> bool {
        self.member_index.get(path) == Some(&MemberKind::Measure)
    }

    /// Is this a dimension?
    pub fn is_dimension(&self, path: &str) -> bool {
        self.member_index.get(path) == Some(&MemberKind::Dimension)
    }

    /// Get all view names.
    pub fn view_names(&self) -> Vec<&String> {
        self.views.keys().collect()
    }

    /// Get primary keys for a view.
    pub fn primary_keys(&self, view: &str) -> Option<&Vec<String>> {
        self.primary_keys.get(view)
    }

    /// Find which view owns a primary entity.
    pub fn primary_view_for_entity(&self, entity_name: &str) -> Option<&str> {
        self.entity_index.get(entity_name).and_then(|entries| {
            entries
                .iter()
                .find(|(_, is_primary)| *is_primary)
                .map(|(view_name, _)| view_name.as_str())
        })
    }

    /// Build a map of entity_name -> view_alias for resolving cross-entity refs
    /// within a given query context.
    pub fn build_entity_to_alias_map(
        &self,
        base_view: &str,
        joined_views: &[&str],
    ) -> HashMap<String, String> {
        let mut map = HashMap::new();

        // Add entities from base view
        if let Some(view) = self.views.get(base_view) {
            for entity in &view.entities {
                if entity.entity_type == EntityType::Primary {
                    map.insert(entity.name.clone(), view.name.clone());
                }
            }
        }

        // Add entities from joined views
        for &jv in joined_views {
            if let Some(view) = self.views.get(jv) {
                for entity in &view.entities {
                    if entity.entity_type == EntityType::Primary {
                        map.entry(entity.name.clone())
                            .or_insert_with(|| view.name.clone());
                    }
                }
            }
        }

        map
    }

    /// Get all views.
    pub fn all_views(&self) -> impl Iterator<Item = &View> {
        self.views.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::join_graph::JoinGraph;

    fn make_test_layer() -> SemanticLayer {
        SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: "Orders".to_string(),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("orders".to_string()),
                sql: None,
                entities: vec![Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("order_id".to_string()),
                    keys: None,
                    inherits_from: None,
                    meta: None,
                }],
                dimensions: vec![
                    Dimension {
                        name: "order_id".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "id".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                        sub_query: None,
                        inherits_from: None,
                        meta: None,
                    },
                    Dimension {
                        name: "status".to_string(),
                        dimension_type: DimensionType::String,
                        description: None,
                        expr: "status".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                        sub_query: None,
                        inherits_from: None,
                        meta: None,
                    },
                ],
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
                    meta: None,
                }]),
                segments: vec![],
                meta: None,
            }],
            None,
        )
    }

    #[test]
    fn test_member_lookup() {
        let layer = make_test_layer();
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();

        assert!(eval.is_dimension("orders.order_id"));
        assert!(eval.is_dimension("orders.status"));
        assert!(eval.is_measure("orders.count"));
        assert!(!eval.is_measure("orders.status"));
        assert!(!eval.is_dimension("orders.count"));
    }

    #[test]
    fn test_parse_member_path() {
        let layer = make_test_layer();
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();

        let (view, member) = eval.parse_member_path("orders.status").unwrap();
        assert_eq!(view, "orders");
        assert_eq!(member, "status");
    }
}
