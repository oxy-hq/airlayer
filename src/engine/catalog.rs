//! Catalog: flat listing of all semantic layer objects.
//!
//! Produces an LLM-friendly index of every view, dimension, measure, segment,
//! entity, motif, and saved query in the semantic layer.

use serde::Serialize;
use std::collections::HashMap;

use crate::engine::motifs::builtin_motifs;
use crate::schema::models::SemanticLayer;

/// The kind of semantic object.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogEntryKind {
    View,
    Dimension,
    Measure,
    Segment,
    Entity,
    Motif,
    SavedQuery,
    Topic,
}

/// A single entry in the catalog.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogEntry {
    /// What kind of object this is.
    pub kind: CatalogEntryKind,
    /// Qualified name (e.g., "orders.total_revenue" for a measure, "yoy" for a motif).
    pub name: String,
    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The owning view (for dimensions, measures, segments, entities).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    /// Type info (dimension type, measure type, entity type, motif kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_type: Option<String>,
    /// User-defined metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

/// Build a flat catalog of all objects in the semantic layer.
pub fn catalog(layer: &SemanticLayer) -> Vec<CatalogEntry> {
    let mut entries = Vec::new();

    for view in &layer.views {
        // View itself
        entries.push(CatalogEntry {
            kind: CatalogEntryKind::View,
            name: view.name.clone(),
            description: view.description.as_ref().filter(|s| !s.is_empty()).cloned(),
            view: None,
            member_type: None,
            meta: view.meta.clone(),
        });

        // Dimensions
        for dim in &view.dimensions {
            entries.push(CatalogEntry {
                kind: CatalogEntryKind::Dimension,
                name: format!("{}.{}", view.name, dim.name),
                description: dim.description.clone(),
                view: Some(view.name.clone()),
                member_type: Some(dim.dimension_type.to_string()),
                meta: dim.meta.clone(),
            });
        }

        // Measures
        for measure in view.measures_list() {
            entries.push(CatalogEntry {
                kind: CatalogEntryKind::Measure,
                name: format!("{}.{}", view.name, measure.name),
                description: measure.description.clone(),
                view: Some(view.name.clone()),
                member_type: Some(measure.measure_type.to_string()),
                meta: measure.meta.clone(),
            });
        }

        // Segments
        for segment in &view.segments {
            entries.push(CatalogEntry {
                kind: CatalogEntryKind::Segment,
                name: format!("{}.{}", view.name, segment.name),
                description: segment.description.clone(),
                view: Some(view.name.clone()),
                member_type: None,
                meta: segment.meta.clone(),
            });
        }

        // Entities
        for entity in &view.entities {
            entries.push(CatalogEntry {
                kind: CatalogEntryKind::Entity,
                name: format!("{}.{}", view.name, entity.name),
                description: entity.description.clone(),
                view: Some(view.name.clone()),
                member_type: Some(format!("{:?}", entity.entity_type).to_lowercase()),
                meta: entity.meta.clone(),
            });
        }
    }

    // Topics
    for topic in layer.topics_list() {
        entries.push(CatalogEntry {
            kind: CatalogEntryKind::Topic,
            name: topic.name.clone(),
            description: topic.description.as_ref().filter(|s| !s.is_empty()).cloned(),
            view: None,
            member_type: None,
            meta: topic.meta.clone(),
        });
    }

    // Motifs: builtins + custom
    let builtins = builtin_motifs();
    let all_motifs = builtins.iter().chain(layer.motifs_list().iter());
    for motif in all_motifs {
        let params: Vec<String> = motif.params.keys().cloned().collect();
        let outputs: Vec<String> = motif.outputs.iter().map(|o| o.name.clone()).collect();
        let mut desc = motif.description.clone().unwrap_or_default();
        if !params.is_empty() {
            desc.push_str(&format!(" (params: {})", params.join(", ")));
        }
        if !outputs.is_empty() {
            desc.push_str(&format!(" (outputs: {})", outputs.join(", ")));
        }
        entries.push(CatalogEntry {
            kind: CatalogEntryKind::Motif,
            name: motif.name.clone(),
            description: Some(desc).filter(|s| !s.is_empty()),
            view: None,
            member_type: Some(format!("{:?}", motif.motif_kind).to_lowercase()),
            meta: motif.meta.clone(),
        });
    }

    // Saved queries
    for sq in layer.saved_queries_list() {
        let steps = sq.effective_steps();
        let step_names: Vec<String> = steps.iter().map(|s| s.name.clone()).collect();
        let mut desc = sq.description.clone().unwrap_or_default();
        if step_names.len() > 1 {
            desc.push_str(&format!(" (steps: {})", step_names.join(", ")));
        }
        entries.push(CatalogEntry {
            kind: CatalogEntryKind::SavedQuery,
            name: sq.name.clone(),
            description: Some(desc).filter(|s| !s.is_empty()),
            view: None,
            member_type: if steps.len() > 1 {
                Some("multi_step".to_string())
            } else {
                Some("single_step".to_string())
            },
            meta: sq.meta.clone(),
        });
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    fn test_view() -> View {
        View {
            name: "orders".to_string(),
            description: Some("Order transactions".to_string()),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("public.orders".to_string()),
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
            dimensions: vec![Dimension {
                name: "status".to_string(),
                dimension_type: DimensionType::String,
                description: Some("Order status".to_string()),
                expr: "status".to_string(),
                original_expr: None,
                samples: Some(vec!["active".to_string(), "cancelled".to_string()]),
                synonyms: None,
                primary_key: None,
                sub_query: None,
                inherits_from: None,
                meta: Some(HashMap::from([(
                    "tags".to_string(),
                    vec!["filter-friendly".to_string()],
                )])),
            }],
            measures: Some(vec![Measure {
                name: "total_revenue".to_string(),
                measure_type: MeasureType::Sum,
                description: Some("Total revenue".to_string()),
                expr: Some("amount".to_string()),
                original_expr: None,
                filters: None,
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
                meta: Some(HashMap::from([(
                    "questions".to_string(),
                    vec!["What is our revenue?".to_string()],
                )])),
            }]),
            segments: vec![Segment {
                name: "active_only".to_string(),
                expr: "status = 'active'".to_string(),
                description: Some("Active orders only".to_string()),
                inherits_from: None,
                meta: None,
            }],
            meta: Some(HashMap::from([(
                "domain".to_string(),
                vec!["commerce".to_string()],
            )])),
        }
    }

    #[test]
    fn test_catalog_basic() {
        let layer = SemanticLayer::new(vec![test_view()], None);
        let entries = catalog(&layer);

        // Should have: 1 view + 1 dim + 1 measure + 1 segment + 1 entity + 12 builtin motifs
        let views: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.kind, CatalogEntryKind::View))
            .collect();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "orders");
        assert_eq!(views[0].meta.as_ref().unwrap()["domain"], vec!["commerce"]);

        let dims: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.kind, CatalogEntryKind::Dimension))
            .collect();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].name, "orders.status");
        assert_eq!(dims[0].member_type.as_deref(), Some("string"));

        let measures: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.kind, CatalogEntryKind::Measure))
            .collect();
        assert_eq!(measures.len(), 1);
        assert_eq!(measures[0].name, "orders.total_revenue");
        assert_eq!(
            measures[0].meta.as_ref().unwrap()["questions"],
            vec!["What is our revenue?"]
        );

        let motifs: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.kind, CatalogEntryKind::Motif))
            .collect();
        assert_eq!(motifs.len(), 12); // builtins only
    }

    #[test]
    fn test_catalog_empty_layer() {
        let layer = SemanticLayer::new(vec![], None);
        let entries = catalog(&layer);
        // Only builtins
        assert_eq!(entries.len(), 12);
        assert!(entries
            .iter()
            .all(|e| matches!(e.kind, CatalogEntryKind::Motif)));
    }
}
