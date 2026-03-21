use crate::schema::models::*;
use std::collections::{HashMap, HashSet};

/// Validates a SemanticLayer for correctness.
pub struct SchemaValidator;

impl SchemaValidator {
    pub fn validate(layer: &SemanticLayer) -> Result<(), String> {
        let mut errors = Vec::new();

        Self::validate_view_names(layer, &mut errors);
        for view in &layer.views {
            Self::validate_view(view, &mut errors);
        }
        Self::validate_entity_references(layer, &mut errors);
        Self::validate_cross_entity_refs(layer, &mut errors);
        if let Some(topics) = &layer.topics {
            Self::validate_topics(topics, layer, &mut errors);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    }

    fn validate_view_names(layer: &SemanticLayer, errors: &mut Vec<String>) {
        let mut seen = HashSet::new();
        for view in &layer.views {
            if !seen.insert(&view.name) {
                errors.push(format!("Duplicate view name: '{}'", view.name));
            }
        }
    }

    fn validate_view(view: &View, errors: &mut Vec<String>) {
        let ctx = &view.name;

        // Must have table or sql
        if view.table.is_none() && view.sql.is_none() {
            errors.push(format!("[{}] View must have either 'table' or 'sql'", ctx));
        }
        if view.table.is_some() && view.sql.is_some() {
            errors.push(format!(
                "[{}] View cannot have both 'table' and 'sql'",
                ctx
            ));
        }

        // Validate dimensions
        let mut dim_names = HashSet::new();
        for dim in &view.dimensions {
            if !dim_names.insert(&dim.name) {
                errors.push(format!("[{}] Duplicate dimension name: '{}'", ctx, dim.name));
            }
            if dim.expr.is_empty() {
                errors.push(format!(
                    "[{}] Dimension '{}' has empty expr",
                    ctx, dim.name
                ));
            }
        }

        // Validate measures
        for measure in view.measures_list() {
            if measure.measure_type != MeasureType::Count && measure.expr.is_none() {
                errors.push(format!(
                    "[{}] Measure '{}' of type {} requires an expr",
                    ctx, measure.name, measure.measure_type
                ));
            }
        }

        // Validate entity keys reference actual dimensions
        for entity in &view.entities {
            for key in entity.get_keys() {
                if !view.dimensions.iter().any(|d| d.name == key) {
                    errors.push(format!(
                        "[{}] Entity '{}' references key '{}' which is not a dimension",
                        ctx, entity.name, key
                    ));
                }
            }
        }
    }

    fn validate_entity_references(layer: &SemanticLayer, errors: &mut Vec<String>) {
        // Build map of primary entity name -> view
        let mut primary_entities: HashMap<&str, Vec<&str>> = HashMap::new();
        for view in &layer.views {
            for entity in &view.entities {
                if entity.entity_type == EntityType::Primary {
                    primary_entities
                        .entry(&entity.name)
                        .or_default()
                        .push(&view.name);
                }
            }
        }

        // Check foreign entities reference existing primary entities
        for view in &layer.views {
            for entity in &view.entities {
                if entity.entity_type == EntityType::Foreign {
                    if !primary_entities.contains_key(entity.name.as_str()) {
                        errors.push(format!(
                            "[{}] Foreign entity '{}' has no matching primary entity in any view",
                            view.name, entity.name
                        ));
                    }
                }
            }
        }
    }

    fn validate_cross_entity_refs(layer: &SemanticLayer, errors: &mut Vec<String>) {
        // Collect all entity names -> their views
        let mut entity_to_views: HashMap<&str, Vec<&str>> = HashMap::new();
        for view in &layer.views {
            for entity in &view.entities {
                entity_to_views
                    .entry(&entity.name)
                    .or_default()
                    .push(&view.name);
            }
        }

        // Collect all view names for measure-to-measure / dimension references
        let view_names: HashSet<&str> = layer.views.iter().map(|v| v.name.as_str()).collect();

        // Check {{entity.field}} and {{view.member}} references in expressions
        let re = regex::Regex::new(r"\{\{(\w+)\.(\w+)\}\}").unwrap();
        for view in &layer.views {
            for measure in view.measures_list() {
                if let Some(expr) = &measure.expr {
                    for cap in re.captures_iter(expr) {
                        let ref_name = &cap[1];
                        let _field_name = &cap[2];
                        // Skip variable references
                        if ref_name == "variables" {
                            continue;
                        }
                        // Allow entity names and view names (for measure-to-measure refs)
                        if !entity_to_views.contains_key(ref_name)
                            && !view_names.contains(ref_name)
                        {
                            errors.push(format!(
                                "[{}] Measure '{}' references unknown entity/view '{}' in expr",
                                view.name, measure.name, ref_name
                            ));
                        }
                    }
                }
            }
        }
    }

    fn validate_topics(topics: &[Topic], layer: &SemanticLayer, errors: &mut Vec<String>) {
        let view_names: HashSet<&str> = layer.views.iter().map(|v| v.name.as_str()).collect();
        for topic in topics {
            for view_ref in &topic.views {
                if !view_names.contains(view_ref.as_str()) {
                    errors.push(format!(
                        "[topic:{}] References unknown view: '{}'",
                        topic.name, view_ref
                    ));
                }
            }
            if let Some(base) = &topic.base_view {
                if !view_names.contains(base.as_str()) {
                    errors.push(format!(
                        "[topic:{}] base_view '{}' is not a known view",
                        topic.name, base
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_layer(views: Vec<View>) -> SemanticLayer {
        SemanticLayer::new(views, None)
    }

    fn simple_view(name: &str) -> View {
        View {
            name: name.to_string(),
            description: "test".to_string(),
            label: None,
            datasource: None,
            table: Some("t".to_string()),
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
            measures: None,
            segments: vec![],
        }
    }

    #[test]
    fn test_valid_schema() {
        let layer = make_layer(vec![simple_view("orders")]);
        assert!(SchemaValidator::validate(&layer).is_ok());
    }

    #[test]
    fn test_duplicate_view_names() {
        let layer = make_layer(vec![simple_view("orders"), simple_view("orders")]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("Duplicate view name"));
    }

    #[test]
    fn test_missing_table_and_sql() {
        let mut view = simple_view("broken");
        view.table = None;
        view.sql = None;
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("must have either 'table' or 'sql'"));
    }
}
