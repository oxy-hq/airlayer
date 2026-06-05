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
        Self::validate_lifespans(layer, &mut errors);
        Self::validate_shifts(layer, &mut errors);
        if let Some(topics) = &layer.topics {
            Self::validate_topics(topics, layer, &mut errors);
        }
        if let Some(motifs) = &layer.motifs {
            Self::validate_motifs(motifs, &mut errors);
        }
        if let Some(saved_queries) = &layer.saved_queries {
            Self::validate_saved_queries(saved_queries, &mut errors);
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
            errors.push(format!("[{}] View cannot have both 'table' and 'sql'", ctx));
        }

        // Validate dimensions
        let mut dim_names = HashSet::new();
        for dim in &view.dimensions {
            if !dim_names.insert(&dim.name) {
                errors.push(format!(
                    "[{}] Duplicate dimension name: '{}'",
                    ctx, dim.name
                ));
            }
            if dim.expr.is_empty() {
                errors.push(format!("[{}] Dimension '{}' has empty expr", ctx, dim.name));
            }
        }

        // Validate measures
        for measure in view.measures_list() {
            // Shift measures carry no aggregation/expr of their own — they are
            // validated separately in `validate_shifts`.
            if measure.shift.is_some() {
                continue;
            }
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

    fn validate_entity_references(layer: &SemanticLayer, _errors: &mut [String]) {
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

        // Check foreign entities reference existing primary entities.
        // Missing primaries are warnings (the join simply won't be available),
        // not hard errors, to match CubeJS / oxy-semantic behaviour.
        for view in &layer.views {
            for entity in &view.entities {
                if entity.entity_type == EntityType::Foreign
                    && !primary_entities.contains_key(entity.name.as_str())
                {
                    eprintln!(
                        "Warning: [{}] Foreign entity '{}' has no matching primary entity in any view — join will not be available",
                        view.name, entity.name
                    );
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
        let re = crate::engine::member_sql::dotted_ref_regex();
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
                        if !entity_to_views.contains_key(ref_name) && !view_names.contains(ref_name)
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

    /// Validate `lifespan` declarations. For the **derived** form
    /// (`lifespan.from` set), the named view must exist, must declare the same
    /// entity, and the two views' entity declarations must expose the same
    /// number of keys (the cohort JOIN pairs them positionally).
    fn validate_lifespans(layer: &SemanticLayer, errors: &mut Vec<String>) {
        // Index views by name once for cheap lookups.
        let view_by_name: HashMap<&str, &View> =
            layer.views.iter().map(|v| (v.name.as_str(), v)).collect();

        for view in &layer.views {
            for entity in &view.entities {
                let Some(lifespan) = &entity.lifespan else {
                    continue;
                };
                let Some(from_view_name) = lifespan.from.as_deref() else {
                    continue; // direct form; nothing to check here
                };

                let Some(from_view) = view_by_name.get(from_view_name) else {
                    errors.push(format!(
                        "[{}] entity '{}' lifespan `from: {}` names a view that does not exist",
                        view.name, entity.name, from_view_name
                    ));
                    continue;
                };

                let Some(from_entity) = from_view.entities.iter().find(|e| e.name == entity.name)
                else {
                    errors.push(format!(
                        "[{}] entity '{}' lifespan `from: {}` must declare the same entity, but \
                         view '{}' does not have entity '{}'",
                        view.name, entity.name, from_view_name, from_view_name, entity.name
                    ));
                    continue;
                };

                let fact_keys = entity.get_keys();
                let from_keys = from_entity.get_keys();
                if from_keys.is_empty() {
                    errors.push(format!(
                        "[{}] entity '{}' lifespan `from: {}` — entity '{}' on '{}' has no keys; \
                         cannot group lifespan aggregation",
                        view.name, entity.name, from_view_name, entity.name, from_view_name
                    ));
                }
                if !fact_keys.is_empty()
                    && !from_keys.is_empty()
                    && fact_keys.len() != from_keys.len()
                {
                    errors.push(format!(
                        "[{}] entity '{}' declares {} key(s), but `from: {}` declares {}; \
                         both must expose the same number of keys (paired positionally for the \
                         cohort JOIN)",
                        view.name,
                        entity.name,
                        fact_keys.len(),
                        from_view_name,
                        from_keys.len(),
                    ));
                }
            }
        }
    }

    /// Validate `shift` measures and their cohort/lifespan requirements.
    fn validate_shifts(layer: &SemanticLayer, errors: &mut Vec<String>) {
        use crate::engine::shift::Interval;

        // Entity names that declare a lifespan anywhere in the layer.
        let lifespan_entities: HashSet<&str> = layer
            .views
            .iter()
            .flat_map(|v| v.entities.iter())
            .filter(|e| e.lifespan.is_some())
            .map(|e| e.name.as_str())
            .collect();

        for view in &layer.views {
            // Entity names declared on this view (for cohort reachability).
            let view_entities: HashSet<&str> =
                view.entities.iter().map(|e| e.name.as_str()).collect();

            for measure in view.measures_list() {
                let Some(shift) = &measure.shift else {
                    continue;
                };

                // The base measure must exist in scope and not itself be a shift.
                match view
                    .measures_list()
                    .iter()
                    .find(|m| m.name == shift.measure)
                {
                    None => errors.push(format!(
                        "[{}] shift measure '{}' references base measure '{}' which does not exist \
                         in this view",
                        view.name, measure.name, shift.measure
                    )),
                    Some(base) if base.shift.is_some() => errors.push(format!(
                        "[{}] shift measure '{}' references base '{}', which is itself a shift \
                         measure; the base must be a plain measure",
                        view.name, measure.name, shift.measure
                    )),
                    Some(_) => {}
                }

                // The interval (and maturity, if present) must parse.
                if let Err(e) = Interval::parse(&shift.by) {
                    errors.push(format!(
                        "[{}] shift measure '{}' has an invalid `by`: {}",
                        view.name, measure.name, e
                    ));
                }
                if let Some(ref m) = shift.maturity {
                    if let Err(e) = Interval::parse(m) {
                        errors.push(format!(
                            "[{}] shift measure '{}' has an invalid `maturity`: {}",
                            view.name, measure.name, e
                        ));
                    }
                }

                // `comparable_by` must name an entity on this view that declares a
                // `lifespan` (the cohort grain).
                if let Some(entity) = &shift.comparable_by {
                    if !view_entities.contains(entity.as_str()) {
                        errors.push(format!(
                            "[{}] shift measure '{}' is `comparable_by: {}`, but no entity named \
                             '{}' is declared on this view.",
                            view.name, measure.name, entity, entity
                        ));
                    } else if !lifespan_entities.contains(entity.as_str()) {
                        errors.push(format!(
                            "[{}] shift measure '{}' is `comparable_by: {}`, but entity '{}' does \
                             not declare a `lifespan` (needed to derive the cohort).",
                            view.name, measure.name, entity, entity
                        ));
                    }
                }
            }
        }
    }

    fn validate_motifs(motifs: &[Motif], errors: &mut Vec<String>) {
        let mut seen = HashSet::new();
        let builtin_names: HashSet<&str> = [
            "yoy",
            "qoq",
            "mom",
            "wow",
            "dod",
            "anomaly",
            "contribution",
            "trend",
            "moving_average",
            "rank",
            "percent_of_total",
            "cumulative",
        ]
        .into_iter()
        .collect();
        let param_re = crate::engine::member_sql::param_ref_regex();

        for motif in motifs {
            if !seen.insert(&motif.name) {
                errors.push(format!("[motif:{}] Duplicate motif name", motif.name));
            }
            match motif.motif_kind {
                MotifKind::Custom => {
                    if motif.outputs.is_empty() {
                        errors.push(format!(
                            "[motif:{}] Custom motif must have at least one 'outputs' entry",
                            motif.name
                        ));
                    }
                    // Check that {{ param }} references in outputs expressions use declared or auto-bound params.
                    // Auto-bound params (measure, time, dimensions, threshold, window) are always
                    // available at runtime via resolve_params(), so they don't need explicit declaration.
                    let auto_bound: HashSet<&str> =
                        ["measure", "time", "dimensions", "threshold", "window"]
                            .into_iter()
                            .collect();
                    for col in &motif.outputs {
                        for cap in param_re.captures_iter(&col.expr) {
                            let param_name = &cap[1];
                            if !motif.params.contains_key(param_name)
                                && !auto_bound.contains(param_name)
                            {
                                errors.push(format!(
                                    "[motif:{}] outputs column '{}' references undeclared param '{{{{{}}}}}' in expr",
                                    motif.name, col.name, param_name
                                ));
                            }
                        }
                    }
                }
                MotifKind::Builtin => {
                    if !builtin_names.contains(motif.name.as_str()) {
                        errors.push(format!("[motif:{}] Unknown builtin motif name", motif.name));
                    }
                }
            }
        }
    }

    fn validate_saved_queries(queries: &[SavedQuery], errors: &mut Vec<String>) {
        for sq in queries {
            let steps = sq.effective_steps();
            if steps.is_empty() {
                errors.push(format!(
                    "[query:{}] Query must have at least one step or inline query fields",
                    sq.name
                ));
            }
            let mut step_names = HashSet::new();
            for step in &steps {
                if !step_names.insert(&step.name) {
                    errors.push(format!(
                        "[query:{}] Duplicate step name: '{}'",
                        sq.name, step.name
                    ));
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
            description: Some("test".to_string()),
            label: None,
            datasource: None,
            dialect: None,
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
                meta: None,
            }],
            measures: None,
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
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

    #[test]
    fn test_duplicate_motif_names() {
        let motif = Motif {
            name: "yoy".into(),
            description: None,
            motif_kind: MotifKind::Builtin,
            params: HashMap::new(),
            returns: None,
            outputs: vec![],
            meta: None,
        };
        let mut layer = make_layer(vec![simple_view("orders")]);
        layer.motifs = Some(vec![motif.clone(), motif]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("Duplicate motif name"));
    }

    #[test]
    fn test_custom_motif_missing_adds() {
        let motif = Motif {
            name: "my_motif".into(),
            description: None,
            motif_kind: MotifKind::Custom,
            params: HashMap::new(),
            returns: None,
            outputs: vec![],
            meta: None,
        };
        let mut layer = make_layer(vec![simple_view("orders")]);
        layer.motifs = Some(vec![motif]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("must have at least one 'outputs'"));
    }

    #[test]
    fn test_query_duplicate_step_name() {
        use crate::engine::query::QueryRequest;
        let sq = SavedQuery {
            name: "test_q".into(),
            description: None,
            params: HashMap::new(),
            steps: vec![
                SavedQueryStep {
                    name: "step1".into(),
                    query: QueryRequest::new(),
                    description: None,
                },
                SavedQueryStep {
                    name: "step1".into(),
                    query: QueryRequest::new(),
                    description: None,
                },
            ],
            query: None,
            source_path: None,
            meta: None,
        };
        let mut layer = make_layer(vec![simple_view("orders")]);
        layer.saved_queries = Some(vec![sq]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("Duplicate step name"));
    }

    #[test]
    fn test_shift_comparable_by_entity_without_lifespan_errors() {
        // The named entity exists on the view but declares no lifespan anywhere.
        let yaml = r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_prior
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: store_id
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "test")
            .unwrap();
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(
            err.contains("comparable_by") && err.contains("lifespan"),
            "expected a clear lifespan error, got: {err}"
        );
    }

    #[test]
    fn test_shift_comparable_by_unknown_entity_errors() {
        // The named entity isn't declared on the view at all.
        let yaml = r#"
name: sales
table: sales_daily
dimensions:
  - name: id
    type: string
    expr: id
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_prior
    shift:
      measure: net_sales
      by: 1 year
      comparable_by: store_id
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "test")
            .unwrap();
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(
            err.contains("comparable_by") && err.contains("no entity named"),
            "expected an unknown-entity error, got: {err}"
        );
    }

    #[test]
    fn test_shift_unknown_base_measure_errors() {
        let yaml = r#"
name: sales
table: sales_daily
dimensions:
  - name: id
    type: string
    expr: id
measures:
  - name: net_sales_prior
    shift:
      measure: does_not_exist
      by: 1 year
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "test")
            .unwrap();
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(
            err.contains("does not exist"),
            "expected unknown base error, got: {err}"
        );
    }

    #[test]
    fn test_shift_with_lifespan_is_valid() {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      start: opened_at
      end: closed_at
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: opened_at
    type: date
    expr: opened_at
  - name: closed_at
    type: date
    expr: closed_at
"#;
        let sales = r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_prior
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: store_id
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(stores, "stores").unwrap(),
            parser.parse_view_str(sales, "sales").unwrap(),
        ]);
        assert!(SchemaValidator::validate(&layer).is_ok());
    }

    /// Derived lifespan: `lifespan.from` must name a view that exists in the
    /// layer. A typo (`from: salez`) should fail at validation time with a
    /// clear message — not be silently accepted only to error out at query
    /// compile time.
    #[test]
    fn test_derived_lifespan_unknown_from_view_errors() {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      from: salez                  # typo
      start: MIN(sale_date)
      end: MAX(sale_date)
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let sales = r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
measures:
  - name: net_sales
    type: sum
    expr: net_sales
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(stores, "stores").unwrap(),
            parser.parse_view_str(sales, "sales").unwrap(),
        ]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(
            err.contains("salez") && err.contains("does not exist"),
            "expected an unknown-view error mentioning 'salez', got: {err}"
        );
    }

    /// Derived lifespan: the `from:` view must declare the same entity (its
    /// keys define the GROUP BY for the aggregate). Otherwise the synthesized
    /// CTE has nothing to group on.
    #[test]
    fn test_derived_lifespan_from_view_missing_entity_errors() {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      from: activity
      start: MIN(event_at)
      end: MAX(event_at)
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        // `activity` exists but doesn't declare the `store_id` entity, so it
        // cannot group the lifespan aggregation.
        let activity = r#"
name: activity
table: activity_log
dimensions:
  - name: event_at
    type: date
    expr: event_at
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(stores, "stores").unwrap(),
            parser.parse_view_str(activity, "activity").unwrap(),
        ]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(
            err.contains("activity") && err.contains("does not have entity 'store_id'"),
            "expected a missing-entity error mentioning 'activity', got: {err}"
        );
    }

    /// Derived lifespan: a key-count mismatch between the fact-side and the
    /// `from`-side entity declarations breaks the cohort JOIN (we pair the
    /// keys positionally), so it must fail at validation.
    #[test]
    fn test_derived_lifespan_key_arity_mismatch_errors() {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    keys: [store_id, region_id]      # composite
    lifespan:
      from: sales
      start: MIN(sale_date)
      end: MAX(sale_date)
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let sales = r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id                    # single — arity mismatch with stores
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: sale_date
    type: date
    expr: sale_date
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(stores, "stores").unwrap(),
            parser.parse_view_str(sales, "sales").unwrap(),
        ]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(
            err.contains("same number of keys"),
            "expected a key-arity error, got: {err}"
        );
    }

    #[test]
    fn test_query_empty_steps() {
        let sq = SavedQuery {
            name: "empty_q".into(),
            description: None,
            params: HashMap::new(),
            steps: vec![],
            query: None,
            source_path: None,
            meta: None,
        };
        let mut layer = make_layer(vec![simple_view("orders")]);
        layer.saved_queries = Some(vec![sq]);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("must have at least one step"));
    }
}
