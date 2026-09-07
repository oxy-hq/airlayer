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
        Self::validate_promotions(layer, &mut errors);
        Self::validate_cohorts(layer, &mut errors);
        Self::validate_drivers(layer, &mut errors);
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
            if dim.analysis.is_some() && dim.segmentable.is_some() {
                eprintln!(
                    "[{}] dimension '{}' declares both `analysis` and the deprecated \
                     `segmentable`; `analysis` wins. Remove `segmentable` to silence this.",
                    view.name, dim.name
                );
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

    /// Validate `parent:` declarations on entities and surface the
    /// promotion-closure-level findings (name collisions, cross-source
    /// ambiguities) as warnings.
    ///
    /// Hard errors (fail validation):
    /// 1. `parent: X` on a non-Primary entity — the parent relationship is
    ///    intrinsic to the entity itself and is declared in its definition
    ///    (its Primary). Foreign declarations are usages and must not carry
    ///    `parent:` (it would mean different things at different usage sites
    ///    and silently disagree).
    /// 2. `parent: X` where no view declares `X` as a Primary entity — the
    ///    chain dead-ends and no rollup can be realised.
    /// 3. A cycle in the parent chain — surfaced via `Promotions::build`.
    ///
    /// Warnings (do not fail validation; reported to stderr to mirror the
    /// existing `validate_entity_references` behaviour):
    /// - Induced name collides with an explicit measure on the target view
    ///   (explicit wins; induced is dropped).
    /// - The same induced name is reachable from multiple distinct source
    ///   views (ambiguity; planner resolves via `through`).
    fn validate_promotions(layer: &SemanticLayer, errors: &mut Vec<String>) {
        let mut primary_owners: HashSet<&str> = HashSet::new();
        for v in &layer.views {
            for e in &v.entities {
                if e.entity_type == EntityType::Primary {
                    primary_owners.insert(e.name.as_str());
                }
            }
        }

        for view in &layer.views {
            for entity in &view.entities {
                let Some(parent) = entity.parent.as_deref() else {
                    continue;
                };
                if entity.entity_type != EntityType::Primary {
                    errors.push(format!(
                        "[{}] entity '{}' declares `parent: {}` but is not a primary entity. \
                         The parent relationship is intrinsic to the entity and belongs on its \
                         primary declaration; foreign declarations are usages and cannot carry \
                         `parent:`.",
                        view.name, entity.name, parent
                    ));
                    continue;
                }
                if !primary_owners.contains(parent) {
                    errors.push(format!(
                        "[{}] entity '{}' declares `parent: {}` but no view declares '{}' as a \
                         primary entity. The hierarchy dead-ends; the chain cannot be walked.",
                        view.name, entity.name, parent, parent
                    ));
                }
            }
        }

        // Always run Promotions::build regardless of earlier errors so that
        // cycle errors in one part of the hierarchy are reported alongside
        // dead-end or foreign-parent errors in another part.
        let promotions = match crate::engine::promotions::Promotions::build(&layer.views) {
            Err(e) => {
                errors.push(format!("{:?}", e));
                return;
            }
            Ok(p) => p,
        };
        for c in promotions.collisions() {
            let sources = c
                .dropped_sources
                .iter()
                .map(|(src, path)| format!("{}[{}]", src, path.join("→")))
                .collect::<Vec<_>>()
                .join(", ");
            eprintln!(
                "Warning: [{}] explicit measure '{}' shadows promoted measure(s) from {} — \
                 the induced measure is not exposed.",
                c.target_view, c.measure_name, sources,
            );
        }
        for a in promotions.ambiguities() {
            let cands = a
                .candidates
                .iter()
                .map(|(src, path)| format!("{}[{}]", src, path.join("→")))
                .collect::<Vec<_>>()
                .join(", ");
            eprintln!(
                "Warning: [{}] measure '{}' is induced from multiple source views ({}). \
                 Queries must use `through` to disambiguate.",
                a.target_view, a.measure_name, cands,
            );
        }
    }

    /// Validate `cohorts:` declarations on entities, and `default_cohort:` on
    /// measures.
    ///
    /// All hard errors. A cohort that cannot be resolved is not a degraded
    /// cohort — it is a silently wrong benchmark, which is the failure mode
    /// this whole feature exists to prevent.
    fn validate_cohorts(layer: &SemanticLayer, errors: &mut Vec<String>) {
        // Every declared cohort, as "entity.name", for default_cohort checks.
        let mut declared: HashSet<String> = HashSet::new();

        for view in &layer.views {
            for entity in &view.entities {
                let Some(cohorts) = entity.cohorts.as_ref() else {
                    continue;
                };
                if entity.entity_type != EntityType::Primary {
                    errors.push(format!(
                        "[{}] entity '{}' declares `cohorts:` but is not a primary entity. \
                         A cohort compares instances of an entity, which needs a row identity; \
                         foreign declarations are usages and cannot carry cohorts.",
                        view.name, entity.name
                    ));
                    continue;
                }

                // The entity-grain pull selects the key as a dimension.
                let keys = entity.get_keys();
                for key in &keys {
                    let backed = view
                        .dimensions
                        .iter()
                        .any(|d| &d.name == key || &d.expr == key);
                    if !backed {
                        errors.push(format!(
                            "[{}] entity '{}' declares `cohorts:` but its key '{}' has no \
                             dimension backing it (matched by name, then by expr). Cohort \
                             resolution selects the key as a dimension to group the entity \
                             universe; declare a dimension for '{}'.",
                            view.name, entity.name, key, key
                        ));
                    }
                }

                for (cohort_name, cohort) in cohorts {
                    declared.insert(format!("{}.{}", entity.name, cohort_name));

                    if let Some(band) = &cohort.band {
                        if !band.tolerance.is_finite() || band.tolerance <= 0.0 {
                            errors.push(format!(
                                "[{}] cohort '{}.{}' has tolerance {} — must be finite and > 0. \
                                 (There is deliberately no upper bound: 1.0 means 'up to 2x' \
                                 and is legitimate.)",
                                view.name, entity.name, cohort_name, band.tolerance
                            ));
                        }
                        Self::require_member(
                            layer,
                            &band.measure,
                            view,
                            entity,
                            cohort_name,
                            "band.measure",
                            errors,
                        );
                        if let Some(per) = &band.per {
                            Self::require_member(
                                layer,
                                per,
                                view,
                                entity,
                                cohort_name,
                                "band.per",
                                errors,
                            );
                        }
                    }

                    for req in &cohort.require {
                        Self::require_member(
                            layer,
                            req,
                            view,
                            entity,
                            cohort_name,
                            "require",
                            errors,
                        );
                    }

                    if let Some(mp) = cohort.min_peers {
                        if mp < 1 {
                            errors.push(format!(
                                "[{}] cohort '{}.{}' has min_peers: 0 — a cohort needs at least \
                                 one peer to have a baseline at all.",
                                view.name, entity.name, cohort_name
                            ));
                        }
                    }
                }
            }
        }

        for view in &layer.views {
            for m in view.measures_list() {
                let Some(dc) = m.default_cohort.as_deref() else {
                    continue;
                };
                if !declared.contains(dc) {
                    errors.push(format!(
                        "[{}] measure '{}' declares `default_cohort: {}` but no entity declares \
                         that cohort. Expected 'entity_name.cohort_name'; declared cohorts are: {}.",
                        view.name,
                        m.name,
                        dc,
                        if declared.is_empty() {
                            "(none)".to_string()
                        } else {
                            let mut d: Vec<&String> = declared.iter().collect();
                            d.sort();
                            d.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                        }
                    ));
                }
            }
        }
    }

    /// Check that `member` ("view.member") resolves to a dimension or measure.
    fn require_member(
        layer: &SemanticLayer,
        member: &str,
        view: &View,
        entity: &Entity,
        cohort_name: &str,
        field: &str,
        errors: &mut Vec<String>,
    ) {
        let ok = member.split_once('.').is_some_and(|(v, name)| {
            layer.views.iter().any(|target| {
                target.name == v
                    && (target.dimensions.iter().any(|d| d.name == name)
                        || target.measures_list().iter().any(|m| m.name == name))
            })
        });
        if !ok {
            errors.push(format!(
                "[{}] cohort '{}.{}' field `{}` references '{}', which does not resolve to a \
                 dimension or measure. Expected 'view.member'.",
                view.name, entity.name, cohort_name, field, member
            ));
        }
    }

    /// Driver declarations whose response magnitude cannot be resolved to a
    /// shape.
    ///
    /// `Driver::response_coefficients` refuses a contradictory declaration
    /// (both `coefficient:` and `coefficients:`) and one whose vector does not
    /// match its `form:`'s basis width, rather than silently picking or
    /// padding. `MetricTree::build` records that refusal and leaves the edge
    /// qualitative — but a build warning is not something the author is made
    /// to read, and the symptom is a lever that moves nothing with no visible
    /// cause. Catch it at validation, where a malformed declaration is what it
    /// is: an error in the schema, not a discarded number.
    fn validate_drivers(layer: &SemanticLayer, errors: &mut Vec<String>) {
        for view in &layer.views {
            for measure in view.measures_list() {
                for driver in measure.drivers.iter().flatten() {
                    if let Err(e) = driver.response_coefficients() {
                        errors.push(format!(
                            "[{}] measure '{}' driver '{}' {}",
                            view.name, measure.name, driver.measure, e
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
                segmentable: None,
                analysis: None,
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

    /// A driver whose magnitude cannot be resolved to a shape is a schema
    /// error, not a number to discard. `MetricTree::build` leaves such an edge
    /// qualitative and records why, but nothing made the author read that — so
    /// three quarters of the declared magnitudes below used to vanish while
    /// `validate` reported a clean schema.
    #[test]
    fn test_malformed_driver_coefficients_error() {
        let yaml = r#"
name: orders
table: orders
dimensions:
  - { name: id, type: number, expr: id }
measures:
  - { name: total, type: sum, expr: v }
  - name: kpi
    type: number
    expr: "{{orders.total}} * 2"
    drivers:
      - { measure: orders.total, coefficients: [1.5, -0.02], form: quadratic }
      - { measure: orders.total, coefficient: 1.0, coefficients: [2.0] }
      - { measure: orders.total, coefficients: [1.0, 2.0] }
      - { measure: orders.total, coefficient: 0.5, form: linear-log-quadratic }
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "orders")
            .unwrap();
        let err = SchemaValidator::validate(&make_layer(vec![view]))
            .expect_err("three of the four drivers are malformed");
        assert!(
            err.contains("declares both `coefficient:` and `coefficients:`"),
            "the contradictory declaration must be reported: {err}"
        );
        assert_eq!(
            err.matches("coefficient(s) but this `form:` needs").count(),
            2,
            "both width mismatches must be reported: {err}"
        );
        // The well-formed quadratic must not be flagged.
        assert_eq!(err.lines().count(), 3, "exactly three errors: {err}");
    }

    /// A driver declaring only a direction, or a scalar with no `form:`, is the
    /// ordinary case and must stay valid.
    #[test]
    fn test_well_formed_drivers_validate() {
        let yaml = r#"
name: orders
table: orders
dimensions:
  - { name: id, type: number, expr: id }
measures:
  - { name: total, type: sum, expr: v }
  - name: kpi
    type: number
    expr: "{{orders.total}} * 2"
    drivers:
      - { measure: orders.total, direction: positive }
      - { measure: orders.total, coefficient: 1.25 }
      - { measure: orders.total, coefficients: [1.5, -0.02], form: quadratic }
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "orders")
            .unwrap();
        assert!(SchemaValidator::validate(&make_layer(vec![view])).is_ok());
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

    /// `parent:` belongs on the entity's *definition* — the Primary
    /// declaration. Foreign declarations are usages; if they could carry
    /// `parent:` independently they'd silently disagree across views.
    #[test]
    fn test_parent_on_foreign_entity_errors() {
        let yaml = r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
    parent: company_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "sales")
            .unwrap();
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(
            err.contains("parent: company_id") && err.contains("not a primary entity"),
            "expected a parent-on-foreign error, got: {err}"
        );
    }

    /// `parent: X` where X is never declared as a Primary entity → the chain
    /// dead-ends; reject at validation time so the user gets a clear message
    /// instead of silently missing rollups.
    #[test]
    fn test_parent_dead_end_errors() {
        let yaml = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    parent: company_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let view = crate::schema::parser::SchemaParser::new()
            .parse_view_str(yaml, "stores")
            .unwrap();
        let layer = make_layer(vec![view]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(
            err.contains("parent: company_id") && err.contains("no view declares 'company_id'"),
            "expected a dead-end-parent error, got: {err}"
        );
    }

    /// A cycle in the parent chain is caught at validation time. Two entities
    /// each naming the other as parent.
    #[test]
    fn test_parent_cycle_errors() {
        let a = r#"
name: a
table: a
entities:
  - name: ea
    type: primary
    key: ea
    parent: eb
dimensions:
  - name: ea
    type: string
    expr: ea
"#;
        let b = r#"
name: b
table: b
entities:
  - name: eb
    type: primary
    key: eb
    parent: ea
dimensions:
  - name: eb
    type: string
    expr: eb
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(a, "a").unwrap(),
            parser.parse_view_str(b, "b").unwrap(),
        ]);
        let err = SchemaValidator::validate(&layer).expect_err("expected validation error");
        assert!(err.contains("cycle"), "expected a cycle error, got: {err}");
    }

    /// A well-formed hierarchy validates cleanly. Sales uses store_id, which
    /// is defined on stores with `parent: company_id`, and company_id is
    /// defined on companies.
    #[test]
    fn test_parent_chain_validates() {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    parent: company_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let companies = r#"
name: companies
table: companies
entities:
  - name: company_id
    type: primary
    key: company_id
dimensions:
  - name: company_id
    type: string
    expr: company_id
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
    expr: amount
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(stores, "stores").unwrap(),
            parser.parse_view_str(companies, "companies").unwrap(),
            parser.parse_view_str(sales, "sales").unwrap(),
        ]);
        assert!(SchemaValidator::validate(&layer).is_ok());
    }

    /// Bug 10: `validate_promotions` returns early (before calling
    /// `Promotions::build`) when it has already collected errors (e.g. a
    /// dead-end parent). This means a cycle in a DIFFERENT part of the entity
    /// graph is silently hidden behind the dead-end error.
    ///
    /// Expected (correct) behaviour: when a schema has BOTH a dead-end parent
    /// AND a cycle, validation should surface BOTH errors.
    ///
    /// This test is expected to FAIL until the bug is fixed: currently only
    /// the dead-end error is reported; the cycle error is swallowed.
    #[test]
    fn test_dead_end_and_cycle_both_reported() {
        // View with a dead-end: store_id declares parent "nonexistent" which is
        // never a Primary entity anywhere.
        let dead_end = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    parent: nonexistent
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        // Two views that form a cycle: entity_a → entity_b → entity_a.
        let cycle_a = r#"
name: cycle_view_a
table: cycle_view_a
entities:
  - name: entity_a
    type: primary
    key: id_a
    parent: entity_b
dimensions:
  - name: id_a
    type: string
    expr: id_a
"#;
        let cycle_b = r#"
name: cycle_view_b
table: cycle_view_b
entities:
  - name: entity_b
    type: primary
    key: id_b
    parent: entity_a
dimensions:
  - name: id_b
    type: string
    expr: id_b
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer = make_layer(vec![
            parser.parse_view_str(dead_end, "stores").unwrap(),
            parser.parse_view_str(cycle_a, "cycle_view_a").unwrap(),
            parser.parse_view_str(cycle_b, "cycle_view_b").unwrap(),
        ]);
        // Verify the cycle alone IS detected by Promotions::build.
        let cycle_views_only: Vec<_> = layer
            .views
            .iter()
            .filter(|v| v.name != "stores")
            .cloned()
            .collect();
        assert!(
            crate::engine::promotions::Promotions::build(&cycle_views_only).is_err(),
            "Promotions::build must detect the cycle in cycle_view_a/cycle_view_b"
        );
        // The validator must report BOTH the dead-end error AND the cycle error.
        // Currently it returns early after the dead-end, hiding the cycle.
        let err = SchemaValidator::validate(&layer).expect_err("schema has errors");
        assert!(
            err.contains("nonexistent") || err.contains("dead-end") || err.contains("dead_ends"),
            "expected a dead-end error mentioning 'nonexistent', got: {err}"
        );
        assert!(
            err.contains("cycle"),
            "expected a cycle error but only got: {err}"
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

    // --- Cohort validation ---------------------------------------------

    /// A fully valid two-view (stores + sales) layer with one cohort,
    /// `size_matched`, on `stores.store_id`. Callers override one field via
    /// the closure to isolate a single rule violation.
    fn cohort_layer_yaml(band_measure: &str, tolerance: f64, min_peers: &str) -> (String, String) {
        let stores = format!(
            r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched:
        band:
          measure: {band_measure}
          per: sales.trading_days
          tolerance: {tolerance}
        require: [stores.accounting_basis]
        min_peers: {min_peers}
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: accounting_basis
    type: string
    expr: accounting_basis
"#
        );
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
  - name: trading_days
    type: sum
    expr: trading_days
"#
        .to_string();
        (stores, sales)
    }

    fn layer_with_cohort_on_foreign() -> SemanticLayer {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: foreign
    key: store_id
    cohorts:
      size_matched: {}
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![parser.parse_view_str(stores, "stores").unwrap()])
    }

    fn layer_with_cohort_band_measure(band_measure: &str) -> SemanticLayer {
        let (stores, sales) = cohort_layer_yaml(band_measure, 0.35, "3");
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![
            parser.parse_view_str(&stores, "stores").unwrap(),
            parser.parse_view_str(&sales, "sales").unwrap(),
        ])
    }

    fn layer_with_cohort_tolerance(tolerance: f64) -> SemanticLayer {
        let (stores, sales) = cohort_layer_yaml("sales.net_sales", tolerance, "3");
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![
            parser.parse_view_str(&stores, "stores").unwrap(),
            parser.parse_view_str(&sales, "sales").unwrap(),
        ])
    }

    fn layer_with_cohort_min_peers(min_peers: usize) -> SemanticLayer {
        let (stores, sales) = cohort_layer_yaml("sales.net_sales", 0.35, &min_peers.to_string());
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![
            parser.parse_view_str(&stores, "stores").unwrap(),
            parser.parse_view_str(&sales, "sales").unwrap(),
        ])
    }

    fn layer_with_cohort_but_no_key_dimension() -> SemanticLayer {
        // `store_id` is the entity's key, but no dimension named `store_id`
        // (or with expr `store_id`) exists on the view.
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched: {}
dimensions:
  - name: name
    type: string
    expr: name
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![parser.parse_view_str(stores, "stores").unwrap()])
    }

    fn layer_with_default_cohort(default_cohort: &str) -> SemanticLayer {
        let stores = format!(
            r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched: {{}}
dimensions:
  - name: store_id
    type: string
    expr: store_id
measures:
  - name: net_sales
    type: sum
    expr: net_sales
    default_cohort: "{default_cohort}"
"#
        );
        let parser = crate::schema::parser::SchemaParser::new();
        make_layer(vec![parser.parse_view_str(&stores, "stores").unwrap()])
    }

    #[test]
    fn test_cohort_on_foreign_entity_errors() {
        let layer = layer_with_cohort_on_foreign();
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(
            err.contains("not a primary entity"),
            "expected primary-entity rejection, got: {err}"
        );
    }

    #[test]
    fn test_cohort_unknown_band_measure_errors() {
        let layer = layer_with_cohort_band_measure("sales.no_such_measure");
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("no_such_measure"), "got: {err}");
    }

    #[test]
    fn test_cohort_zero_tolerance_errors() {
        let layer = layer_with_cohort_tolerance(0.0);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("tolerance"), "got: {err}");
    }

    #[test]
    fn test_cohort_large_tolerance_is_valid() {
        // `1.0` means "up to 2x" and is legitimate. The first draft's (0,1)
        // upper bound was arbitrary; do not reintroduce it.
        let layer = layer_with_cohort_tolerance(1.5);
        assert!(
            SchemaValidator::validate(&layer).is_ok(),
            "expected large tolerance to be valid, got: {:?}",
            SchemaValidator::validate(&layer)
        );
    }

    #[test]
    fn test_cohort_min_peers_zero_errors() {
        let layer = layer_with_cohort_min_peers(0);
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("min_peers"), "got: {err}");
    }

    #[test]
    fn test_cohort_key_without_backing_dimension_errors() {
        // The entity-grain pull selects the key AS A DIMENSION. If no
        // dimension answers to the key by name or by expr, the pull cannot
        // be built — say so here, not at query time.
        let layer = layer_with_cohort_but_no_key_dimension();
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(
            err.contains("no dimension"),
            "expected backing-dimension error, got: {err}"
        );
    }

    #[test]
    fn test_measure_default_cohort_unknown_errors() {
        let layer = layer_with_default_cohort("store_id.no_such_cohort");
        let err = SchemaValidator::validate(&layer).unwrap_err();
        assert!(err.contains("no_such_cohort"), "got: {err}");
    }
}
