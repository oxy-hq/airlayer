//! Peer cohorts: the cross-sectional sibling of `shift` (`engine/shift.rs`).
//!
//! `shift` answers "how did this ONE entity's measure move between two time
//! windows". A cohort answers a different question: "how does this entity's
//! measure compare, in ONE window, to a group of similar-enough peers". The
//! two are orthogonal and compose (see `Measure.default_cohort` and the
//! `cohorts:` block on `Entity` in `schema/models.rs`).
//!
//! This module is the standalone core: resolving an entity's declared
//! cohort, pulling entity-grain rows for the candidate population, and the
//! two guards that stand between a peer comparison and a silently wrong
//! number:
//!
//! - **Cardinality ceiling** ([`MAX_COHORT_ENTITIES`]) — a cohort is a
//!   correlated self-join per subject, `O(n^2)`. A population in the
//!   thousands is fine; a population in the millions must be refused, not
//!   hung on.
//! - **Truncation cross-check** — the entity-grain pull sets an explicit
//!   unbounded `limit` (see [`crate::engine::UNBOUNDED_QUERY_LIMIT`]) so
//!   airlayer's own default row cap can't quietly slice the universe. But a
//!   *warehouse-side* cap (a REST API page limit, a driver-imposed ceiling)
//!   is outside airlayer's control, and a truncated pull is indistinguishable
//!   from a complete one by inspection — the row count is cross-checked
//!   against an independent `COUNT(DISTINCT ...)` query for exactly this
//!   reason.
//!
//! Task 6 fills in the peer-matching loop (the band + `require` join per
//! subject); this module currently returns an empty `subjects` list once the
//! guards pass. Task 7 wires `resolve_cohort` into the CLI.
//!
//! Cohort membership is deliberately **non-reciprocal**: A can be in B's
//! band while B is outside A's (see [`crate::schema::models::CohortBand`]).
//! Nothing in this module should evolve toward a symmetric bucketing or
//! `NTILE` partition — that would silently change what "peer" means.

use crate::engine::metric_tree_ops::{BenchmarkStatistic, QueryExecutor};
use crate::engine::query::{QueryRequest, TimeDimensionQuery};
use crate::engine::{EngineError, UNBOUNDED_QUERY_LIMIT};
use crate::schema::models::{
    Cohort, Entity, EntityType, Measure, MeasureDirection, MeasureType, SemanticLayer, View,
};
use serde::{Deserialize, Serialize};

/// A row of query results, keyed by column alias. Matches
/// [`QueryExecutor`]'s return type.
type Row = serde_json::Map<String, serde_json::Value>;

/// The largest entity population a cohort may be resolved over. A cohort is
/// a correlated self-join per subject (`O(n^2)` pairs), so this is a
/// tractability ceiling, not a business rule — 5,000 entities is 25M
/// candidate pairs, already a lot of work for a single query; the reference
/// deployment's largest entity population (restaurants) is two orders of
/// magnitude below this.
pub const MAX_COHORT_ENTITIES: usize = 5_000;

/// The result of resolving a peer cohort for a measure over a period.
///
/// `subjects` is empty until Task 6 fills in the peer-matching loop; the
/// guards in this module (cardinality ceiling, truncation cross-check) are
/// the reason a caller can trust that an eventually non-empty `subjects`
/// reflects the WHOLE candidate population, not an arbitrary slice of it.
#[derive(Debug, Clone, Serialize)]
pub struct PeerCohortResult {
    /// The entity name the cohort is declared on (e.g. `"store_id"`).
    pub entity: String,
    /// The cohort name used (e.g. `"size_matched"`).
    pub cohort: String,
    /// The fully-qualified measure being compared (e.g. `"sales.wage_pct"`).
    pub measure: String,
    /// Which statistic over the peer population is used as each subject's
    /// baseline.
    pub statistic: BenchmarkStatistic,
    /// The [start, end] period the comparison was run over.
    pub period: (String, String),
    /// Per-subject comparison results. Empty until Task 6.
    pub subjects: Vec<CohortSubject>,
    /// Subjects dropped before comparison (e.g. a NULL `require` dimension),
    /// each with a human-readable reason. Empty until Task 6.
    pub excluded: Vec<ExcludedSubject>,
}

/// A single subject's comparison against its peer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortSubject {
    /// The subject entity's key value.
    pub key: String,
    /// The subject's own measure value.
    pub value: f64,
    /// The baseline computed from the peer group (per `statistic`).
    pub baseline: f64,
    /// `value - baseline`.
    pub gap: f64,
    /// Keys of the entities that matched as peers for this subject.
    /// Non-reciprocal: this subject's presence in a peer's `peers` list is
    /// not implied, and vice versa.
    pub peers: Vec<String>,
    /// `peers.len()`, kept alongside for a consumer that only wants the
    /// count.
    pub peer_count: usize,
    /// Whether `peer_count` meets the cohort's declared `min_peers`. NOT a
    /// gate — a subject below the floor is still returned so a caller can
    /// decide for itself whether a thin baseline is usable.
    pub sufficient: bool,
}

/// A subject dropped before peer matching, with why.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcludedSubject {
    pub key: String,
    pub reason: String,
}

/// The internal measure name `augment_layer_for_cohort` installs: a
/// `count_distinct` over the entity's key, used ONLY for the cardinality
/// guard's independent count. Double-underscore-wrapped, matching the
/// `__lifespan_<entity>`/support-measure convention used elsewhere for
/// engine-installed, non-user-facing members.
const COHORT_TOTAL_MEASURE: &str = "__cohort_total__";

/// Install a `__cohort_total__` `count_distinct` measure over `entity`'s key
/// on `entity`'s owning view, if not already present.
///
/// Follows `augment_layer_for_opportunity`'s clone-and-install shape
/// (`metric_tree_ops.rs`): `resolve_cohort` needs an independent
/// `COUNT(DISTINCT key)` to guard against a truncated pull, and the SAME
/// augmented layer must reach both the engine (so the measure is queryable)
/// and this function's own count query.
///
/// Returns `false` if `entity` names no Primary entity in the layer, or if
/// its key does not resolve to a single column.
pub fn augment_layer_for_cohort(layer: &mut SemanticLayer, entity: &str) -> bool {
    let Some(view_name) = owning_view(layer, entity).map(|v| v.name.clone()) else {
        return false;
    };
    let Some(entity_decl) = layer
        .views
        .iter()
        .find(|v| v.name == view_name)
        .and_then(|v| find_primary_entity(v, entity))
    else {
        return false;
    };
    let Some(key_expr) = entity_key_expr(
        layer.views.iter().find(|v| v.name == view_name).unwrap(),
        entity_decl,
    ) else {
        return false;
    };

    let Some(view) = layer.views.iter_mut().find(|v| v.name == view_name) else {
        return false;
    };
    if !view
        .measures_list()
        .iter()
        .any(|m| m.name == COHORT_TOTAL_MEASURE)
    {
        view.measures.get_or_insert_with(Vec::new).push(Measure {
            name: COHORT_TOTAL_MEASURE.to_string(),
            measure_type: MeasureType::CountDistinct,
            description: Some(format!(
                "Internal: distinct '{entity}' count, used to guard peer cohort resolution against a truncated pull."
            )),
            expr: Some(key_expr),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            shift: None,
            direction: MeasureDirection::default(),
            default_cohort: None,
            meta: None,
        });
    }
    true
}

/// The view that declares `entity` as its Primary entity, if any.
fn owning_view<'a>(layer: &'a SemanticLayer, entity: &str) -> Option<&'a View> {
    layer
        .views
        .iter()
        .find(|v| find_primary_entity(v, entity).is_some())
}

/// The Primary entity declaration named `entity` on `view`, if any.
fn find_primary_entity<'a>(view: &'a View, entity: &str) -> Option<&'a Entity> {
    view.entities
        .iter()
        .find(|e| e.name == entity && e.entity_type == EntityType::Primary)
}

/// An entity's single-column key, resolved to its SQL expression — mirrors
/// `view_primary_entity_key`'s resolution order (name match, then `expr`
/// match against a declared dimension; falls back to the raw key as a bare
/// column when no dimension answers to it). `None` for a composite key or a
/// keyless entity — a cohort self-join needs one scalar identity per row.
fn entity_key_expr(view: &View, entity: &Entity) -> Option<String> {
    let keys = entity.get_keys();
    if keys.len() != 1 {
        return None;
    }
    let key = keys.into_iter().next()?;
    let backing = view
        .dimensions
        .iter()
        .find(|d| d.name == key)
        .or_else(|| view.dimensions.iter().find(|d| d.expr == key));
    Some(backing.map(|d| d.expr.clone()).unwrap_or(key))
}

/// An entity's single-column key, resolved to its declared dimension NAME
/// (e.g. `"store_id"`, not the column it resolves to) — the form a
/// `QueryRequest.dimensions` entry needs. Mirrors `identifier_dimensions`'s
/// resolution order. `None` when the key is composite or does not back a
/// declared dimension (a bare column can't be requested as a member).
fn entity_key_dimension_name(view: &View, entity: &Entity) -> Option<String> {
    let keys = entity.get_keys();
    if keys.len() != 1 {
        return None;
    }
    let key = keys.into_iter().next()?;
    view.dimensions
        .iter()
        .find(|d| d.name == key)
        .or_else(|| view.dimensions.iter().find(|d| d.expr == key))
        .map(|d| d.name.clone())
}

/// The bare member name (portion after the last `.`), used as the row
/// lookup key for query results in this module. A query's `measures`/
/// `dimensions` are fully-qualified `"view.member"` paths, but every path
/// this module resolves for one comparison lives on a single subject row, so
/// the bare name is unambiguous here and keeps row construction (and the
/// test fixtures that stand in for a real executor) simple.
fn short_name(member: &str) -> &str {
    member.rsplit('.').next().unwrap_or(member)
}

/// Read a numeric cell out of a result row by its bare member name (see
/// [`short_name`]).
fn row_f64(row: &Row, member: &str) -> Option<f64> {
    match row.get(short_name(member))? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Resolve `entity`'s `cohort` declaration and pull the entity-grain rows
/// needed to compare `measure` across the cohort's population over `period`.
///
/// Guard order (deliberate, not incidental):
/// 1. Resolve entity → owning view → cohort. A caller who names an unknown
///    cohort finds out immediately, not after two round trips to the
///    warehouse.
/// 2. Run an independent `COUNT(DISTINCT key)` query and refuse if it is
///    over [`MAX_COHORT_ENTITIES`] — the self-join tractability ceiling.
///    Checked BEFORE the pull: there is no reason to pull rows for a
///    population we're about to refuse anyway.
/// 3. Build and run the entity-grain pull, with an explicit
///    `UNBOUNDED_QUERY_LIMIT` so airlayer's own default row cap can't
///    truncate it.
/// 4. Cross-check the pull's row count against the independent count from
///    step 2. A mismatch means something outside airlayer's control (a
///    warehouse-side page cap) truncated the pull — refuse rather than
///    compute a baseline over a partial universe.
///
/// Returns an empty `subjects`/`excluded` result once the guards pass; the
/// peer-matching loop is Task 6.
pub fn resolve_cohort(
    layer: &SemanticLayer,
    entity: &str,
    cohort: &str,
    measure: &str,
    time_dimension: &str,
    period: (&str, &str),
    statistic: BenchmarkStatistic,
    executor: &QueryExecutor,
) -> Result<PeerCohortResult, EngineError> {
    let (start, end) = period;

    let view = owning_view(layer, entity).ok_or_else(|| {
        EngineError::SchemaError(format!(
            "no primary entity named '{entity}' found in the layer"
        ))
    })?;
    let entity_decl = find_primary_entity(view, entity)
        .expect("owning_view only returns a view where find_primary_entity succeeds");
    let cohort_decl: &Cohort = entity_decl
        .cohorts
        .as_ref()
        .and_then(|cohorts| cohorts.get(cohort))
        .ok_or_else(|| {
            let known: Vec<&str> = entity_decl
                .cohorts
                .as_ref()
                .map(|c| c.keys().map(String::as_str).collect())
                .unwrap_or_default();
            EngineError::SchemaError(format!(
                "entity '{entity}' has no cohort named '{cohort}' (known cohorts: {known:?})"
            ))
        })?;

    let key_dim = entity_key_dimension_name(view, entity_decl).ok_or_else(|| {
        EngineError::SchemaError(format!(
            "entity '{entity}' on view '{}' has no single-column key resolvable to a declared dimension",
            view.name
        ))
    })?;

    let period_time_dimension = || TimeDimensionQuery {
        dimension: time_dimension.to_string(),
        granularity: None,
        date_range: Some(vec![start.to_string(), end.to_string()]),
    };

    // Step 2: the independent count, checked BEFORE the pull.
    let count_measure = format!("{}.{COHORT_TOTAL_MEASURE}", view.name);
    let count_query = QueryRequest {
        measures: vec![count_measure.clone()],
        time_dimensions: vec![period_time_dimension()],
        ..Default::default()
    };
    let count_rows = executor(&count_query)?;
    let total = count_rows
        .first()
        .and_then(|r| row_f64(r, &count_measure))
        .unwrap_or(0.0) as usize;

    if total > MAX_COHORT_ENTITIES {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}' spans {total} entities, over the \
             {MAX_COHORT_ENTITIES}-entity cap on a peer self-join ({total} entities is \
             {total}^2 candidate pairs); narrow the period or the population before comparing"
        )));
    }

    // Step 3: the entity-grain pull, unbounded on purpose.
    let mut dimensions = vec![format!("{}.{key_dim}", view.name)];
    dimensions.extend(cohort_decl.require.iter().cloned());
    dimensions.dedup();

    let mut measures = vec![measure.to_string()];
    if let Some(band) = &cohort_decl.band {
        measures.push(band.measure.clone());
        if let Some(per) = &band.per {
            measures.push(per.clone());
        }
    }
    measures.dedup();

    let pull_query = QueryRequest {
        measures,
        dimensions,
        time_dimensions: vec![period_time_dimension()],
        limit: Some(UNBOUNDED_QUERY_LIMIT),
        ..Default::default()
    };
    let rows = executor(&pull_query)?;

    // Step 4: the truncation cross-check. A cap airlayer doesn't control
    // (e.g. a warehouse REST API's own page limit) can still slice the pull
    // even though we asked for everything; a truncated pull is otherwise
    // indistinguishable from a complete one, and a median over an arbitrary
    // slice is a wrong answer with no error.
    if rows.len() != total {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}' pulled {} rows but the independent count \
             query reported {total}; refusing a possibly truncated universe rather than \
             computing a baseline over part of it",
            rows.len()
        )));
    }

    Ok(PeerCohortResult {
        entity: entity.to_string(),
        cohort: cohort.to_string(),
        measure: measure.to_string(),
        statistic,
        period: (start.to_string(), end.to_string()),
        // Task 6 fills the peer-matching loop.
        subjects: Vec::new(),
        excluded: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a `row(&[..])`-style test row from key/value pairs.
    fn row(pairs: &[(&str, serde_json::Value)]) -> Row {
        pairs
            .iter()
            .cloned()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    /// A canned entity-grain row, distinct per `i`, carrying every field the
    /// `size_matched` cohort's pull query in `cohort_test_layer` requests:
    /// the key, the `require`d dimension, the band's measure and its `per`,
    /// and the target measure. Task 5's guards only look at row COUNT, not
    /// content, so the values below are placeholders sized for Task 6's
    /// peer-matching loop to consume later.
    fn entity_row(i: i64) -> Row {
        row(&[
            ("store_id", json!(format!("store_{i}"))),
            ("accounting_basis", json!("accrual")),
            ("net_sales", json!(100_000.0 + i as f64)),
            ("trading_days", json!(360.0)),
            ("wage_pct", json!(0.25)),
        ])
    }

    /// A `stores` view (primary `store_id` entity, declaring the
    /// `size_matched` cohort per the spec example: band on
    /// `sales.net_sales` / `sales.trading_days` with 0.35 tolerance,
    /// `require: [stores.accounting_basis]`, `min_peers: 3`) plus a `sales`
    /// view supplying `net_sales`, `trading_days`, and `wage_pct`.
    ///
    /// Calls `augment_layer_for_cohort` itself (rather than hand-declaring
    /// `__cohort_total__` in the YAML) so the fixture exercises the same
    /// augmentation path production code takes — the count query's
    /// `stores.__cohort_total__` measure must come from the SAME function
    /// `resolve_cohort`'s caller is expected to run first, not a hand-typed
    /// stand-in that could silently drift from it.
    fn cohort_test_layer() -> SemanticLayer {
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched:
        band:
          measure: sales.net_sales
          per: sales.trading_days
          tolerance: 0.35
        require: [stores.accounting_basis]
        min_peers: 3
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: accounting_basis
    type: string
    expr: accounting_basis
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
  - name: sale_date
    type: date
    expr: sale_date
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: trading_days
    type: sum
    expr: trading_days
  - name: wage_pct
    type: number
    expr: "wages / NULLIF(net_sales, 0)"
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let mut layer = SemanticLayer::new(
            vec![
                parser.parse_view_str(stores, "stores").unwrap(),
                parser.parse_view_str(sales, "sales").unwrap(),
            ],
            None,
        );
        assert!(
            augment_layer_for_cohort(&mut layer, "store_id"),
            "fixture setup: augment_layer_for_cohort must succeed for store_id"
        );
        layer
    }

    #[test]
    fn resolve_cohort_refuses_a_truncated_universe() {
        // Controller ruling: the count query returns 4000 (below the 5000
        // cardinality ceiling, so that guard does not fire), but the
        // entity-grain pull returns only 3000 rows. That mismatch is exactly
        // the case the warehouse-side cap this cross-check exists for: a
        // truncated pull is indistinguishable from a complete one by
        // inspection, so the count cross-check is the only thing standing
        // between us and a median computed over an arbitrary 3k slice of a
        // 4k universe — a wrong number with no error.
        let layer = cohort_test_layer();
        let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("__cohort_total__", json!(4_000.0))])]);
            }
            Ok((0..3_000).map(entity_row).collect())
        };
        let err = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "sales.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3000") && msg.contains("4000"), "got: {msg}");
    }

    #[test]
    fn resolve_cohort_sets_an_explicit_unbounded_limit() {
        // Both halves of the guard matter: the explicit limit prevents the
        // common case, the count assertion catches a warehouse-side cap we
        // don't control.
        let layer = cohort_test_layer();
        // QueryExecutor is `dyn Fn(..) + Send + Sync + 'static`, so the
        // closure must own its capture — Arc::clone in, keep the original
        // outside to inspect (same pattern as
        // `reachable_values_filtered_appends_scope_to_date_filters` above).
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_inner = std::sync::Arc::clone(&seen);
        let executor = move |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            seen_inner.lock().unwrap().push(q.limit);
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("__cohort_total__", json!(2.0))])]);
            }
            Ok(vec![entity_row(0), entity_row(1)])
        };
        let _ = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "sales.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        );
        let limits = seen.lock().unwrap();
        assert!(
            limits
                .iter()
                .any(|l| *l == Some(crate::engine::UNBOUNDED_QUERY_LIMIT)),
            "the entity-grain pull must set an explicit unbounded limit, saw {limits:?}"
        );
    }

    #[test]
    fn resolve_cohort_refuses_an_unbounded_entity_grain() {
        // A cohort self-join is O(N^2): 23 restaurants is 529 pairs, 2M
        // customers is 4e12. Refuse with the count in the message rather
        // than hanging.
        let layer = cohort_test_layer();
        let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("__cohort_total__", json!(2_000_000.0))])]);
            }
            Ok(vec![])
        };
        let err = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "sales.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .unwrap_err();
        assert!(err.to_string().contains("2000000"));
        assert!(err.to_string().contains("5000"));
    }

    #[test]
    fn resolve_cohort_rejects_an_unknown_cohort_name() {
        let layer = cohort_test_layer();
        let executor = |_: &QueryRequest| -> Result<Vec<Row>, EngineError> { Ok(vec![]) };
        let err = resolve_cohort(
            &layer,
            "store_id",
            "no_such",
            "sales.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no_such"));
    }
}
