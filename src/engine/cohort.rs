//! Peer cohorts: the cross-sectional sibling of `shift` (`engine/shift.rs`).
//!
//! `shift` answers "how did this ONE entity's measure move between two time
//! windows". A cohort answers a different question: "how does this entity's
//! measure compare, in ONE window, to a group of similar-enough peers". The
//! two are orthogonal and compose (see `Measure.default_cohort` and the
//! `cohorts:` block on `Entity` in `schema/models.rs`).
//!
//! This module is the standalone core: resolving an entity's declared
//! cohort, pulling entity-grain rows for the candidate population, matching
//! each subject against its own peer group, and the guards that stand
//! between a peer comparison and a silently wrong number:
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
//!   reason — in BOTH directions, since a pull with *more* rows than there
//!   are entities is not at entity grain and would double-count.
//! - **Reported exclusions** — a subject that cannot be compared (a NULL
//!   `require` value, an un-normalisable band, an unreadable measure) lands
//!   in `excluded` with a reason. Nothing is dropped without a trace, and a
//!   thin cohort is *reported* insufficient rather than filtered away: a
//!   subject appears in `subjects` or in `excluded`, never in both and never
//!   in neither.
//!
//! [`resolve_cohort`] is wired into the CLI via the `cohort` subcommand
//! (`src/cli/mod.rs`).
//!
//! Cohort membership is deliberately **non-reciprocal**: A can be in B's
//! band while B is outside A's (see [`crate::schema::models::CohortBand`]).
//! Nothing in this module should evolve toward a symmetric bucketing or
//! `NTILE` partition — that would silently change what "peer" means.

use crate::engine::metric_tree_ops::{
    measure_direction, quantile_r7, BenchmarkStatistic, QueryExecutor,
};
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
///
/// The ceiling is a memory bound as well as a time bound: `CohortSubject.peers`
/// is `O(n^2)` in aggregate across a cohort's subjects, so at the ceiling
/// with a wide tolerance and no `require` narrowing the candidate pool, the
/// peer lists alone are on the order of a gigabyte.
pub const MAX_COHORT_ENTITIES: usize = 5_000;

/// The result of resolving a peer cohort for a measure over a period.
///
/// The guards in this module (cardinality ceiling, truncation cross-check)
/// are the reason a caller can trust that `subjects` reflects the WHOLE
/// candidate population, not an arbitrary slice of it.
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
    /// Per-subject comparison results, in pull order. Every comparable
    /// entity appears here — including one whose peer group is too thin,
    /// which is reported with `sufficient: false` rather than filtered out.
    pub subjects: Vec<CohortSubject>,
    /// Subjects dropped before comparison (e.g. a NULL `require` dimension),
    /// each with a human-readable reason. Disjoint from `subjects`: an
    /// excluded entity is also nobody else's peer.
    pub excluded: Vec<ExcludedSubject>,
}

/// A single subject's comparison against its peer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortSubject {
    /// The subject entity's key value.
    pub key: String,
    /// The subject's own measure value.
    pub value: f64,
    /// The baseline computed from the peer group (per `statistic`). `0.0`
    /// when `peer_count` is zero — there is no baseline, not a baseline of
    /// zero.
    pub baseline: f64,
    /// The distance from the baseline, oriented so **positive always means
    /// opportunity**, matching `opportunity`'s convention:
    /// `baseline - value` for a higher-is-better measure, `value - baseline`
    /// for a lower-is-better one (a cost or defect rate above its peers).
    /// `0.0` when there are no peers.
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

/// Why [`augment_layer_for_cohort`] refused `entity`, phrased for a user.
///
/// The bool return is deliberately kept (callers only branch on it), but
/// "is not a primary entity with a single-column key" conflates three very
/// different schema problems — no such entity, a foreign-only declaration,
/// and a composite key — and reads as the first one. A caller reporting the
/// refusal asks here for the actual cause.
pub fn augment_failure_reason(layer: &SemanticLayer, entity: &str) -> String {
    let Some(view) = owning_view(layer, entity) else {
        return format!(
            "no primary entity named '{entity}' is declared in the layer (a cohort compares \
             instances of an entity, so it needs a `type: primary` declaration)"
        );
    };
    let decl = find_primary_entity(view, entity)
        .expect("owning_view only returns a view where find_primary_entity succeeds");
    let keys = decl.get_keys();
    match keys.len() {
        0 => format!(
            "entity '{entity}' on view '{}' declares no key; a peer cohort needs a \
             single-column key — peer identity is one scalar value per row",
            view.name
        ),
        1 => format!(
            "entity '{entity}' on view '{}' could not be prepared for cohort resolution",
            view.name
        ),
        _ => format!(
            "entity '{entity}' on view '{}' has a composite key ({keys:?}); a peer cohort \
             needs a single-column key — peer identity is one scalar value per row, and the \
             truncation guard counts one COUNT(DISTINCT <key>)",
            view.name
        ),
    }
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

/// A measure as the CALLER named it, paired with the name the COMPILER
/// actually projects it under.
///
/// The two differ for an **induced** (promoted) measure. `stores.wage_pct`
/// is not declared anywhere: it exists because `sales` declares `store_id`
/// as Foreign and `stores` declares it Primary, so every `sales` measure is
/// queryable at `stores` grain (see `engine/promotions.rs`).
/// `SemanticEngine::compile_query` rewrites such a name to
/// `sales.wage_pct` *before* generating SQL and patches the requested name
/// back onto `QueryResult.columns` only — never onto the executor's rows, and
/// never onto `ColumnMeta.alias`. So the cells come back keyed
/// `sales__wage_pct`, and every other lookup that takes a measure name (the
/// polarity read below, most of all) must use the source name too.
///
/// Keeping both is the point: `requested` is what the caller asked and what
/// every message and `PeerCohortResult.measure` reports back, `source` is
/// what the warehouse answered under.
struct MeasureRef {
    requested: String,
    source: String,
}

/// The band's two measures, resolved. `Some` exactly when the cohort
/// declares a band.
struct BandMeasures {
    measure: MeasureRef,
    per: Option<MeasureRef>,
}

/// Every measure a cohort reads out of the entity-grain pull, resolved once
/// up front so no read site has to remember the distinction.
struct CohortMeasures {
    target: MeasureRef,
    band: Option<BandMeasures>,
}

/// Resolve `member` to the measure name the compiler will project it under.
///
/// Three cases, in order:
/// 1. The named view declares the measure → it is its own source.
/// 2. Exactly one promotion candidate → that candidate's source measure.
/// 3. More than one candidate → **refused**, naming them.
///
/// A name that is neither declared nor induced is returned unchanged, so the
/// SQL generator's own "measure not found" error is what the caller sees —
/// this function does not invent a second wording for it.
///
/// Case 3 mirrors `compile_query`'s ambiguity refusal rather than its
/// resolution: `QueryRequest.through` is the engine's disambiguator, and a
/// cohort declaration has no equivalent field to carry a hint. Picking one
/// candidate silently would answer a different question from the one asked,
/// in a module whose whole purpose is that the reported comparison and the
/// executed one cannot drift apart.
fn resolve_measure_source(
    layer: &SemanticLayer,
    promotions: &crate::engine::promotions::Promotions,
    member: &str,
    what: &str,
) -> Result<MeasureRef, EngineError> {
    let as_requested = || MeasureRef {
        requested: member.to_string(),
        source: member.to_string(),
    };
    let Some((view_name, measure_name)) = member.split_once('.') else {
        return Ok(as_requested());
    };
    let declared = layer
        .views
        .iter()
        .find(|v| v.name == view_name)
        .is_some_and(|v| v.measures_list().iter().any(|m| m.name == measure_name));
    if declared {
        return Ok(as_requested());
    }
    match promotions.candidates(view_name, measure_name) {
        [] => Ok(as_requested()),
        [only] => Ok(MeasureRef {
            requested: member.to_string(),
            source: format!("{}.{}", only.source_view, only.source_measure),
        }),
        many => {
            let mut sources: Vec<&str> = many.iter().map(|c| c.source_view.as_str()).collect();
            sources.sort_unstable();
            sources.dedup();
            Err(EngineError::QueryError(format!(
                "{what} '{member}' is an induced measure reachable from more than one source \
                 view ({sources:?}), so it is refused rather than resolved to one of them \
                 silently. Name the source measure directly (e.g. '{}.{measure_name}')",
                sources[0]
            )))
        }
    }
}

/// The SQL column alias the compiler gives a fully-qualified member:
/// `sales.net_sales` is projected as `sales__net_sales`. Executor rows come
/// back keyed by that alias, never by the bare member name, so every row
/// lookup in this module goes through here — the same convention
/// `metric_tree_ops.rs` uses throughout (`target.replace('.', "__")`).
///
/// Deliberately with **no** bare-name fallback: a fallback would paper over
/// exactly the mismatch that makes a cohort read the wrong column and
/// silently compare zeroes.
fn member_alias(member: &str) -> String {
    member.replace('.', "__")
}

/// Read a numeric cell out of a result row by its member alias (see
/// [`member_alias`]). `None` for a missing column, a SQL NULL, a value that
/// will not parse as a number, or a non-finite one — all four are "the
/// warehouse did not give us a number", which this module reports rather than
/// reads as zero.
///
/// The finiteness filter is load-bearing, not defensive tidiness. The REST
/// executors (Snowflake, BigQuery, Databricks) return numerics as JSON
/// strings and `"NaN"`/`"inf"` both parse, so a non-finite cell is a live
/// path. A NaN that gets past here passes the band's `d != 0.0` divisor guard
/// (`NaN != 0.0` is true), makes every band comparison false so the subject
/// reports `peer_count: 0` instead of being excluded, and — in an
/// exact-match-only cohort, where no numeric filter stands in its way —
/// becomes a peer of everyone and drags a NaN through `select_baseline`'s
/// sort into other subjects' baselines. Rejecting it here routes every
/// non-finite cell into the exclusion channels that already exist for a null.
fn row_f64(row: &Row, member: &str) -> Option<f64> {
    match row.get(&member_alias(member))? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
    .filter(|v| v.is_finite())
}

/// Read a cell as a string identity (an entity key or a `require` value).
///
/// `None` means SQL NULL or an absent column. Keeping that distinct from the
/// literal string `"NULL"` is load-bearing: a NULL `require` value joins
/// nothing, and the subject carrying it must be *reported* as excluded, not
/// silently grouped with every other NULL-valued subject under a shared
/// `"NULL"` bucket.
fn row_str(row: &Row, member: &str) -> Option<String> {
    match row.get(&member_alias(member))? {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Append `member` to `into` only if it is not already there.
///
/// `Vec::dedup` collapses only ADJACENT duplicates, so it misses the shapes
/// that actually occur here: a `require` naming the key dimension after
/// another dimension, or a band whose `per` is the measure being compared.
fn push_unique(into: &mut Vec<String>, member: String) {
    if !into.contains(&member) {
        into.push(member);
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
/// Precondition: the caller must have run `augment_layer_for_cohort` on the
/// same `layer` passed here — it wires the synthetic `COHORT_TOTAL_MEASURE`
/// count measure this function depends on for its guards.
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
        // Two very different causes share this guard, and saying so matters:
        // a composite key is a schema shape a cohort cannot express at all,
        // while a missing backing dimension is a one-line fix. The validator
        // rejects both up front, but a layer built programmatically can skip
        // it, so the runtime diagnosis has to be accurate on its own.
        EngineError::SchemaError(match entity_decl.get_keys().len() {
            1 => format!(
                "entity '{entity}' on view '{}' has no single-column key resolvable to a \
                 declared dimension",
                view.name
            ),
            0 => format!(
                "entity '{entity}' on view '{}' declares no key; a peer cohort needs a \
                 single-column key — peer identity is one scalar value per row",
                view.name
            ),
            _ => format!(
                "entity '{entity}' on view '{}' has a composite key ({:?}); a peer cohort \
                 needs a single-column key — peer identity is one scalar value per row",
                view.name,
                entity_decl.get_keys()
            ),
        })
    })?;

    // Resolve every measure name the pull will read to the name the compiler
    // actually projects it under (see [`MeasureRef`]). Done HERE, before the
    // first round trip, because the one way this can fail — an induced name
    // reachable from two source views — is a question about the schema, and
    // a caller should not pay two warehouse queries to be told so.
    //
    // `require` entries deliberately get no such treatment: promotion is a
    // measure-only mechanism (`rewrite_induced_measures` touches
    // `QueryRequest.measures` and nothing else), so a dimension is always
    // projected under the name it was requested by.
    let promotions = crate::engine::promotions::Promotions::build(&layer.views)?;
    let measures = CohortMeasures {
        target: resolve_measure_source(layer, &promotions, measure, "the compared measure")?,
        band: match &cohort_decl.band {
            None => None,
            Some(band) => Some(BandMeasures {
                measure: resolve_measure_source(
                    layer,
                    &promotions,
                    &band.measure,
                    "the cohort's `band.measure`",
                )?,
                per: match &band.per {
                    None => None,
                    Some(per) => Some(resolve_measure_source(
                        layer,
                        &promotions,
                        per,
                        "the cohort's `band.per`",
                    )?),
                },
            }),
        },
    };

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
    // A missing or unparseable count is NOT zero. Reading it as zero would
    // silently disarm both guards below — the cardinality ceiling would never
    // fire, and the truncation cross-check would compare every real pull
    // against a fabricated 0 — so it refuses on its own terms.
    let Some(total) = count_rows
        .first()
        .and_then(|r| row_f64(r, &count_measure))
        .filter(|t| t.is_finite() && *t >= 0.0)
        .map(|t| t as usize)
    else {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}': the entity-count query returned no \
             readable value for '{count_measure}'; that count is the only cross-check on a \
             truncated pull, so it is refused rather than assumed to be zero"
        )));
    };

    if total > MAX_COHORT_ENTITIES {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}' spans {total} entities, over the \
             {MAX_COHORT_ENTITIES}-entity cap on a peer self-join ({total} entities is \
             {total}^2 candidate pairs); narrow the period or the population before comparing"
        )));
    }

    // Step 3: the entity-grain pull, unbounded on purpose.
    let key_member = format!("{}.{key_dim}", view.name);
    let mut dimensions = vec![key_member.clone()];
    for req in &cohort_decl.require {
        push_unique(&mut dimensions, req.clone());
    }

    // The pull asks for the SOURCE names, so the rows come back keyed exactly
    // as this module reads them. Requesting the induced names instead would
    // compile to the identical SQL — `compile_query` rewrites them the same
    // way — but would leave the alias the reads depend on one indirection
    // away from anything this function can see.
    let mut pull_measures = vec![measures.target.source.clone()];
    if let Some(band) = &measures.band {
        push_unique(&mut pull_measures, band.measure.source.clone());
        if let Some(per) = &band.per {
            push_unique(&mut pull_measures, per.source.clone());
        }
    }

    let pull_query = QueryRequest {
        measures: pull_measures,
        dimensions,
        time_dimensions: vec![period_time_dimension()],
        limit: Some(UNBOUNDED_QUERY_LIMIT),
        ..Default::default()
    };
    let rows = executor(&pull_query)?;

    // A pulled row whose entity key is NULL is not an entity, and must be
    // partitioned off BEFORE the cross-check below.
    //
    // The pull joins the entity view through a `ManyToOne` hop, which always
    // compiles to a LEFT JOIN, and `pick_base_view` puts the FACT view on the
    // left because it owns every measure the pull names. So an orphaned fact
    // row — a key with no matching row in the entity view, routine on a real
    // warehouse — survives the join, and `GROUP BY key` emits it as one
    // NULL-key group. The independent `COUNT(DISTINCT key)` skips NULLs, and
    // its own query has the OPPOSITE base view (it names only the synthetic
    // count measure, declared on the entity view) so it never sees the orphan
    // at all. Counting the NULL-key group against that total would make a
    // single orphaned fact row look like a fanned-out pull and refuse the
    // whole cohort, blaming a `require` member that is blameless.
    //
    // `parse_candidates` already has the right answer for such a row —
    // reported in `excluded`, nobody's peer — so the rows are passed through
    // whole and only the CROSS-CHECK counts the keyed ones.
    let keyed = rows
        .iter()
        .filter(|r| row_str(r, &key_member).is_some())
        .count();

    // Step 4: the truncation cross-check. A cap airlayer doesn't control
    // (e.g. a warehouse REST API's own page limit) can still slice the pull
    // even though we asked for everything; a truncated pull is otherwise
    // indistinguishable from a complete one, and a median over an arbitrary
    // slice is a wrong answer with no error.
    //
    // Both directions are wrong answers, for different reasons, so both are
    // refused — with different wording, because they point at different
    // causes.
    if keyed < total {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}' pulled {} rows but the independent count \
             query reported {total}; refusing a possibly truncated universe rather than \
             computing a baseline over part of it. This can be a warehouse-side page cap, or \
             an inner join to a `require` member's view that has no matching row for some \
             entities — check {:?}",
            keyed, cohort_decl.require
        )));
    }
    if keyed > total {
        return Err(EngineError::QueryError(format!(
            "cohort '{cohort}' on entity '{entity}' pulled {} rows but there are only {total} \
             distinct '{entity}' values; the pull is not at entity grain, so some entities \
             appear more than once and would be counted more than once in every peer set. The \
             usual cause is a `require` member that is not entity-scoped (one value per \
             entity) and so fans the group-by out — check {:?}",
            keyed, cohort_decl.require
        )));
    }

    // Step 5: the peer loop. Polarity comes from the shared
    // `measure_direction` — a cohort's baseline and `opportunity`'s benchmark
    // must never disagree about which way a measure is "better", and there is
    // no result-shape reason to hold a second copy of that lookup.
    //
    // The direction is read under the measure's SOURCE name for the same
    // reason the cells are: `stores` declares no `wage_pct` to carry a
    // `direction:`, so looking an induced target up under its requested name
    // finds nothing and silently falls back to the `HigherIsBetter` default —
    // inverting the sign of every gap for the lower-is-better measures
    // (wage percentages, waste rates) cohorts mostly compare.
    let direction = measure_direction(layer, &measures.target.source);
    let (candidates, excluded) = parse_candidates(&rows, &key_member, &measures, cohort_decl);
    let subjects = match_peers(&candidates, cohort_decl, direction, statistic);

    Ok(PeerCohortResult {
        entity: entity.to_string(),
        cohort: cohort.to_string(),
        measure: measure.to_string(),
        statistic,
        period: (start.to_string(), end.to_string()),
        subjects,
        excluded,
    })
}

/// One entity-grain row, reduced to exactly what peer matching needs.
struct Candidate {
    /// The entity key value — the identity a peer set is reported in.
    key: String,
    /// The `require` values, in declaration order. Two candidates are
    /// exact-match compatible iff these are equal element-wise. Empty when
    /// the cohort declares no `require`, in which case every candidate
    /// trivially matches every other.
    require_tuple: Vec<String>,
    /// The band's normalised size, `band.measure / band.per` (or the raw
    /// `band.measure` when `per` is absent). `None` only when the cohort
    /// declares no band at all — a candidate that *should* have a norm but
    /// could not be given one is excluded, never carried with `None`.
    norm: Option<f64>,
    /// The subject's own value of the measure being compared.
    value: f64,
}

/// Split the entity-grain pull into comparable candidates and reported
/// exclusions.
///
/// Everything dropped here is dropped for a reason the caller can read, and
/// a dropped row is **not** a peer for anybody either: a subject whose
/// `require` value is NULL matches nothing exactly, and one whose band
/// divisor is zero has no position on the size axis at all. Silently keeping
/// either would contaminate other subjects' baselines.
///
/// Cells are read by each measure's `source` name — the alias the compiler
/// projected — while every reason string names its `requested` one, so a
/// reader is told about the measure they asked for, not the promotion source
/// they have never heard of.
fn parse_candidates(
    rows: &[Row],
    key_member: &str,
    measures: &CohortMeasures,
    cohort_decl: &Cohort,
) -> (Vec<Candidate>, Vec<ExcludedSubject>) {
    let mut candidates = Vec::with_capacity(rows.len());
    let mut excluded = Vec::new();

    for r in rows {
        let Some(key) = row_str(r, key_member) else {
            excluded.push(ExcludedSubject {
                key: "(null)".to_string(),
                reason: format!(
                    "the entity key '{key_member}' is null or absent in the pulled row, so this \
                     row has no identity to report a peer set under"
                ),
            });
            continue;
        };

        // 1. The exact-match tuple. A null here is the spec's named case: it
        //    joins nothing, so it is reported rather than silently vanishing.
        let mut require_tuple = Vec::with_capacity(cohort_decl.require.len());
        let mut null_require = None;
        for req in &cohort_decl.require {
            match row_str(r, req) {
                Some(v) => require_tuple.push(v),
                None => {
                    null_require = Some(req.clone());
                    break;
                }
            }
        }
        if let Some(req) = null_require {
            excluded.push(ExcludedSubject {
                key,
                reason: format!(
                    "required dimension '{req}' is null, so this subject matches no peer \
                     exactly and is excluded from every peer set (including its own)"
                ),
            });
            continue;
        }

        // 2. The value being compared. A null target is not zero — a subject
        //    with no readable value cannot be positioned against a baseline.
        let Some(value) = row_f64(r, &measures.target.source) else {
            excluded.push(ExcludedSubject {
                key,
                reason: format!(
                    "measure '{}' is null or unreadable for this subject; there is \
                     nothing to compare against a peer baseline",
                    measures.target.requested
                ),
            });
            continue;
        };

        // 3. The band position, normalised per subject.
        let norm = match &measures.band {
            None => None,
            Some(band) => {
                let Some(size) = row_f64(r, &band.measure.source) else {
                    excluded.push(ExcludedSubject {
                        key,
                        reason: format!(
                            "band measure '{}' is null or unreadable for this subject, so it \
                             has no position on the size axis the band compares",
                            band.measure.requested
                        ),
                    });
                    continue;
                };
                match &band.per {
                    None => Some(size),
                    Some(per) => match row_f64(r, &per.source) {
                        Some(d) if d != 0.0 => Some(size / d),
                        _ => {
                            excluded.push(ExcludedSubject {
                                key,
                                reason: format!(
                                    "band divisor '{}' is zero, null or unreadable for this \
                                     subject; '{}' cannot be normalised and the subject is \
                                     reported rather than divided by zero",
                                    per.requested, band.measure.requested
                                ),
                            });
                            continue;
                        }
                    },
                }
            }
        };

        candidates.push(Candidate {
            key,
            require_tuple,
            norm,
            value,
        });
    }

    (candidates, excluded)
}

/// The `O(n^2)` correlated peer loop: for each subject, the peers that match
/// its `require` tuple exactly and fall inside the band **centred on that
/// subject**.
///
/// This is deliberately not a bucketing, an `NTILE`, or any other symmetric
/// partition. Because each subject carries its own band, membership is
/// **non-reciprocal** — A can sit inside B's band while B sits outside A's —
/// and that asymmetry is the measured, accepted semantics of a cohort (see
/// [`crate::schema::models::CohortBand`]). Replacing this loop with a
/// partition would give every pair a single shared verdict and silently
/// change every answer.
fn match_peers(
    candidates: &[Candidate],
    cohort_decl: &Cohort,
    direction: MeasureDirection,
    statistic: BenchmarkStatistic,
) -> Vec<CohortSubject> {
    let tolerance = cohort_decl.band.as_ref().map(|b| b.tolerance);
    // Absent `min_peers` means "one peer is enough to have a baseline at
    // all", not "no floor" — a subject with zero peers has no baseline.
    let min_peers = cohort_decl.min_peers.unwrap_or(1);

    candidates
        .iter()
        .map(|subject| {
            let band = tolerance.zip(subject.norm).map(|(tol, norm)| {
                // Ordered rather than assumed: for a negative normalised
                // size, `norm * (1 + tol)` is the LOWER end.
                let (a, b) = (norm * (1.0 - tol), norm * (1.0 + tol));
                if a <= b {
                    (a, b)
                } else {
                    (b, a)
                }
            });

            let matched: Vec<&Candidate> = candidates
                .iter()
                .filter(|peer| {
                    if cohort_decl.exclude_self && peer.key == subject.key {
                        return false;
                    }
                    if peer.require_tuple != subject.require_tuple {
                        return false;
                    }
                    match (band, peer.norm) {
                        (Some((lo, hi)), Some(n)) => n >= lo && n <= hi,
                        // No band declared: exact match alone defines the
                        // cohort, so every require-matching candidate is a
                        // peer.
                        (None, _) => true,
                        // The subject HAS a band but this candidate has no
                        // position on it. Unreachable today —
                        // `parse_candidates` excludes any row it cannot
                        // normalise whenever a band is declared, so a
                        // candidate list never mixes the two — but spelled
                        // out rather than folded into a catch-all: the two
                        // arms above have opposite correct answers, and a
                        // future change that lets an unpositioned candidate
                        // through should refuse it loudly, not quietly admit
                        // it to every peer set.
                        (Some(_), None) => false,
                    }
                })
                .collect();

            let peer_count = matched.len();
            // No peers means no baseline. Reporting 0.0 as if it were one
            // would make an arbitrary gap out of nothing; `peer_count: 0` and
            // `sufficient: false` are what say so.
            let (baseline, gap) = if matched.is_empty() {
                (0.0, 0.0)
            } else {
                let values: Vec<f64> = matched.iter().map(|p| p.value).collect();
                let baseline = select_baseline(&values, direction, statistic);
                (
                    baseline,
                    polarity_aware_gap(subject.value, baseline, direction),
                )
            };

            CohortSubject {
                key: subject.key.clone(),
                value: subject.value,
                baseline,
                gap,
                peers: matched.into_iter().map(|p| p.key.clone()).collect(),
                peer_count,
                sufficient: peer_count >= min_peers,
            }
        })
        .collect()
}

/// The gap between a subject and its peer baseline, oriented so that
/// **positive always means opportunity** — the same convention `opportunity`
/// uses (`metric_tree_ops.rs`).
///
/// For a cost or defect rate (`LowerIsBetter`) a value *above* the peer
/// baseline is the thing to fix, so the subtraction flips. Every one of the
/// reference implementation's five cohort metrics is lower-is-better;
/// hardcoding the higher-is-better subtraction would invert all of them.
fn polarity_aware_gap(value: f64, baseline: f64, direction: MeasureDirection) -> f64 {
    match direction {
        MeasureDirection::HigherIsBetter => baseline - value,
        MeasureDirection::LowerIsBetter => value - baseline,
    }
}

/// The baseline statistic over a peer group's values.
///
/// Mirrors `select_benchmark` in `metric_tree_ops.rs` exactly, including its
/// direction handling: `P75` means "75% of the way toward better", which is
/// the raw 25th percentile for a `LowerIsBetter` measure, and `BestPeer` is
/// the max for `HigherIsBetter` and the min for `LowerIsBetter`. Duplicated
/// rather than called because `select_benchmark` is private to
/// `metric_tree_ops` and belongs to `opportunity`'s result shape (it also
/// returns a `basis` string a cohort has no use for); the arithmetic itself
/// is shared via [`quantile_r7`].
fn select_baseline(
    values: &[f64],
    direction: MeasureDirection,
    statistic: BenchmarkStatistic,
) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match statistic {
        BenchmarkStatistic::Median => quantile_r7(&sorted, 0.5),
        BenchmarkStatistic::P75 => {
            let q = match direction {
                MeasureDirection::HigherIsBetter => 0.75,
                MeasureDirection::LowerIsBetter => 0.25,
            };
            quantile_r7(&sorted, q)
        }
        BenchmarkStatistic::BestPeer => match direction {
            MeasureDirection::HigherIsBetter => *sorted.last().unwrap(),
            MeasureDirection::LowerIsBetter => *sorted.first().unwrap(),
        },
    }
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
    /// and the target measure.
    ///
    /// Keys are the compiled SQL **aliases** (`view.member` with the dot
    /// replaced by `__`), which is what a real executor returns — see
    /// [`member_alias`].
    fn entity_row(i: i64) -> Row {
        row(&[
            ("stores__store_id", json!(format!("store_{i}"))),
            ("stores__accounting_basis", json!("accrual")),
            ("sales__net_sales", json!(100_000.0 + i as f64)),
            ("sales__trading_days", json!(360.0)),
            ("sales__wage_pct", json!(0.25)),
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
        cohort_layer_from(
            r#"
        band:
          measure: sales.net_sales
          per: sales.trading_days
          tolerance: 0.35
        require: [stores.accounting_basis]
        min_peers: 3
"#,
            MeasureDirection::HigherIsBetter,
        )
    }

    /// [`cohort_test_layer`] with the target measure declared
    /// `direction: lower_is_better` — a wage percentage, like every one of
    /// the reference implementation's five cohort metrics.
    fn cohort_test_layer_lower_is_better() -> SemanticLayer {
        cohort_layer_from(
            r#"
        band:
          measure: sales.net_sales
          per: sales.trading_days
          tolerance: 0.35
        require: [stores.accounting_basis]
        min_peers: 3
"#,
            MeasureDirection::LowerIsBetter,
        )
    }

    /// The shared body of the fixtures above: `cohort_yaml` is spliced under
    /// `cohorts: size_matched:` (8-space indented), and `direction` is
    /// declared on the `sales.wage_pct` target measure.
    fn cohort_layer_from(cohort_yaml: &str, direction: MeasureDirection) -> SemanticLayer {
        let stores = format!(
            r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched:{cohort_yaml}
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: accounting_basis
    type: string
    expr: accounting_basis
"#
        );
        let direction_yaml = match direction {
            MeasureDirection::HigherIsBetter => "higher_is_better",
            MeasureDirection::LowerIsBetter => "lower_is_better",
        };
        let sales = format!(
            r#"
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
    direction: {direction_yaml}
"#
        );
        let parser = crate::schema::parser::SchemaParser::new();
        let mut layer = SemanticLayer::new(
            vec![
                parser.parse_view_str(&stores, "stores").unwrap(),
                parser.parse_view_str(&sales, "sales").unwrap(),
            ],
            None,
        );
        assert!(
            augment_layer_for_cohort(&mut layer, "store_id"),
            "fixture setup: augment_layer_for_cohort must succeed for store_id"
        );
        layer
    }

    /// One entity-grain row for the `cohort_test_layer` fixtures, keyed by
    /// compiled SQL alias: the store key, the `require`d
    /// `stores.accounting_basis` (matching for every subject unless a test
    /// says otherwise), the band's `measure`/`per` pair, and the target.
    fn subject_row(key: &str, band_measure: f64, per: f64, target: f64) -> Row {
        row(&[
            ("stores__store_id", json!(key)),
            ("stores__accounting_basis", json!("accrual")),
            ("sales__net_sales", json!(band_measure)),
            ("sales__trading_days", json!(per)),
            ("sales__wage_pct", json!(target)),
        ])
    }

    /// [`subject_row`] with raw JSON cells, for the shapes a real executor
    /// hands back that a plain `f64` argument cannot express — in particular
    /// the string numerics the REST executors (Snowflake, BigQuery,
    /// Databricks) return, which include `"NaN"` and `"inf"`.
    fn subject_row_raw(
        key: &str,
        band_measure: serde_json::Value,
        per: serde_json::Value,
        target: serde_json::Value,
    ) -> Row {
        row(&[
            ("stores__store_id", json!(key)),
            ("stores__accounting_basis", json!("accrual")),
            ("sales__net_sales", band_measure),
            ("sales__trading_days", per),
            ("sales__wage_pct", target),
        ])
    }

    /// [`subject_row`] with a SQL NULL in the `require`d dimension — the
    /// subject that can match nobody exactly and must be *reported*, not
    /// silently dropped.
    fn subject_row_with_null_basis(key: &str, band_measure: f64, per: f64, target: f64) -> Row {
        row(&[
            ("stores__store_id", json!(key)),
            ("stores__accounting_basis", serde_json::Value::Null),
            ("sales__net_sales", json!(band_measure)),
            ("sales__trading_days", json!(per)),
            ("sales__wage_pct", json!(target)),
        ])
    }

    /// [`subject_row`] with a SQL NULL in the ENTITY KEY — the shape a
    /// `GROUP BY key` emits for an orphaned fact row (one whose key has no
    /// matching row in the entity view, kept by the pull's LEFT JOIN). It is
    /// a row with no identity, not an extra entity.
    fn subject_row_with_null_key(band_measure: f64, per: f64, target: f64) -> Row {
        row(&[
            ("stores__store_id", serde_json::Value::Null),
            ("stores__accounting_basis", serde_json::Value::Null),
            ("sales__net_sales", json!(band_measure)),
            ("sales__trading_days", json!(per)),
            ("sales__wage_pct", json!(target)),
        ])
    }

    /// Run `resolve_cohort` against a canned executor that answers the
    /// entity-count query with `rows.len()` (so both guards pass) and the
    /// entity-grain pull with `rows`.
    fn resolve_with_rows(
        layer: &SemanticLayer,
        rows: Vec<Row>,
        statistic: BenchmarkStatistic,
    ) -> PeerCohortResult {
        try_resolve_measure_with_rows(layer, "sales.wage_pct", rows, statistic)
            .expect("the guards must pass: the canned count matches the canned pull")
    }

    /// [`resolve_with_rows`] for a caller-chosen target measure — the same
    /// canned executor, but the target name is the variable under test. The
    /// point is a name the caller asks for that is NOT the name the compiler
    /// projects: an induced measure (`stores.wage_pct`) arrives in the rows
    /// as its SOURCE alias (`sales__wage_pct`), which is exactly what
    /// [`subject_row`] builds.
    fn resolve_measure_with_rows(
        layer: &SemanticLayer,
        measure: &str,
        rows: Vec<Row>,
        statistic: BenchmarkStatistic,
    ) -> PeerCohortResult {
        try_resolve_measure_with_rows(layer, measure, rows, statistic)
            .expect("the guards must pass: the canned count matches the canned pull")
    }

    /// The fallible core of the two helpers above, for the cases that are
    /// supposed to be refused.
    fn try_resolve_measure_with_rows(
        layer: &SemanticLayer,
        measure: &str,
        rows: Vec<Row>,
        statistic: BenchmarkStatistic,
    ) -> Result<PeerCohortResult, EngineError> {
        let total = rows.len() as f64;
        let rows = std::sync::Arc::new(rows);
        let executor = move |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("stores____cohort_total__", json!(total))])]);
            }
            Ok((*rows).clone())
        };
        resolve_cohort(
            layer,
            "store_id",
            "size_matched",
            measure,
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            statistic,
            &executor,
        )
    }

    #[test]
    fn cohort_membership_is_not_reciprocal() {
        // THE property most likely to be "optimised" into a bucketing later.
        // tol=0.35, so a subject at X admits peers in [0.65X, 1.35X]:
        //   a=100 -> [65, 135]   contains c(130), excludes b(200)
        //   b=200 -> [130, 270]  contains c(130), EXCLUDES a(100)
        // c is a peer of both, a and b are not peers of each other, and the
        // relation is directional. Asserting only one side would also pass
        // against a bucketing implementation, which is the thing to catch.
        //
        // c sits exactly ON b's lower edge, and that is deliberate rather
        // than luck: `200.0 * (1.0 - 0.35)` is bit-exactly 130.0 in f64 (the
        // rounding error at that magnitude is below half an ULP), and the
        // band comparison is inclusive (`n >= lo && n <= hi`). A later switch
        // to a strict `>`, or to a differently-associated expression for the
        // edge (`200.0 - 200.0 * 0.35`, say), would break this test — which
        // is the point: it is pinning the edge convention, not just the
        // asymmetry.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 200.0, 1.0, 20.0),
            subject_row("c", 130.0, 1.0, 15.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        let b = res.subjects.iter().find(|s| s.key == "b").unwrap();
        assert!(
            a.peers.contains(&"c".to_string()),
            "c(130) is inside a(100)'s +-35% band"
        );
        assert!(
            !a.peers.contains(&"b".to_string()),
            "b(200) is outside a(100)'s band"
        );
        assert!(
            !b.peers.contains(&"a".to_string()),
            "a(100) is outside b(200)'s band"
        );
        assert!(
            b.peers.contains(&"c".to_string()),
            "c(130) is inside b(200)'s band"
        );
    }

    #[test]
    fn the_band_normalises_by_the_per_measure() {
        // The test that would have caught the `per: Day` mistake. Two entities
        // with EQUAL totals but different trading-day counts must land in
        // different bands — banding on the raw total conflates size with tenure.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("mature", 900.0, 90.0, 10.0), // 10/day
            subject_row("new", 900.0, 9.0, 12.0),     // 100/day
            subject_row("peer", 880.0, 88.0, 11.0),   // 10/day
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let mature = res.subjects.iter().find(|s| s.key == "mature").unwrap();
        assert!(mature.peers.contains(&"peer".to_string()));
        assert!(
            !mature.peers.contains(&"new".to_string()),
            "equal totals, 10x the daily rate — must not be peers"
        );
    }

    #[test]
    fn the_baseline_excludes_the_subject_itself() {
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 100.0), // wild outlier target value
            subject_row("b", 100.0, 1.0, 10.0),
            subject_row("c", 100.0, 1.0, 20.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        assert_eq!(a.peer_count, 2);
        assert!(
            (a.baseline - 15.0).abs() < 1e-9,
            "median of [10,20], not of [10,20,100]"
        );
    }

    #[test]
    fn r7_interpolates_an_even_sized_peer_set() {
        // The reference documented that upper-of-two-middle understated a real
        // gap by $933. Interpolation is not cosmetic.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("s", 100.0, 1.0, 50.0),
            subject_row("p1", 100.0, 1.0, 10.0),
            subject_row("p2", 100.0, 1.0, 20.0),
            subject_row("p3", 100.0, 1.0, 30.0),
            subject_row("p4", 100.0, 1.0, 44.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let s = res.subjects.iter().find(|s| s.key == "s").unwrap();
        assert_eq!(s.peer_count, 4);
        assert!(
            (s.baseline - 25.0).abs() < 1e-9,
            "R-7 of [10,20,30,44] is 25, not 30"
        );
    }

    #[test]
    fn a_thin_cohort_is_reported_not_dropped() {
        // min_peers is a REPORTING predicate, never a gate. The reference moved
        // its guards out of the WHERE into a reported column precisely because a
        // store "simply did not appear in the labor list and no screen said why".
        let layer = cohort_test_layer(); // min_peers: 3
        let rows = vec![
            subject_row("lonely", 100.0, 1.0, 10.0),
            subject_row("friend", 105.0, 1.0, 12.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let lonely = res.subjects.iter().find(|s| s.key == "lonely").unwrap();
        assert_eq!(lonely.peer_count, 1);
        assert!(!lonely.sufficient);
        assert!(
            !res.excluded.iter().any(|e| e.key == "lonely"),
            "a thin cohort is insufficient, not excluded — it must not appear in both"
        );
    }

    #[test]
    fn a_null_require_value_is_excluded_and_reported() {
        let layer = cohort_test_layer();
        let mut rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 12.0),
        ];
        rows.push(subject_row_with_null_basis("orphan", 100.0, 1.0, 15.0));
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        assert!(!res.subjects.iter().any(|s| s.key == "orphan"));
        let ex = res
            .excluded
            .iter()
            .find(|e| e.key == "orphan")
            .expect("reported, not vanished");
        assert!(
            ex.reason.contains("accounting_basis"),
            "reason should name which required dimension was null, got: {}",
            ex.reason
        );
        // And it contributes to nobody's baseline.
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        assert!(!a.peers.contains(&"orphan".to_string()));
    }

    #[test]
    fn the_gap_is_polarity_aware() {
        // All five reference cohort metrics are lower-is-better. Positive gap
        // means opportunity in BOTH directions.
        let layer = cohort_test_layer_lower_is_better();
        let rows = vec![
            subject_row("spendy", 100.0, 1.0, 30.0),
            subject_row("p1", 100.0, 1.0, 10.0),
            subject_row("p2", 100.0, 1.0, 20.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let s = res.subjects.iter().find(|s| s.key == "spendy").unwrap();
        assert!((s.baseline - 15.0).abs() < 1e-9);
        assert!(
            s.gap > 0.0,
            "a cost 2x the peer median is an opportunity, not a credit"
        );
        assert!((s.gap - 15.0).abs() < 1e-9);
    }

    #[test]
    fn the_gap_inverts_with_the_declared_polarity() {
        // The mirror of the test above, on the SAME numbers: with the default
        // higher-is-better polarity a value above the peer median is ahead,
        // so the gap is negative. Without this pair, an implementation that
        // ignores `direction` entirely still passes one of them.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("ahead", 100.0, 1.0, 30.0),
            subject_row("p1", 100.0, 1.0, 10.0),
            subject_row("p2", 100.0, 1.0, 20.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        let s = res.subjects.iter().find(|s| s.key == "ahead").unwrap();
        assert!((s.baseline - 15.0).abs() < 1e-9);
        assert!((s.gap - -15.0).abs() < 1e-9, "got gap {}", s.gap);
    }

    #[test]
    fn best_peer_follows_the_measures_declared_direction() {
        // `best_peer` is the one statistic whose answer is a different END of
        // the sorted values per polarity. Mirrors `select_benchmark`: max for
        // higher-is-better, min for lower-is-better.
        let rows = || {
            vec![
                subject_row("s", 100.0, 1.0, 50.0),
                subject_row("p1", 100.0, 1.0, 10.0),
                subject_row("p2", 100.0, 1.0, 20.0),
            ]
        };
        let higher = resolve_with_rows(&cohort_test_layer(), rows(), BenchmarkStatistic::BestPeer);
        let s = higher.subjects.iter().find(|s| s.key == "s").unwrap();
        assert!(
            (s.baseline - 20.0).abs() < 1e-9,
            "higher-is-better: the best peer is the max, got {}",
            s.baseline
        );

        let lower = resolve_with_rows(
            &cohort_test_layer_lower_is_better(),
            rows(),
            BenchmarkStatistic::BestPeer,
        );
        let s = lower.subjects.iter().find(|s| s.key == "s").unwrap();
        assert!(
            (s.baseline - 10.0).abs() < 1e-9,
            "lower-is-better: the best peer is the min, got {}",
            s.baseline
        );
    }

    #[test]
    fn a_zero_band_divisor_is_excluded_not_divided_by() {
        // A store with no trading days in the window cannot be normalised.
        // Dividing anyway yields an infinity that silently swallows or
        // excludes every peer depending on the sign.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 12.0),
            subject_row("dark", 100.0, 0.0, 15.0),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        assert!(!res.subjects.iter().any(|s| s.key == "dark"));
        let ex = res
            .excluded
            .iter()
            .find(|e| e.key == "dark")
            .expect("reported, not vanished");
        assert!(
            ex.reason.contains("sales.trading_days"),
            "the reason must name the divisor, got: {}",
            ex.reason
        );
        assert!(
            !res.subjects
                .iter()
                .any(|s| s.peers.contains(&"dark".to_string())),
            "an un-normalisable subject is nobody's peer either"
        );
    }

    /// [`cohort_test_layer`] with no `band:` at all — the exact-match-only
    /// cohort the schema explicitly supports (`Cohort.band` is optional).
    /// Every candidate sharing the `require` tuple is a peer, so nothing
    /// filters a subject out on numeric grounds.
    fn cohort_test_layer_exact_match_only() -> SemanticLayer {
        cohort_layer_from(
            r#"
        require: [stores.accounting_basis]
        min_peers: 3
"#,
            MeasureDirection::HigherIsBetter,
        )
    }

    #[test]
    fn a_non_finite_cell_is_excluded_not_read() {
        // The REST executors (Snowflake, BigQuery, Databricks) return
        // numerics as JSON strings, and `"NaN"`/`"inf"` both parse as f64 —
        // so this is a live path, not a hypothetical.
        //
        // A NaN that reaches the peer loop is worse than a null. It passes
        // the `d != 0.0` divisor guard (`NaN != 0.0` is true), so the band
        // becomes `(NaN, NaN)`, every `>= lo && <= hi` comparison is false,
        // and the subject comes back with `peer_count: 0, gap: 0.0` —
        // silently uncomparable rather than excluded and reported, which
        // contradicts this module's own contract. An infinite divisor is the
        // mirror case: `100 / inf` is a perfectly finite `0.0`, so the
        // subject silently lands at the bottom of the size axis.
        //
        // Non-finite is "the warehouse did not give us a number", and belongs
        // in the same exclusion channels as a null.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 20.0),
            subject_row_raw("nan_target", json!(100.0), json!(1.0), json!("NaN")),
            subject_row_raw("inf_per", json!(100.0), json!("inf"), json!(15.0)),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        for key in ["nan_target", "inf_per"] {
            assert!(
                !res.subjects.iter().any(|s| s.key == key),
                "{key} carries a non-finite cell and must not be compared, got {:?}",
                res.subjects.iter().map(|s| &s.key).collect::<Vec<_>>()
            );
            assert!(
                res.excluded.iter().any(|e| e.key == key),
                "{key} must be reported as excluded, not silently uncomparable"
            );
            assert!(
                !res.subjects
                    .iter()
                    .any(|s| s.peers.contains(&key.to_string())),
                "{key} must be nobody's peer either"
            );
        }
        // And the comparable subjects are untouched by them.
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        assert_eq!(a.peer_count, 1, "only b remains a peer of a");
        assert!((a.baseline - 20.0).abs() < 1e-9, "got {}", a.baseline);
    }

    #[test]
    fn a_non_finite_value_cannot_become_everyones_peer() {
        // The exact-match-only shape, where a NaN does the most damage: with
        // no band there is no numeric filter at all, so a NaN-valued subject
        // is a peer of everyone sharing its `require` tuple. Its value then
        // reaches the baseline, where `sort_by(partial_cmp().unwrap_or(Equal))`
        // leaves the NaN at an arbitrary position and `quantile_r7` can hand
        // back a NaN baseline for entities that have nothing wrong with them.
        let layer = cohort_test_layer_exact_match_only();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 20.0),
            subject_row_raw("nan_target", json!(100.0), json!(1.0), json!("NaN")),
        ];
        let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
        assert!(res.excluded.iter().any(|e| e.key == "nan_target"));
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        assert!(
            !a.peers.contains(&"nan_target".to_string()),
            "an unreadable subject is nobody's peer, band or no band"
        );
        assert!(
            a.baseline.is_finite() && (a.baseline - 20.0).abs() < 1e-9,
            "the baseline must be the median of the readable peers, got {}",
            a.baseline
        );
    }

    #[test]
    fn the_result_names_the_cohort_actually_used() {
        // The self-describing result is what closes the "screen says one thing,
        // query did another" bug class. Never omit it.
        let layer = cohort_test_layer();
        let res = resolve_with_rows(
            &layer,
            vec![subject_row("a", 100.0, 1.0, 10.0)],
            BenchmarkStatistic::Median,
        );
        assert_eq!(res.cohort, "size_matched");
        assert_eq!(res.entity, "store_id");
        assert_eq!(res.statistic, BenchmarkStatistic::Median);
    }

    /// A layer whose cohort makes the pull's member lists contain
    /// NON-ADJACENT duplicates, which is what `Vec::dedup` misses:
    /// `require` names the key dimension *after* another dimension, and the
    /// band's `per` is the same measure the caller is comparing.
    fn cohort_test_layer_require_repeats_key() -> SemanticLayer {
        cohort_layer_from(
            r#"
        band:
          measure: sales.trading_days
          per: sales.net_sales
          tolerance: 0.35
        require: [stores.accounting_basis, stores.store_id]
        min_peers: 3
"#,
            MeasureDirection::HigherIsBetter,
        )
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
                return Ok(vec![row(&[("stores____cohort_total__", json!(4_000.0))])]);
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
                return Ok(vec![row(&[("stores____cohort_total__", json!(2.0))])]);
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
            limits.contains(&Some(crate::engine::UNBOUNDED_QUERY_LIMIT)),
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
                return Ok(vec![row(&[(
                    "stores____cohort_total__",
                    json!(2_000_000.0),
                )])]);
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
    fn resolve_cohort_refuses_a_fanned_out_pull() {
        // The mirror image of the truncation case, and just as wrong: the
        // pull came back with MORE rows than there are distinct entities, so
        // some entity appears twice and would be counted twice in the peer
        // loop (once as a subject, twice in everyone's baseline). The usual
        // cause is a `require` member that is not entity-scoped — one value
        // per entity — so the group-by fanned out. Name that cause in the
        // message; "5 != 2" on its own sends the reader nowhere.
        let layer = cohort_test_layer();
        let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("stores____cohort_total__", json!(2.0))])]);
            }
            Ok((0..5).map(entity_row).collect())
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
        assert!(msg.contains('5') && msg.contains('2'), "got: {msg}");
        assert!(
            msg.contains("require"),
            "the message must name the likely cause (a non-entity-scoped `require` member), got: {msg}"
        );
    }

    #[test]
    fn resolve_cohort_refuses_an_unreadable_entity_count() {
        // The count is the ONLY thing standing between a truncated pull and a
        // baseline over an arbitrary slice. Silently reading a missing or
        // unparseable count as 0 turns the guard into a coin flip, so it gets
        // its own refusal rather than falling through to the cross-check with
        // a fabricated zero.
        let layer = cohort_test_layer();
        let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("some_other_column", json!("n/a"))])]);
            }
            Ok(vec![entity_row(0)])
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
        assert!(
            msg.contains("no readable value"),
            "an unreadable count must refuse on its own terms, not fall through \
             to the truncation cross-check with a fabricated 0; got: {msg}"
        );
    }

    #[test]
    fn the_entity_grain_pull_requests_each_member_once() {
        // `Vec::dedup` only collapses ADJACENT duplicates. When a cohort's
        // `require` names the key dimension itself (or the band's `measure`
        // equals its `per`, or the target), a duplicate separated by another
        // element survives into the QueryRequest — a member requested twice
        // is at best a confusing compiled SELECT and at worst a duplicate
        // alias.
        let layer = cohort_test_layer_require_repeats_key();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_inner = std::sync::Arc::clone(&seen);
        let executor = move |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("stores____cohort_total__", json!(1.0))])]);
            }
            seen_inner
                .lock()
                .unwrap()
                .push((q.dimensions.clone(), q.measures.clone()));
            Ok(vec![entity_row(0)])
        };
        let _ = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "sales.net_sales",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        );
        let seen = seen.lock().unwrap();
        let (dims, measures) = seen.first().expect("the pull query must have run");
        let mut sorted_dims = dims.clone();
        sorted_dims.sort();
        sorted_dims.dedup();
        assert_eq!(
            sorted_dims.len(),
            dims.len(),
            "duplicate dimension: {dims:?}"
        );
        let mut sorted_measures = measures.clone();
        sorted_measures.sort();
        sorted_measures.dedup();
        assert_eq!(
            sorted_measures.len(),
            measures.len(),
            "duplicate measure: {measures:?}"
        );
    }

    #[test]
    fn a_null_key_row_is_excluded_not_mistaken_for_a_fan_out() {
        // The entity-grain pull's join to the entity view is a LEFT JOIN (no
        // `ManyToOne` hop compiles to INNER), and `pick_base_view` puts the
        // FACT view on the left — it owns every measure the pull names. So an
        // orphaned fact row (a key with no row in the entity view) survives
        // the join and `GROUP BY key` emits it as one NULL-key group. The
        // independent `COUNT(DISTINCT key)` skips NULLs, so comparing the raw
        // `rows.len()` against it makes a single orphaned fact row look like
        // a fanned-out pull and refuses the whole cohort — blaming a
        // `require` member that is blameless.
        //
        // A NULL-key row is not an entity: it is partitioned off BEFORE the
        // cross-check and reported through the exclusion channel that already
        // exists for it.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 20.0),
            subject_row_with_null_key(100.0, 1.0, 99.0),
        ];
        let rows = std::sync::Arc::new(rows);
        // Two entities, three pulled rows: exactly the arithmetic that used
        // to trip the fan-out branch.
        let executor = move |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
                return Ok(vec![row(&[("stores____cohort_total__", json!(2.0))])]);
            }
            Ok((*rows).clone())
        };
        let res = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "sales.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .expect("one NULL-key row must not refuse the whole cohort");

        let ex = res
            .excluded
            .iter()
            .find(|e| e.key == "(null)")
            .unwrap_or_else(|| panic!("reported, not vanished; got {:?}", res.excluded));
        assert!(
            ex.reason.contains("stores.store_id"),
            "the reason must name the entity key member, got: {}",
            ex.reason
        );

        // And the keyed rows still resolve normally.
        let mut keys: Vec<&str> = res.subjects.iter().map(|s| s.key.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
        let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
        assert_eq!(a.peer_count, 1, "peers were {:?}", a.peers);
        assert!((a.baseline - 20.0).abs() < 1e-9, "got {}", a.baseline);
    }

    #[test]
    fn resolve_cohort_says_composite_key_when_that_is_the_cause() {
        // The runtime guard is defence-in-depth for a layer built
        // programmatically (bypassing the validator). Its message must name
        // the actual cause: "no single-column key resolvable to a declared
        // dimension" reads as "your dimension is missing" when the real
        // problem is that the key has two columns.
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    keys: [store_id, accounting_basis]
    cohorts:
      size_matched:
        require: [stores.accounting_basis]
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: accounting_basis
    type: string
    expr: accounting_basis
"#;
        let parser = crate::schema::parser::SchemaParser::new();
        let layer =
            SemanticLayer::new(vec![parser.parse_view_str(stores, "stores").unwrap()], None);
        let executor = |_: &QueryRequest| -> Result<Vec<Row>, EngineError> { Ok(vec![]) };
        let err = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "stores.anything",
            "stores.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("composite key"),
            "the message must say the key is composite, got: {msg}"
        );
    }

    // ── Induced (promoted) target measures ──────────────────────────────
    //
    // `stores.wage_pct` is INDUCED in `cohort_layer_from`'s fixture: `sales`
    // declares `store_id` as Foreign, `stores` declares it Primary, and
    // `Promotions::build` seeds its BFS from every Foreign entity with a
    // known Primary owner — no `parent:` needed for that first hop. So every
    // `sales` measure is queryable as `stores.<name>`.
    //
    // The compiler rewrites such a name to its source before generating SQL
    // and patches the original back onto `ColumnMeta` only — never onto the
    // row keys — so the cells arrive under `sales__wage_pct`. These two tests
    // pin both halves of reading them: the VALUE (which alias is read) and
    // the POLARITY (which measure's `direction:` is consulted). Fixing only
    // the first would return numbers with every sign inverted.

    #[test]
    fn an_induced_target_measure_is_read_by_its_source_alias() {
        // Rows keyed by the SOURCE alias (`sales__wage_pct`), which is what a
        // real executor hands back for a request naming `stores.wage_pct`.
        let layer = cohort_test_layer();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 10.0),
            subject_row("b", 100.0, 1.0, 20.0),
            subject_row("c", 100.0, 1.0, 30.0),
        ];
        let res =
            resolve_measure_with_rows(&layer, "stores.wage_pct", rows, BenchmarkStatistic::Median);

        assert!(
            res.excluded.is_empty(),
            "no subject is unreadable — the target resolves to sales.wage_pct: {:?}",
            res.excluded
        );
        let a = res
            .subjects
            .iter()
            .find(|s| s.key == "a")
            .expect("'a' must be compared");
        assert_eq!(
            a.value, 10.0,
            "the subject's own cell, read by source alias"
        );
        // All three sit at norm 100 and share a basis, so a's peers are b and
        // c: median of [20, 30] is 25 (R-7 of a 2-element set is the mean).
        assert_eq!(a.peer_count, 2, "peers were {:?}", a.peers);
        assert_eq!(a.baseline, 25.0);
        // `cohort_test_layer`'s wage_pct is higher_is_better.
        assert_eq!(a.gap, 15.0, "higher_is_better: baseline - value");

        // The self-describing half: the result reports what the CALLER asked
        // for, not the source name resolution happened to route through.
        assert_eq!(
            res.measure, "stores.wage_pct",
            "the result must name the requested measure, not its promotion source"
        );
    }

    #[test]
    fn an_induced_target_measure_keeps_its_source_polarity() {
        // The half a value-only test would miss. `sales.wage_pct` declares
        // `direction: lower_is_better`; looking the direction up under the
        // INDUCED name finds nothing on `stores` and falls back to the
        // `HigherIsBetter` default, which flips the sign of every gap.
        //
        // Subject a is WORSE than its peers (a higher wage percentage), so a
        // correct, polarity-aware gap is POSITIVE — positive always means
        // opportunity. Peers of a are b(10), c(20), d(40); median 20; a is
        // 30, so gap = 30 - 20 = +10. Under the default direction it would be
        // 20 - 30 = -10: same magnitude, wrong sign, and the store that most
        // needs attention sorts last.
        let layer = cohort_test_layer_lower_is_better();
        let rows = vec![
            subject_row("a", 100.0, 1.0, 30.0),
            subject_row("b", 100.0, 1.0, 10.0),
            subject_row("c", 100.0, 1.0, 20.0),
            subject_row("d", 100.0, 1.0, 40.0),
        ];
        let res =
            resolve_measure_with_rows(&layer, "stores.wage_pct", rows, BenchmarkStatistic::Median);

        let a = res
            .subjects
            .iter()
            .find(|s| s.key == "a")
            .expect("'a' must be compared");
        assert_eq!(a.value, 30.0);
        assert_eq!(a.peer_count, 3, "peers were {:?}", a.peers);
        assert_eq!(a.baseline, 20.0, "median of [10, 20, 40]");
        assert_eq!(
            a.gap, 10.0,
            "lower_is_better must survive promotion: a worse-than-peers subject has a \
             POSITIVE gap (value - baseline), not a negative one"
        );
    }

    #[test]
    fn an_ambiguous_induced_target_measure_is_refused_by_name() {
        // The same induced name reachable from two source views. A silent
        // pick would answer a question the caller did not ask; the refusal
        // names both candidates so they can qualify it themselves.
        let stores = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched:
        require: [stores.accounting_basis]
        min_peers: 1
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: accounting_basis
    type: string
    expr: accounting_basis
"#;
        let fact = |name: &str, table: &str| {
            format!(
                r#"
name: {name}
table: {table}
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
  - name: wage_pct
    type: sum
    expr: wage_pct
"#
            )
        };
        let parser = crate::schema::parser::SchemaParser::new();
        let mut layer = SemanticLayer::new(
            vec![
                parser.parse_view_str(stores, "stores").unwrap(),
                parser
                    .parse_view_str(&fact("sales", "sales_daily"), "sales")
                    .unwrap(),
                parser
                    .parse_view_str(&fact("returns", "returns_daily"), "returns")
                    .unwrap(),
            ],
            None,
        );
        assert!(augment_layer_for_cohort(&mut layer, "store_id"));

        // The executor must never be reached: an ambiguity is a question
        // about the schema, answerable without a round trip.
        let executor = |_: &QueryRequest| -> Result<Vec<Row>, EngineError> {
            panic!("the ambiguity must be refused before any query is executed")
        };
        let err = resolve_cohort(
            &layer,
            "store_id",
            "size_matched",
            "stores.wage_pct",
            "sales.sale_date",
            ("2024-01-01", "2024-12-31"),
            BenchmarkStatistic::Median,
            &executor,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("stores.wage_pct") && msg.contains("sales") && msg.contains("returns"),
            "the refusal must name the induced measure and both candidate sources, got: {msg}"
        );
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
