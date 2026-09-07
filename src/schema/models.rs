use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// Entity type: primary (owns the key) or foreign (references another view's entity).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum EntityType {
    #[default]
    Primary,
    Foreign,
}

/// Lifespan declaration on an entity: the columns (or aggregate expressions)
/// marking when an entity row became active and (optionally) when it ceased.
/// Declared once per entity, it lets the compiler derive cohort membership for
/// time-shifted comparisons (e.g. same-store sales) with no per-query
/// arithmetic.
///
/// Two forms:
///
/// **Direct columns** — `start`/`end` are columns on the entity's owning view.
/// ```yaml
/// entities:
///   - name: store_id
///     lifespan:
///       start: opened_at   # column on stores
///       end: closed_at     # column on stores; null = still active
/// ```
///
/// **Derived** (`from:` set) — the engine emits a CTE that groups the named
/// view by the entity's key and exposes `start`/`end` as aggregates. Useful
/// when the entity table doesn't carry open/close columns and the lifespan
/// must be inferred from activity in another view (e.g. transactions).
/// ```yaml
/// entities:
///   - name: store_id
///     lifespan:
///       from: sales              # view to derive from
///       start: MIN(sale_date)    # aggregate expression
///       end: MAX(sale_date)      # aggregate expression; null end = still active
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lifespan {
    /// Column name (direct form) or aggregate expression (derived form) for the
    /// start of the entity's active life.
    pub start: String,
    /// Column name (direct form) or aggregate expression (derived form) for the
    /// end of the entity's active life. NULL means the entity is still active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// View to derive the lifespan from. When set, the engine builds a
    /// `__lifespan_<entity>` CTE by grouping this view on the entity's key and
    /// evaluating `start`/`end` as aggregates. The named view must declare the
    /// same entity (its keys define the GROUP BY).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

/// An entity within a view. Entities drive automatic join generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub name: String,
    /// Entity type. Optional during parsing when inherits_from is set; resolved before use.
    #[serde(rename = "type", default)]
    pub entity_type: EntityType,
    #[serde(default)]
    pub description: Option<String>,
    /// Single key (simple FK/PK).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Composite keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// Lifespan columns (start/end of the entity's active life). Powers
    /// cohort derivation for `shift` measures with `comparable_by`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifespan: Option<Lifespan>,
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
    /// Names this entity's *parent* in the dimensional hierarchy: a coarser-
    /// grain entity that this one rolls up to. Intrinsic to the entity, so
    /// it belongs on the Primary declaration (the place where the entity is
    /// defined). Foreign declarations are usages and should leave this unset
    /// — the validator rejects `parent:` on a Foreign entity.
    ///
    /// The chain transitively defines the rollup graph: any measure declared
    /// at this entity's grain is induced at every ancestor's grain. Direction
    /// is unambiguous because `parent:` is directional; even when both views
    /// over-declare Foreign/Primary symmetrically, only the side that names a
    /// parent participates in the hierarchy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}

impl Entity {
    /// All keys for this entity (handles single key vs composite).
    pub fn get_keys(&self) -> Vec<String> {
        if let Some(ref keys) = self.keys {
            keys.clone()
        } else if let Some(ref key) = self.key {
            vec![key.clone()]
        } else {
            vec![]
        }
    }

    pub fn is_composite(&self) -> bool {
        self.keys.as_ref().map(|k| k.len() > 1).unwrap_or(false)
    }
}

/// Dimension data types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DimensionType {
    String,
    Number,
    Date,
    Datetime,
    Boolean,
    Geo,
}

impl std::fmt::Display for DimensionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DimensionType::String => write!(f, "string"),
            DimensionType::Number => write!(f, "number"),
            DimensionType::Date => write!(f, "date"),
            DimensionType::Datetime => write!(f, "datetime"),
            DimensionType::Boolean => write!(f, "boolean"),
            DimensionType::Geo => write!(f, "geo"),
        }
    }
}

/// A dimension (attribute/column) within a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dimension_type: DimensionType,
    #[serde(default)]
    pub description: Option<String>,
    /// SQL expression for this dimension.
    pub expr: String,
    /// Original expression before variable encoding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_expr: Option<String>,
    /// Example values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<String>>,
    /// Alternative names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<Vec<String>>,
    /// Whether this dimension is a primary key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<bool>,
    /// Whether this dimension is a subquery dimension.
    /// When true, the expr references a measure from a related view,
    /// compiled as a correlated subquery.
    #[serde(default)]
    pub sub_query: Option<bool>,
    /// Whether this dimension may be used as a segment in automated analysis
    /// (currently `opportunity()`). Defaults to true.
    ///
    /// Set `false` for a dimension that is queryable and meaningful but is not
    /// a *lever* — something a human could never act on to move a measure.
    /// Three recurring cases:
    ///
    /// - descriptive noise with no owner (`address_line_2`, `postal_code`);
    /// - attributes it would be inappropriate to frame as upside (`gender`);
    /// - numeric columns that back a measure (`total_amount`), where grouping
    ///   the measure by itself is circular.
    ///
    /// This is about actionability, not cardinality — the cardinality cap
    /// already prunes wide identifier columns, but only *after* paying for the
    /// warehouse aggregate. Marking a dimension unsegmentable prunes it before
    /// the query is issued, so it also saves real money on wide views.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segmentable: Option<bool>,
    /// See [`DimensionAnalysis`]. Supersedes `segmentable` when both are set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis: Option<DimensionAnalysis>,
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

/// What a dimension may be *used for* in analysis, beyond plain grouping.
///
/// Grouping is not represented: a dimension that cannot be grouped by is not a
/// dimension. These are the two analytical uses that are separately valid, and
/// they must stay separate — benchmarking across `party_size` is invalid (a
/// 6-top outspends a 2-top by arithmetic), while splitting an observed drop by
/// it is legitimate. One flag serving both silently breaks the second.
///
/// Both fields default to `true`, so a typo in a key name would otherwise
/// deserialize to an all-permissive block indistinguishable from omitting
/// `analysis` entirely — `analysis: {explan: true}` would silently keep
/// benchmarking a dimension the modeller meant to exclude. `deny_unknown_fields`
/// turns that into a parse error naming the bad key.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DimensionAnalysis {
    /// May be used to decompose an observed change or gap (`explain`, `drill`).
    #[serde(default = "default_true")]
    pub explain: bool,
    /// May be *benchmarked across* — two segments held to the same standard
    /// (`opportunity`'s scan).
    #[serde(default = "default_true")]
    pub benchmark: bool,
}

fn default_true() -> bool {
    true
}

impl Default for DimensionAnalysis {
    fn default() -> Self {
        Self {
            explain: true,
            benchmark: true,
        }
    }
}

impl Dimension {
    /// Resolve this dimension's analysis capabilities, honouring the deprecated
    /// `segmentable` alias.
    ///
    /// `segmentable: false` means both capabilities off — the alias predates
    /// the split and its only consumer, `discover_dimensions`, sits upstream of
    /// every analysis call site.
    ///
    /// `analysis` wins when both are present, and that is enforced by every
    /// consumer reading capabilities through THIS method rather than testing
    /// `segmentable` directly. A call site that short-circuits on
    /// `segmentable == Some(false)` on its own silently reinstates the alias's
    /// precedence and contradicts the validator's deprecation warning.
    pub fn analysis_caps(&self) -> DimensionAnalysis {
        if let Some(a) = self.analysis {
            return a;
        }
        if self.segmentable == Some(false) {
            return DimensionAnalysis {
                explain: false,
                benchmark: false,
            };
        }
        DimensionAnalysis::default()
    }
}

/// Measure aggregation types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MeasureType {
    Count,
    Sum,
    Average,
    Min,
    Max,
    CountDistinct,
    CountDistinctApprox,
    Median,
    Custom,
    /// Pass-through measure — expression is used as-is (already contains aggregation).
    Number,
}

impl MeasureType {
    /// Map to the SQL aggregate function name.
    pub fn sql_function(&self) -> &str {
        match self {
            MeasureType::Count => "COUNT",
            MeasureType::Sum => "SUM",
            MeasureType::Average => "AVG",
            MeasureType::Min => "MIN",
            MeasureType::Max => "MAX",
            MeasureType::CountDistinct => "COUNT_DISTINCT",
            MeasureType::CountDistinctApprox => "COUNT_DISTINCT_APPROX",
            MeasureType::Median => "PERCENTILE_CONT",
            MeasureType::Custom => "CUSTOM",
            MeasureType::Number => "NUMBER",
        }
    }

    /// Whether this is a pass-through type (no wrapping aggregate function).
    pub fn is_passthrough(&self) -> bool {
        matches!(self, MeasureType::Custom | MeasureType::Number)
    }

    /// Whether a pre-aggregation rollup storing this measure type must add its
    /// raw expression column to the build-time `GROUP BY` (see
    /// `generate_build_sql`'s `extra_group_cols`), making the table's on-disk
    /// grain finer than its declared dimension set. `matches_exact_grain` in
    /// `preagg.rs` vetoes the GROUP-BY-skipping passthrough whenever a rollup
    /// stores any measure of this kind, so this is the single source of truth
    /// both call sites must agree with.
    pub fn adds_raw_group_column(&self) -> bool {
        matches!(
            self,
            MeasureType::CountDistinct | MeasureType::CountDistinctApprox | MeasureType::Median
        )
    }

    /// Parse the lowercase type name emitted by `Display` (the form stored in
    /// pre-aggregation manifest JSON). Inverse of `Display`; keep both in sync.
    pub fn from_type_name(name: &str) -> Option<MeasureType> {
        Some(match name {
            "count" => MeasureType::Count,
            "sum" => MeasureType::Sum,
            "average" => MeasureType::Average,
            "min" => MeasureType::Min,
            "max" => MeasureType::Max,
            "count_distinct" => MeasureType::CountDistinct,
            "count_distinct_approx" => MeasureType::CountDistinctApprox,
            "median" => MeasureType::Median,
            "custom" => MeasureType::Custom,
            "number" => MeasureType::Number,
            _ => return None,
        })
    }

    /// How this measure behaves under promotion (aggregation up a many-to-one
    /// chain to a coarser grain).
    pub fn additivity_class(&self) -> AdditivityClass {
        match self {
            // Re-foldable: aggregating an already-aggregated intermediate
            // gives the same answer as aggregating the source rows directly.
            MeasureType::Sum | MeasureType::Count | MeasureType::Min | MeasureType::Max => {
                AdditivityClass::Additive
            }
            // Must recompute from source-grain rows; re-folding an
            // intermediate value silently changes the answer.
            MeasureType::Average
            | MeasureType::CountDistinct
            | MeasureType::CountDistinctApprox
            | MeasureType::Median => AdditivityClass::NonAdditive,
            // Pass-through expressions embed {{view.measure}} references; the
            // referenced leaves are projected to the target grain and the
            // expression is re-evaluated there.
            MeasureType::Number | MeasureType::Custom => AdditivityClass::Passthrough,
        }
    }

    /// What this measure's value over a window IS, in terms of the per-row
    /// values a response is fitted against.
    ///
    /// `None` for the passthrough types: an expression's space depends on the
    /// expression, so only the metric tree — which knows the component edges —
    /// can resolve it. See [`crate::engine::metric_tree::MetricNode`].
    ///
    /// **Not [`MeasureType::additivity_class`]**, which answers a neighbouring
    /// but different question: whether an already-aggregated intermediate can
    /// be re-folded to a coarser grain. `min`/`max` are `Additive` there and
    /// deliberately `Unaggregatable` here — a window `MIN` is neither the sum
    /// of the per-row minima nor their mean.
    pub fn aggregate_space(&self) -> Option<AggregateSpace> {
        match self {
            MeasureType::Sum | MeasureType::Count => Some(AggregateSpace::Total),
            MeasureType::Average => Some(AggregateSpace::Mean),
            MeasureType::Min
            | MeasureType::Max
            | MeasureType::CountDistinct
            | MeasureType::CountDistinctApprox
            | MeasureType::Median => Some(AggregateSpace::Unaggregatable),
            MeasureType::Number | MeasureType::Custom => None,
        }
    }
}

/// What a measure's value over a window is, relative to the per-row values a
/// response was fitted against.
///
/// A fit is measured per row, and an identity-link response aggregates through
/// the basis moments — so what it produces is a change in the **sum** of the
/// target over those rows. Adding that to the target's window value is right
/// only when the window value IS that sum. This says whether it is.
///
/// The failure it exists to stop: `sales_per_guest → avg_order_value` fitted a
/// clean `coefficient 1.00` over n=2,005 rows, and a +15% lever moved the
/// target from 27.50 to 8.3k — the summed response, added to a mean, is out by
/// the row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateSpace {
    /// `SUM(y_i)` — `sum` and `count`, and expressions that add and subtract
    /// them. A summed response lands here unchanged.
    Total,
    /// `SUM(y_i) / n` — `average`, and expressions that add and subtract
    /// averages. A summed response has to be divided by the row count first.
    /// That division is exact against the mean of the rows the response was
    /// fitted over; against a window `AVG` computed from the underlying source
    /// rows it also assumes those fitted rows carry equal weight.
    Mean,
    /// Neither: `min`, `max`, `median`, `count_distinct`, and any expression
    /// that multiplies or divides — a ratio over a window is a ratio of two
    /// aggregates, not a fold of the per-row ratios. A **fitted** response
    /// cannot be carried onto these at all. A **declared** `coefficient:`
    /// still can: it states the effect on the aggregate directly, which is
    /// why this refuses rather than being a dead end.
    Unaggregatable,
}

/// How a measure can be aggregated up a promotion chain. Derived from
/// `MeasureType`; no per-measure annotation is required (or allowed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdditivityClass {
    /// SUM / COUNT / MIN / MAX — re-foldable. An intermediate aggregate can
    /// be re-aggregated to a coarser grain without recomputing from source.
    Additive,
    /// AVG / COUNT_DISTINCT / COUNT_DISTINCT_APPROX / MEDIAN — must be
    /// computed by aggregating source-grain rows directly to the requested
    /// target grain. Never re-fold an intermediate.
    NonAdditive,
    /// NUMBER / CUSTOM — expression-typed. Recurse into its `{{view.measure}}`
    /// references, project each leaf to the target grain, then re-evaluate
    /// the expression there.
    Passthrough,
}

impl std::fmt::Display for MeasureType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeasureType::Count => write!(f, "count"),
            MeasureType::Sum => write!(f, "sum"),
            MeasureType::Average => write!(f, "average"),
            MeasureType::Min => write!(f, "min"),
            MeasureType::Max => write!(f, "max"),
            MeasureType::CountDistinct => write!(f, "count_distinct"),
            MeasureType::CountDistinctApprox => write!(f, "count_distinct_approx"),
            MeasureType::Median => write!(f, "median"),
            MeasureType::Custom => write!(f, "custom"),
            MeasureType::Number => write!(f, "number"),
        }
    }
}

/// A filter condition on a measure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureFilter {
    pub expr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Rolling window configuration for cumulative/running measures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingWindow {
    /// Trailing interval (e.g., "7 days", "1 month", "unbounded").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trailing: Option<String>,
    /// Leading interval (e.g., "1 day", "unbounded").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leading: Option<String>,
    /// Offset (e.g., "start" or "end").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
}

/// A segment (predefined reusable filter) within a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub name: String,
    /// SQL boolean expression that defines this segment.
    pub expr: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

// ── Driver types (metric tree relationships) ────────────

/// Direction of a driver relationship.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DriverDirection {
    Positive,
    Negative,
    #[default]
    Unknown,
}

impl std::fmt::Display for DriverDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverDirection::Positive => write!(f, "positive"),
            DriverDirection::Negative => write!(f, "negative"),
            DriverDirection::Unknown => write!(f, "unknown"),
        }
    }
}

/// Strength of a driver relationship.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DriverStrength {
    Strong,
    #[default]
    Moderate,
    Weak,
}

impl std::fmt::Display for DriverStrength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverStrength::Strong => write!(f, "strong"),
            DriverStrength::Moderate => write!(f, "moderate"),
            DriverStrength::Weak => write!(f, "weak"),
        }
    }
}

/// Confidence level in a driver relationship.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DriverConfidence {
    High,
    #[default]
    Medium,
    Low,
}

impl std::fmt::Display for DriverConfidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverConfidence::High => write!(f, "high"),
            DriverConfidence::Medium => write!(f, "medium"),
            DriverConfidence::Low => write!(f, "low"),
        }
    }
}

/// Functional form of a quantitative driver relationship.
/// Describes the variable transformation used to model the relationship.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DriverForm {
    /// Y = a + bX (coefficient = unit change in Y per unit X)
    #[default]
    Linear,
    /// ln(Y) = a + b·ln(X) (coefficient = elasticity: % Y per % X)
    LogLog,
    /// ln(Y) = a + bX (coefficient = % change in Y per unit X)
    LogLinear,
    /// Y = a + b·ln(X) (coefficient = unit change in Y per % X, diminishing returns)
    LinearLog,
    /// Y = a + b₁X + b₂X² — the only shape here that can **turn around**, so it
    /// is the only one that can express "helps, then helps less, then hurts".
    ///
    /// Needs TWO coefficients, which is why `coefficients:` exists: the scalar
    /// `coefficient:` can only describe a shape that points one way for ever.
    /// Declare it as `coefficients: [slope, curvature]` with the curvature
    /// opposed in sign, or declare neither and let the fit measure both.
    Quadratic,
    /// Y = a + b₁X + b₂X² + b₃X³ — the S-curve. A quadratic can only bend once,
    /// so it cannot say "slow to start, then steep, then flattening"; a cubic
    /// can, at the cost of a third coefficient and far worse extrapolation.
    Cubic,
    /// Y = a + b·√X — diminishing returns that, unlike `linear-log`, is defined
    /// AT zero. A driver that spends real days at zero keeps those rows here
    /// instead of having them dropped for want of a logarithm.
    Sqrt,
    /// Y = a + b/X — a ceiling the response approaches and never crosses. The
    /// only shape here with a horizontal asymptote, which is what a capacity
    /// limit actually looks like; `linear-log` keeps climbing for ever.
    Inverse,
    /// Y = a + b₁·ln(X) + b₂·ln(X)² — saturating AND able to turn. `quadratic`
    /// turns on the level scale, so its peak sits at an absolute value of the
    /// driver; this one turns on the multiplicative scale, where a peak sits at
    /// a RATIO. That is the right scale for spend-like drivers.
    ///
    /// Named for its link first, like `linear-log`: the target enters linearly
    /// (`linear-`) and the driver enters as a quadratic in its log
    /// (`-log-quadratic`). It is NOT `log-quadratic`, which by that same reading
    /// would mean a log-linked target — a shape that cannot be honoured exactly
    /// on an aggregate lever.
    LinearLogQuadratic,
}

impl std::fmt::Display for DriverForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverForm::Linear => write!(f, "linear"),
            DriverForm::LogLog => write!(f, "log-log"),
            DriverForm::LogLinear => write!(f, "log-linear"),
            DriverForm::LinearLog => write!(f, "linear-log"),
            DriverForm::Quadratic => write!(f, "quadratic"),
            DriverForm::Cubic => write!(f, "cubic"),
            DriverForm::Sqrt => write!(f, "sqrt"),
            DriverForm::Inverse => write!(f, "inverse"),
            DriverForm::LinearLogQuadratic => write!(f, "linear-log-quadratic"),
        }
    }
}

/// A driver relationship: a measure that influences this measure's value.
///
/// Supports two mutually exclusive modes:
/// - **Qualitative**: `direction` + `strength` + `confidence` (domain knowledge, no numbers)
/// - **Quantitative**: `coefficient` + `form` + optional `intercept` + optional `lag`
///
/// When `coefficient` is set, direction/strength are inferred from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Driver {
    /// Fully qualified measure reference (e.g., "marketing.ad_spend").
    pub measure: String,
    // -- Qualitative fields --
    /// Direction of the relationship.
    #[serde(default)]
    pub direction: DriverDirection,
    /// Strength of the relationship.
    #[serde(default)]
    pub strength: DriverStrength,
    /// Confidence in the relationship.
    #[serde(default)]
    pub confidence: DriverConfidence,
    // -- Quantitative fields --
    /// Marginal effect coefficient. Interpretation depends on `form`.
    ///
    /// Shorthand for a single-term `coefficients`, which is every form but
    /// `quadratic`. Kept because it is what every existing `.view.yml` writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coefficient: Option<f64>,
    /// One coefficient per basis term of `form`'s response — the general form of
    /// `coefficient`, and the only way to declare a shape needing more than one
    /// (a `quadratic` needs `[slope, curvature]`).
    ///
    /// Declaring both this and `coefficient` is an error rather than a precedence
    /// rule: a reader cannot tell which one the engine used, and silently picking
    /// is how the wrong shape gets forecast for months. See
    /// `Driver::response_coefficients`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coefficients: Option<Vec<f64>>,
    /// Functional form of the relationship — **optional**.
    ///
    /// Left out, the shape is measured from history alongside the magnitude
    /// (`response::INFERENCE_CANDIDATES`). Declared, it pins the shape and skips
    /// the search. `form:` is an optimization, not a prerequisite: a modeller
    /// should not have to know the functional form of a relationship in order to
    /// ask what it is.
    ///
    /// Declaring it buys three things — every row the other candidates would have
    /// had to drop (inference needs a row set valid for all of them), the shapes
    /// inference will not select because they cannot be aggregated exactly
    /// (`log-linear`), and a shape held fixed rather than re-chosen as the window
    /// moves.
    ///
    /// `None` is distinct from `Some(Linear)`: the first says "measure it", the
    /// second asserts a straight line and refuses anything else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<DriverForm>,
    /// Intercept term (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intercept: Option<f64>,
    /// Lag in days — how long before a change in this driver affects the target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    // -- Common fields --
    /// Description of the relationship.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Links to supporting research, experiments, or documentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refs: Option<Vec<String>>,
}

/// Why a declared driver's coefficients cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoefficientError {
    /// Both `coefficient:` and `coefficients:` were written.
    Both,
    /// The vector's length does not match the declared `form`'s basis.
    Width { declared: usize, expected: usize },
}

impl std::fmt::Display for CoefficientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoefficientError::Both => write!(
                f,
                "declares both `coefficient:` and `coefficients:` — write one, \
                 since which of them the forecast used would be unknowable"
            ),
            CoefficientError::Width { declared, expected } => write!(
                f,
                "declares {declared} coefficient(s) but this `form:` needs {expected}"
            ),
        }
    }
}

impl Driver {
    /// The coefficient vector this driver declares, in basis order.
    ///
    /// `Ok(None)` is the qualitative case — a direction with no magnitude, which
    /// the fit may later measure. The scalar and the vector are the same thing at
    /// width 1, so old YAML needs no migration; declaring both is refused rather
    /// than resolved by precedence, and a vector of the wrong width is refused
    /// rather than padded, because both silent repairs end in a shape nobody
    /// declared being forecast.
    pub fn response_coefficients(&self) -> Result<Option<Vec<f64>>, CoefficientError> {
        // An undeclared form has no width yet — the fit picks the shape. Declaring
        // coefficients without a form is therefore contradictory: the numbers have
        // no basis to belong to. Treated as width 1, so a lone scalar still works
        // (it can only ever mean a single-term shape) and a vector is refused.
        let expected = self.form.as_ref().map(|f| f.spec().width()).unwrap_or(1);
        match (self.coefficient, self.coefficients.as_ref()) {
            (Some(_), Some(_)) => Err(CoefficientError::Both),
            (None, None) => Ok(None),
            (Some(c), None) => {
                if expected == 1 {
                    Ok(Some(vec![c]))
                } else {
                    Err(CoefficientError::Width {
                        declared: 1,
                        expected,
                    })
                }
            }
            (None, Some(v)) => {
                if v.len() == expected {
                    Ok(Some(v.clone()))
                } else {
                    Err(CoefficientError::Width {
                        declared: v.len(),
                        expected,
                    })
                }
            }
        }
    }
}

// ── Shift types (time-shifted measure modifier) ─────────

/// Direction a `shift` re-evaluates its base measure relative to the query's
/// current time window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ShiftDirection {
    /// Earlier window (e.g. prior year). The default.
    #[default]
    Prior,
    /// Later window (e.g. next year).
    Next,
}

impl std::fmt::Display for ShiftDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShiftDirection::Prior => write!(f, "prior"),
            ShiftDirection::Next => write!(f, "next"),
        }
    }
}

/// A `shift` measure modifier: re-evaluates a base measure over a time-shifted
/// window, and can restrict the query to a lifespan-derived cohort.
///
/// ```yaml
/// measures:
///   - name: net_sales_prior
///     shift:
///       measure: net_sales          # base measure to re-evaluate
///       by: 1 year                  # "<int> <unit>"
///       direction: prior            # prior | next
///       comparable_by: store_id     # entity whose lifespan defines the cohort
///       maturity: 14 months         # optional honeymoon offset; default 0
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Shift {
    /// Base measure (in the same view) to re-evaluate at the shifted window.
    pub measure: String,
    /// Shift amount as an interval string, `"<int> <unit>"` (e.g. `"1 year"`).
    ///
    /// TODO(fiscal-calendar): accept a fiscal/retail calendar step (52/53-week,
    /// 4-4-5) here so QSR calendar-shifted comps can align on retail weeks
    /// rather than literal intervals. Not implemented in this version.
    pub by: String,
    /// Direction to shift the window. Defaults to `prior`.
    #[serde(default)]
    pub direction: ShiftDirection,
    /// When set, restrict the entire query to the cohort of entities that are
    /// live across both the current and shifted windows. Names the entity whose
    /// `lifespan` defines comparability (e.g. `store_id`). Enforced as a single
    /// query-level predicate so base and shifted measures see the identical set.
    /// Absent = plain period-over-period (no cohort).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparable_by: Option<String>,
    /// Optional extra offset pushing the required start-of-life earlier than the
    /// shifted window's start (the honeymoon offset). `"<int> <unit>"`; default 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maturity: Option<String>,
}

/// Which way is "better" for this measure.
///
/// `opportunity` sizes the gap between a segment and a benchmark. For revenue,
/// better is larger and the benchmark is a high performer; for a cost or defect
/// rate, better is smaller and the benchmark is a low one. Without this the
/// engine assumes higher-is-better everywhere, which selects the *cheapest*
/// segments of a cost metric and sizes their "upside" as the cost of becoming
/// average — inverted end to end.
///
/// Defaults to `HigherIsBetter`, which is the engine's historical behaviour. A
/// measure whose polarity nobody has stated is not evidence of either polarity,
/// so this is never inferred from the measure's name.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MeasureDirection {
    #[default]
    HigherIsBetter,
    LowerIsBetter,
}

fn is_higher_is_better(d: &MeasureDirection) -> bool {
    matches!(d, MeasureDirection::HigherIsBetter)
}

/// A measure (aggregation/metric) within a view.
///
/// Deserialization is hand-written (see below) so `type` can be omitted *only*
/// for `shift` measures (which carry no aggregation of their own); a plain
/// measure missing `type` is still rejected, as before.
#[derive(Debug, Clone, Serialize)]
pub struct Measure {
    pub name: String,
    #[serde(rename = "type")]
    pub measure_type: MeasureType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// SQL expression (optional for count).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_expr: Option<String>,
    /// Filters to apply when calculating this measure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<MeasureFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<Vec<String>>,
    /// Rolling window configuration for cumulative/running aggregations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_window: Option<RollingWindow>,
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
    /// Driver relationships: measures that influence this measure's value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drivers: Option<Vec<Driver>>,
    /// Time-shift modifier: re-evaluate a base measure over a shifted window,
    /// optionally restricted to a lifespan-derived cohort. When set, this measure
    /// is compiled via the multi-stage self-join path, not the normal aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shift: Option<Shift>,
    /// Which direction of movement is an improvement. See [`MeasureDirection`].
    #[serde(default, skip_serializing_if = "is_higher_is_better")]
    pub direction: MeasureDirection,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

impl<'de> Deserialize<'de> for Measure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Mirror of `Measure` with an optional `type`, so we can enforce the
        // shift-aware requirement after parsing.
        #[derive(Deserialize)]
        struct Repr {
            name: String,
            #[serde(rename = "type", default)]
            measure_type: Option<MeasureType>,
            #[serde(default)]
            description: Option<String>,
            #[serde(default)]
            expr: Option<String>,
            #[serde(default)]
            original_expr: Option<String>,
            #[serde(default)]
            filters: Option<Vec<MeasureFilter>>,
            #[serde(default)]
            samples: Option<Vec<String>>,
            #[serde(default)]
            synonyms: Option<Vec<String>>,
            #[serde(default)]
            rolling_window: Option<RollingWindow>,
            #[serde(default)]
            inherits_from: Option<String>,
            #[serde(default)]
            drivers: Option<Vec<Driver>>,
            #[serde(default)]
            shift: Option<Shift>,
            #[serde(default)]
            direction: MeasureDirection,
            #[serde(default)]
            meta: Option<HashMap<String, Vec<String>>>,
        }

        let r = Repr::deserialize(deserializer)?;
        // `type` is required for plain measures; `shift` measures may omit it
        // (they have no aggregation of their own → treated as a pass-through).
        let measure_type = match (r.measure_type, r.shift.is_some()) {
            (Some(t), _) => t,
            (None, true) => MeasureType::Number,
            (None, false) => {
                return Err(serde::de::Error::custom(format!(
                    "measure '{}' is missing required field `type`",
                    r.name
                )))
            }
        };

        Ok(Measure {
            name: r.name,
            measure_type,
            description: r.description,
            expr: r.expr,
            original_expr: r.original_expr,
            filters: r.filters,
            samples: r.samples,
            synonyms: r.synonyms,
            rolling_window: r.rolling_window,
            inherits_from: r.inherits_from,
            drivers: r.drivers,
            shift: r.shift,
            direction: r.direction,
            meta: r.meta,
        })
    }
}

/// Retrieval configuration for a topic.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicRetrievalConfig {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// A scalar filter value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicScalarFilter {
    pub value: serde_json::Value,
}

/// An array filter value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicArrayFilter {
    pub values: Vec<serde_json::Value>,
}

/// A date range filter value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDateRangeFilter {
    pub from: serde_json::Value,
    pub to: serde_json::Value,
}

/// Filter operator with embedded value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicFilterType {
    #[serde(rename = "eq")]
    Eq(TopicScalarFilter),
    #[serde(rename = "neq")]
    Neq(TopicScalarFilter),
    #[serde(rename = "gt")]
    Gt(TopicScalarFilter),
    #[serde(rename = "gte")]
    Gte(TopicScalarFilter),
    #[serde(rename = "lt")]
    Lt(TopicScalarFilter),
    #[serde(rename = "lte")]
    Lte(TopicScalarFilter),
    #[serde(rename = "in")]
    In(TopicArrayFilter),
    #[serde(rename = "not_in")]
    NotIn(TopicArrayFilter),
    #[serde(rename = "in_date_range")]
    InDateRange(TopicDateRangeFilter),
    #[serde(rename = "not_in_date_range")]
    NotInDateRange(TopicDateRangeFilter),
}

/// A filter on a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFilter {
    pub field: String,
    #[serde(flatten)]
    pub filter_type: TopicFilterType,
}

/// A topic groups related views for a business domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub views: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_view: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieval: Option<TopicRetrievalConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_filters: Option<Vec<TopicFilter>>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

// ── Motif types ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MotifKind {
    Builtin,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MotifConstraint {
    Numeric,
    Temporal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MotifParamType {
    Measure,
    Dimension,
    Number,
    String,
    Enum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MotifParam {
    #[serde(rename = "type")]
    pub param_type: MotifParamType,
    #[serde(default)]
    pub constraints: Vec<MotifConstraint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<std::string::String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MotifOutputColumn {
    pub name: std::string::String,
    pub expr: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Motif {
    pub name: std::string::String,
    #[serde(default)]
    pub description: Option<std::string::String>,
    #[serde(rename = "type", default = "default_motif_kind")]
    pub motif_kind: MotifKind,
    #[serde(default)]
    pub params: HashMap<std::string::String, MotifParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub returns: Option<std::string::String>,
    #[serde(default, alias = "adds")]
    pub outputs: Vec<MotifOutputColumn>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

fn default_motif_kind() -> MotifKind {
    MotifKind::Custom
}

// ── Saved query types ──────────────────────────────────

/// A step within a multi-step saved query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryStep {
    pub name: std::string::String,
    pub query: crate::engine::query::QueryRequest,
    #[serde(default)]
    pub description: Option<std::string::String>,
}

/// A parameter declaration for a saved query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryParam {
    #[serde(rename = "type")]
    pub param_type: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<std::string::String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<std::string::String>,
}

/// A saved query — either a single query (top-level fields) or a multi-step workflow (`steps`).
///
/// Single-step format (top-level query fields):
/// ```yaml
/// name: revenue_by_region
/// measures: [orders.total_revenue]
/// dimensions: [orders.region]
/// motif: contribution
/// ```
///
/// Multi-step format (`steps` array):
/// ```yaml
/// name: revenue_investigation
/// steps:
///   - name: trend
///     query: { measures: [orders.total_revenue], motif: trend }
///   - name: anomalies
///     query: { measures: [orders.total_revenue], motif: anomaly }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub name: std::string::String,
    #[serde(default)]
    pub description: Option<std::string::String>,
    #[serde(default)]
    pub params: HashMap<std::string::String, SavedQueryParam>,
    /// Multi-step queries have explicit steps.
    #[serde(default)]
    pub steps: Vec<SavedQueryStep>,
    /// Single-step queries have an inline query (flattened from top-level fields).
    #[serde(flatten, default)]
    pub query: Option<crate::engine::query::QueryRequest>,
    /// Source file path (set during parsing, not deserialized from YAML).
    #[serde(skip)]
    pub source_path: Option<std::path::PathBuf>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

impl SavedQuery {
    /// Returns the effective steps: either explicit steps, or a single step from the inline query.
    pub fn effective_steps(&self) -> Vec<SavedQueryStep> {
        if !self.steps.is_empty() {
            self.steps.clone()
        } else if let Some(ref q) = self.query {
            // Only treat as single-step if the inline query has actual content
            if !q.measures.is_empty() || !q.dimensions.is_empty() {
                vec![SavedQueryStep {
                    name: self.name.clone(),
                    query: q.clone(),
                    description: self.description.clone(),
                }]
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }
}

/// Determines when a pre-aggregation rollup should be rebuilt.
///
/// YAML representation (one key is set, the other is absent):
/// ```yaml
/// sql: "SELECT MAX(updated_at) FROM orders"
/// ```
/// or
/// ```yaml
/// every: "6h"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RefreshKey {
    /// Rebuild when this SQL returns a different value than at last build.
    Sql(String),
    /// Rebuild after this interval elapses (e.g. `"6h"`, `"1d"`, `"30m"`).
    Every(String),
}

impl serde::Serialize for RefreshKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(1))?;
        match self {
            RefreshKey::Sql(v) => map.serialize_entry("sql", v)?,
            RefreshKey::Every(v) => map.serialize_entry("every", v)?,
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for RefreshKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use std::collections::HashMap;
        let map: HashMap<String, String> = HashMap::deserialize(d)?;

        for key in map.keys() {
            if key != "sql" && key != "every" {
                return Err(serde::de::Error::custom(format!(
                    "refresh_key has unknown key `{key}`; only `sql` or `every` are allowed"
                )));
            }
        }

        let has_sql = map.contains_key("sql");
        let has_every = map.contains_key("every");

        match (has_sql, has_every) {
            (true, true) => Err(serde::de::Error::custom(
                "refresh_key must have exactly one of `sql` or `every`, not both",
            )),
            (true, false) => Ok(RefreshKey::Sql(map["sql"].clone())),
            (false, true) => Ok(RefreshKey::Every(map["every"].clone())),
            (false, false) => Err(serde::de::Error::custom(
                "refresh_key must have exactly one of `sql` or `every` keys",
            )),
        }
    }
}

/// A pre-aggregation rollup definition within a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAggregation {
    pub name: String,
    #[serde(default)]
    pub dimensions: Vec<String>,
    #[serde(default)]
    pub measures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_dimension: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key: Option<RefreshKey>,
}

/// A view in the semantic layer — the core unit of the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
    /// SQL dialect shortcut (e.g., "postgres", "bigquery").
    /// Used when no config.yml datasource mapping is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    /// Table reference (mutually exclusive with sql).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Custom SQL (mutually exclusive with table).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(default)]
    pub entities: Vec<Entity>,
    #[serde(default)]
    pub dimensions: Vec<Dimension>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<Measure>>,
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Pre-aggregation rollup definitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_aggregations: Option<Vec<PreAggregation>>,
    /// View-level refresh key — applies to all rollups unless a per-rollup key overrides it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key: Option<RefreshKey>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

impl View {
    /// Get the SQL source for this view (either table name or SQL subquery).
    pub fn source_sql(&self) -> String {
        if let Some(ref table) = self.table {
            table.clone()
        } else if let Some(ref sql) = self.sql {
            format!("({})", sql)
        } else {
            // Should be caught by validation
            String::new()
        }
    }

    /// Get primary key dimension names.
    pub fn primary_key_dimensions(&self) -> Vec<&str> {
        let mut pks: Vec<&str> = Vec::new();
        // Collect from entity keys
        for entity in &self.entities {
            if entity.entity_type == EntityType::Primary {
                for key in entity.get_keys() {
                    // Find the dimension with this name
                    if self.dimensions.iter().any(|d| d.name == key) {
                        pks.push(
                            self.dimensions
                                .iter()
                                .find(|d| d.name == key)
                                .map(|d| d.name.as_str())
                                .unwrap(),
                        );
                    }
                }
            }
        }
        pks.dedup();
        pks
    }

    /// All measures (returns empty slice if None).
    pub fn measures_list(&self) -> &[Measure] {
        self.measures.as_deref().unwrap_or(&[])
    }
}

/// The complete semantic layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLayer {
    pub views: Vec<View>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<Topic>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub motifs: Option<Vec<Motif>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saved_queries: Option<Vec<SavedQuery>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl SemanticLayer {
    pub fn new(views: Vec<View>, topics: Option<Vec<Topic>>) -> Self {
        Self {
            views,
            topics,
            motifs: None,
            saved_queries: None,
            metadata: None,
        }
    }

    pub fn with_motifs_and_queries(
        views: Vec<View>,
        topics: Option<Vec<Topic>>,
        motifs: Option<Vec<Motif>>,
        saved_queries: Option<Vec<SavedQuery>>,
    ) -> Self {
        Self {
            views,
            topics,
            motifs,
            saved_queries,
            metadata: None,
        }
    }

    pub fn view_by_name(&self, name: &str) -> Option<&View> {
        self.views.iter().find(|v| v.name == name)
    }

    pub fn topics_list(&self) -> &[Topic] {
        self.topics.as_deref().unwrap_or(&[])
    }

    pub fn motifs_list(&self) -> &[Motif] {
        self.motifs.as_deref().unwrap_or(&[])
    }

    pub fn saved_queries_list(&self) -> &[SavedQuery] {
        self.saved_queries.as_deref().unwrap_or(&[])
    }

    pub fn motif_by_name(&self, name: &str) -> Option<&Motif> {
        self.motifs_list().iter().find(|m| m.name == name)
    }
}

#[cfg(test)]
mod preagg_tests {
    use super::*;

    #[test]
    fn test_view_with_pre_aggregations_parses() {
        let yaml = r#"
name: orders
description: "Test orders"
table: orders
dimensions:
  - name: region
    type: string
    expr: region
  - name: created_at
    type: datetime
    expr: created_at
measures:
  - name: total_revenue
    type: sum
    expr: revenue
pre_aggregations:
  - name: by_region_monthly
    dimensions: [region]
    measures: [total_revenue]
    time_dimension: created_at
    granularity: month
"#;
        let raw: RawView = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(raw.pre_aggregations.as_ref().unwrap().len(), 1);
        let pa = &raw.pre_aggregations.as_ref().unwrap()[0];
        assert_eq!(pa.name, "by_region_monthly");
        assert_eq!(pa.dimensions, vec!["region"]);
        assert_eq!(pa.measures, vec!["total_revenue"]);
        assert_eq!(pa.time_dimension.as_deref(), Some("created_at"));
        assert_eq!(pa.granularity.as_deref(), Some("month"));
    }

    #[test]
    fn test_view_without_pre_aggregations_parses() {
        let yaml = r#"
name: orders
description: "Test orders"
table: orders
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: total_revenue
    type: sum
    expr: revenue
"#;
        let raw: RawView = serde_yaml::from_str(yaml).expect("parse");
        assert!(raw.pre_aggregations.is_none());
    }
}

/// Items that can appear in the dimensions/measures/entities lists.
/// Supports both inline definitions and inherits_from references.
/// When only `inherits_from` is present, the item is resolved from globals.
/// When both fields and `inherits_from` are present, globals provide defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DimensionItem {
    Inline(Dimension),
    Inherit { inherits_from: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum MeasureItem {
    Inline(Measure),
    Inherit { inherits_from: String },
}

/// Entity items: an entity always has a `name`, but may also have `inherits_from`.
/// We parse as a raw YAML value and handle both cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum EntityItem {
    Inline(Entity),
    Inherit { inherits_from: String },
}

/// Raw view as parsed from YAML (before inheritance resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawView {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(default)]
    pub entities: Vec<EntityItem>,
    #[serde(default)]
    pub dimensions: Vec<DimensionItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<MeasureItem>>,
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Pre-aggregation rollup definitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_aggregations: Option<Vec<PreAggregation>>,
    /// View-level refresh key — applies to all rollups unless a per-rollup key overrides it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key: Option<RefreshKey>,
    /// User-defined metadata for discovery and organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, Vec<String>>>,
}

#[cfg(test)]
mod refresh_key_tests {
    use super::*;

    #[test]
    fn test_refresh_key_sql_roundtrip() {
        let yaml = "sql: \"SELECT MAX(updated_at) FROM orders\"";
        let rk: RefreshKey = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            rk,
            RefreshKey::Sql("SELECT MAX(updated_at) FROM orders".into())
        );
    }

    #[test]
    fn test_refresh_key_every_roundtrip() {
        let yaml = "every: \"6h\"";
        let rk: RefreshKey = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rk, RefreshKey::Every("6h".into()));
    }

    #[test]
    fn test_pre_aggregation_with_refresh_key() {
        let yaml = r#"
name: by_region
dimensions: [region]
measures: [revenue]
refresh_key:
  every: "1h"
"#;
        let pa: PreAggregation = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(pa.refresh_key, Some(RefreshKey::Every("1h".into())));
    }

    #[test]
    fn test_view_level_refresh_key() {
        let yaml = r#"
name: orders
table: orders
refresh_key:
  sql: "SELECT MAX(id) FROM orders"
"#;
        let v: View = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            v.refresh_key,
            Some(RefreshKey::Sql("SELECT MAX(id) FROM orders".into()))
        );
    }

    #[test]
    fn test_refresh_key_rejects_both_keys() {
        let yaml = "sql: \"SELECT 1\"\nevery: \"1h\"";
        let result: Result<RefreshKey, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exactly one"), "unexpected error: {msg}");
    }

    #[test]
    fn test_refresh_key_rejects_unknown_keys() {
        let yaml = "sql: \"SELECT 1\"\nfoo: bar";
        let result: Result<RefreshKey, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unknown key"), "unexpected error: {msg}");
    }
}

#[cfg(test)]
mod measure_direction_tests {
    use super::*;

    #[test]
    fn measure_direction_defaults_to_higher_is_better() {
        let m: Measure = serde_yaml::from_str("name: revenue\ntype: sum\nexpr: amount\n").unwrap();
        assert_eq!(m.direction, MeasureDirection::HigherIsBetter);
    }

    #[test]
    fn measure_direction_parses_lower_is_better() {
        let m: Measure = serde_yaml::from_str(
            "name: food_cost_pct\ntype: sum\nexpr: cogs\ndirection: lower_is_better\n",
        )
        .unwrap();
        assert_eq!(m.direction, MeasureDirection::LowerIsBetter);
    }

    #[test]
    fn measure_direction_round_trips_and_omits_default() {
        let m: Measure = serde_yaml::from_str("name: revenue\ntype: sum\nexpr: amount\n").unwrap();
        let out = serde_yaml::to_string(&m).unwrap();
        assert!(
            !out.contains("direction"),
            "default direction must not serialize: {out}"
        );
    }
}

#[cfg(test)]
mod dimension_analysis_tests {
    use super::*;

    #[test]
    fn analysis_caps_defaults_to_all_true() {
        let d: Dimension =
            serde_yaml::from_str("name: region\ntype: string\nexpr: region\n").unwrap();
        let caps = d.analysis_caps();
        assert!(caps.explain && caps.benchmark);
    }

    #[test]
    fn segmentable_false_suppresses_both_capabilities() {
        // segmentable is applied inside discover_dimensions today, which gates all
        // six call sites, so `false` means both capabilities off. This preserves
        // the alias exactly.
        let d: Dimension =
            serde_yaml::from_str("name: gender\ntype: string\nexpr: g\nsegmentable: false\n")
                .unwrap();
        let caps = d.analysis_caps();
        assert!(!caps.explain && !caps.benchmark);
    }

    #[test]
    fn analysis_can_split_the_two_capabilities() {
        // The party_size case: legitimate to decompose by, invalid to benchmark across.
        let d: Dimension = serde_yaml::from_str(
            "name: party_size\ntype: number\nexpr: party_size\nanalysis:\n  explain: true\n  benchmark: false\n",
        )
        .unwrap();
        let caps = d.analysis_caps();
        assert!(caps.explain, "explain must survive");
        assert!(!caps.benchmark, "benchmark must be suppressed");
    }

    #[test]
    fn analysis_wins_over_segmentable_when_both_present() {
        // Not an error: cube.rs machine-generates `segmentable`, so a hard failure
        // would force users to hand-edit generated output.
        let d: Dimension = serde_yaml::from_str(
            "name: party_size\ntype: number\nexpr: p\nsegmentable: false\nanalysis:\n  explain: true\n  benchmark: false\n",
        )
        .unwrap();
        assert!(d.analysis_caps().explain);
    }

    #[test]
    fn analysis_rejects_an_unknown_key() {
        // Both fields default to true, so a typo would otherwise parse to an
        // all-permissive block that is indistinguishable from omitting the
        // section — the modeller's exclusion silently does nothing.
        let err = serde_yaml::from_str::<Dimension>(
            "name: party_size\ntype: number\nexpr: p\nanalysis:\n  explan: true\n",
        )
        .expect_err("a misspelled analysis key must not parse");
        let msg = err.to_string();
        assert!(
            msg.contains("explan"),
            "the error must name the offending key, got: {msg}"
        );
    }

    #[test]
    fn analysis_accepts_a_partial_block() {
        // deny_unknown_fields must not become deny_missing_fields: naming only
        // the capability you want to switch off stays valid.
        let d: Dimension = serde_yaml::from_str(
            "name: party_size\ntype: number\nexpr: p\nanalysis:\n  benchmark: false\n",
        )
        .expect("a partial analysis block stays valid");
        let caps = d.analysis_caps();
        assert!(caps.explain && !caps.benchmark);
    }

    #[test]
    fn analysis_caps_works_on_a_programmatically_built_dimension() {
        // Guards the decision that resolution is a method, not a parse-time rewrite:
        // ~40 sites build Dimension literals without touching the parser.
        let mut d: Dimension = serde_yaml::from_str("name: x\ntype: string\nexpr: x\n").unwrap();
        d.segmentable = Some(false);
        d.analysis = None;
        let caps = d.analysis_caps();
        assert!(!caps.explain && !caps.benchmark);
    }
}
