use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Entity type: primary (owns the key) or foreign (references another view's entity).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EntityType {
    Primary,
    Foreign,
}

impl Default for EntityType {
    fn default() -> Self {
        EntityType::Primary
    }
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
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
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
    /// Inheritance reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherits_from: Option<String>,
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
}

/// A measure (aggregation/metric) within a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measure {
    pub name: String,
    #[serde(rename = "type")]
    pub measure_type: MeasureType,
    #[serde(default)]
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
    pub description: String,
    pub views: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_view: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieval: Option<TopicRetrievalConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_filters: Option<Vec<TopicFilter>>,
}

/// A view in the semantic layer — the core unit of the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
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
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl SemanticLayer {
    pub fn new(views: Vec<View>, topics: Option<Vec<Topic>>) -> Self {
        Self {
            views,
            topics,
            metadata: None,
        }
    }

    pub fn view_by_name(&self, name: &str) -> Option<&View> {
        self.views.iter().find(|v| v.name == name)
    }

    pub fn topics_list(&self) -> &[Topic] {
        self.topics.as_deref().unwrap_or(&[])
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
pub enum MeasureItem {
    Inline(Measure),
    Inherit { inherits_from: String },
}

/// Entity items: an entity always has a `name`, but may also have `inherits_from`.
/// We parse as a raw YAML value and handle both cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EntityItem {
    Inline(Entity),
    Inherit { inherits_from: String },
}

/// Raw view as parsed from YAML (before inheritance resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawView {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
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
}
