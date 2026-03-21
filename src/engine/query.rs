use serde::{Deserialize, Serialize};

/// A query request to the semantic engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Measures to select (e.g., ["orders.total_revenue", "orders.count"]).
    #[serde(default)]
    pub measures: Vec<String>,
    /// Dimensions to group by (e.g., ["orders.status", "customers.name"]).
    #[serde(default)]
    pub dimensions: Vec<String>,
    /// Filters to apply.
    #[serde(default)]
    pub filters: Vec<QueryFilter>,
    /// Segments to apply (predefined filter conditions).
    #[serde(default)]
    pub segments: Vec<String>,
    /// Time dimensions with optional granularity and date range.
    #[serde(default)]
    pub time_dimensions: Vec<TimeDimensionQuery>,
    /// Order by clauses.
    #[serde(default)]
    pub order: Vec<OrderBy>,
    /// Limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Timezone for time dimension conversion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Ungrouped mode: return raw rows without aggregation.
    #[serde(default)]
    pub ungrouped: bool,
}

impl QueryRequest {
    pub fn new() -> Self {
        Self {
            measures: vec![],
            dimensions: vec![],
            filters: vec![],
            segments: vec![],
            time_dimensions: vec![],
            order: vec![],
            limit: None,
            offset: None,
            timezone: None,
            ungrouped: false,
        }
    }

    /// All views referenced by this query.
    pub fn referenced_views(&self) -> Vec<String> {
        let mut views = std::collections::HashSet::new();
        for m in &self.measures {
            if let Some(v) = m.split('.').next() {
                views.insert(v.to_string());
            }
        }
        for d in &self.dimensions {
            if let Some(v) = d.split('.').next() {
                views.insert(v.to_string());
            }
        }
        for td in &self.time_dimensions {
            if let Some(v) = td.dimension.split('.').next() {
                views.insert(v.to_string());
            }
        }
        for s in &self.segments {
            if let Some(v) = s.split('.').next() {
                views.insert(v.to_string());
            }
        }
        for f in &self.filters {
            if let Some(member) = &f.member {
                if let Some(v) = member.split('.').next() {
                    views.insert(v.to_string());
                }
            }
        }
        views.into_iter().collect()
    }
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// A filter in a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    /// Member path (e.g., "orders.status"). None for and/or groups.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    /// Filter operator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<FilterOperator>,
    /// Filter values.
    #[serde(default)]
    pub values: Vec<String>,
    /// AND group of sub-filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<QueryFilter>>,
    /// OR group of sub-filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<QueryFilter>>,
}

/// Supported filter operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FilterOperator {
    Equals,
    NotEquals,
    Contains,
    NotContains,
    StartsWith,
    NotStartsWith,
    EndsWith,
    NotEndsWith,
    Gt,
    Gte,
    Lt,
    Lte,
    Set,
    NotSet,
    InDateRange,
    NotInDateRange,
    BeforeDate,
    BeforeOrOnDate,
    AfterDate,
    AfterOrOnDate,
}

impl FilterOperator {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "equals" => Some(Self::Equals),
            "notEquals" => Some(Self::NotEquals),
            "contains" => Some(Self::Contains),
            "notContains" => Some(Self::NotContains),
            "startsWith" => Some(Self::StartsWith),
            "notStartsWith" => Some(Self::NotStartsWith),
            "endsWith" => Some(Self::EndsWith),
            "notEndsWith" => Some(Self::NotEndsWith),
            "gt" => Some(Self::Gt),
            "gte" => Some(Self::Gte),
            "lt" => Some(Self::Lt),
            "lte" => Some(Self::Lte),
            "set" => Some(Self::Set),
            "notSet" => Some(Self::NotSet),
            "inDateRange" => Some(Self::InDateRange),
            "notInDateRange" => Some(Self::NotInDateRange),
            "beforeDate" => Some(Self::BeforeDate),
            "beforeOrOnDate" => Some(Self::BeforeOrOnDate),
            "afterDate" => Some(Self::AfterDate),
            "afterOrOnDate" => Some(Self::AfterOrOnDate),
            _ => None,
        }
    }
}

/// Time dimension query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeDimensionQuery {
    /// Member path (e.g., "orders.order_date").
    pub dimension: String,
    /// Granularity (day, week, month, quarter, year).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    /// Date range [from, to].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_range: Option<Vec<String>>,
}

/// Order by specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub id: String,
    #[serde(default)]
    pub desc: bool,
}

/// The result of compiling a query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The generated SQL string.
    pub sql: String,
    /// Parameter values for parameterized queries.
    pub params: Vec<String>,
    /// Column aliases in order.
    pub columns: Vec<ColumnMeta>,
}

/// Metadata about a result column.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// The member path (e.g., "orders.status").
    pub member: String,
    /// The SQL alias used in the SELECT clause.
    pub alias: String,
    /// Whether this is a measure or dimension.
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnKind {
    Dimension,
    Measure,
    TimeDimension,
}
