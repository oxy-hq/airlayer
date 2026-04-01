use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Optional entity names to route multi-hop joins through.
    #[serde(default)]
    pub through: Vec<String>,
    /// Motif to apply as post-aggregation transform (e.g., "yoy", "anomaly", "contribution").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub motif: Option<String>,
    /// Parameters for the motif (e.g., threshold, window size).
    #[serde(default)]
    pub motif_params: HashMap<String, serde_json::Value>,
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
            through: vec![],
            motif: None,
            motif_params: HashMap::new(),
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
            collect_filter_views(f, &mut views);
        }
        views.into_iter().collect()
    }
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Recursively collect view names from a filter and its nested and/or groups.
fn collect_filter_views(filter: &QueryFilter, views: &mut std::collections::HashSet<String>) {
    if let Some(member) = &filter.member {
        if let Some(v) = member.split('.').next() {
            views.insert(v.to_string());
        }
    }
    if let Some(and_filters) = &filter.and {
        for f in and_filters {
            collect_filter_views(f, views);
        }
    }
    if let Some(or_filters) = &filter.or {
        for f in or_filters {
            collect_filter_views(f, views);
        }
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
    /// On a specific date (expands to inDateRange for the full day).
    OnTheDate,
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
            "onTheDate" => Some(Self::OnTheDate),
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

impl TimeDimensionQuery {
    /// Resolve the date range, including relative date strings like "last 7 days", "this month", etc.
    /// Returns the resolved [from, to] pair as ISO date strings, or None if no date_range is set.
    pub fn resolved_date_range(&self) -> Option<Vec<String>> {
        let range = self.date_range.as_ref()?;

        // If already a 2-element array, check if the first element is a relative string
        if range.len() == 2 {
            return Some(range.clone());
        }

        // Single-element relative date range
        if range.len() == 1 {
            if let Some(resolved) = parse_relative_date_range(&range[0]) {
                return Some(resolved);
            }
            return Some(range.clone());
        }

        Some(range.clone())
    }
}

/// Parse a relative date range string into [from, to] ISO date strings.
/// Supports: "today", "yesterday", "this week", "this month", "this quarter", "this year",
/// "last week", "last month", "last quarter", "last year", "last N days/weeks/months/years".
pub fn parse_relative_date_range(s: &str) -> Option<Vec<String>> {
    use chrono::{Datelike, Local, NaiveDate};

    let today = Local::now().date_naive();
    let s = s.trim().to_lowercase();

    match s.as_str() {
        "today" => {
            let d = today.format("%Y-%m-%d").to_string();
            Some(vec![d.clone(), d])
        }
        "yesterday" => {
            let d = (today - chrono::Duration::days(1)).format("%Y-%m-%d").to_string();
            Some(vec![d.clone(), d])
        }
        "this week" => {
            let weekday = today.weekday().num_days_from_monday();
            let start = today - chrono::Duration::days(weekday as i64);
            let end = start + chrono::Duration::days(6);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "this month" => {
            let start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)?;
            let end = if today.month() == 12 {
                NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)?
            } else {
                NaiveDate::from_ymd_opt(today.year(), today.month() + 1, 1)?
            } - chrono::Duration::days(1);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "this quarter" => {
            let q = (today.month() - 1) / 3;
            let start_month = q * 3 + 1;
            let start = NaiveDate::from_ymd_opt(today.year(), start_month, 1)?;
            let end_month = start_month + 3;
            let end = if end_month > 12 {
                NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)?
            } else {
                NaiveDate::from_ymd_opt(today.year(), end_month, 1)?
            } - chrono::Duration::days(1);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "this year" => {
            let start = NaiveDate::from_ymd_opt(today.year(), 1, 1)?;
            let end = NaiveDate::from_ymd_opt(today.year(), 12, 31)?;
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "last week" => {
            let weekday = today.weekday().num_days_from_monday();
            let this_week_start = today - chrono::Duration::days(weekday as i64);
            let start = this_week_start - chrono::Duration::days(7);
            let end = this_week_start - chrono::Duration::days(1);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "last month" => {
            let (y, m) = if today.month() == 1 {
                (today.year() - 1, 12)
            } else {
                (today.year(), today.month() - 1)
            };
            let start = NaiveDate::from_ymd_opt(y, m, 1)?;
            let end = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)?
                - chrono::Duration::days(1);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "last quarter" => {
            let q = (today.month() - 1) / 3;
            let (y, prev_q_start) = if q == 0 {
                (today.year() - 1, 10)
            } else {
                (today.year(), (q - 1) * 3 + 1)
            };
            let start = NaiveDate::from_ymd_opt(y, prev_q_start, 1)?;
            let current_q_start = q * 3 + 1;
            let end = NaiveDate::from_ymd_opt(today.year(), current_q_start, 1)?
                - chrono::Duration::days(1);
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        "last year" => {
            let start = NaiveDate::from_ymd_opt(today.year() - 1, 1, 1)?;
            let end = NaiveDate::from_ymd_opt(today.year() - 1, 12, 31)?;
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
        _ => {
            // Try "last N days/weeks/months/years"
            let re = regex::Regex::new(r"^last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)$").ok()?;
            let caps = re.captures(&s)?;
            let n: i64 = caps[1].parse().ok()?;
            let unit = &caps[2];
            let (start, end) = match unit {
                "day" | "days" => {
                    let end = today - chrono::Duration::days(1);
                    let start = today - chrono::Duration::days(n);
                    (start, end)
                }
                "week" | "weeks" => {
                    let end = today - chrono::Duration::days(1);
                    let start = today - chrono::Duration::weeks(n);
                    (start, end)
                }
                "month" | "months" => {
                    let end = today - chrono::Duration::days(1);
                    let mut m = today.month() as i64 - n;
                    let mut y = today.year();
                    while m <= 0 {
                        m += 12;
                        y -= 1;
                    }
                    let start = NaiveDate::from_ymd_opt(y, m as u32, today.day().min(28))
                        .unwrap_or(NaiveDate::from_ymd_opt(y, m as u32, 28)?);
                    (start, end)
                }
                "year" | "years" => {
                    let end = today - chrono::Duration::days(1);
                    let start = NaiveDate::from_ymd_opt(today.year() - n as i32, today.month(), today.day().min(28))
                        .unwrap_or(NaiveDate::from_ymd_opt(today.year() - n as i32, today.month(), 28)?);
                    (start, end)
                }
                _ => return None,
            };
            Some(vec![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ])
        }
    }
}

/// Order by specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub id: String,
    #[serde(default)]
    pub desc: bool,
}

/// The result of compiling a query.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    /// The generated SQL string.
    pub sql: String,
    /// Parameter values for parameterized queries.
    pub params: Vec<String>,
    /// Column aliases in order.
    pub columns: Vec<ColumnMeta>,
}

/// Metadata about a result column.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnMeta {
    /// The member path (e.g., "orders.status").
    pub member: String,
    /// The SQL alias used in the SELECT clause.
    pub alias: String,
    /// Whether this is a measure or dimension.
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ColumnKind {
    Dimension,
    Measure,
    TimeDimension,
    MotifComputed,
}
