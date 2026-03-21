use std::collections::HashMap;

/// A collection of SQL templates for rendering dialect-specific SQL.
/// Uses minijinja for template rendering.
pub struct SqlTemplates {
    templates: HashMap<String, String>,
}

impl SqlTemplates {
    pub fn new(templates: HashMap<String, String>) -> Self {
        Self { templates }
    }

    pub fn get(&self, name: &str) -> Option<&String> {
        self.templates.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.templates.contains_key(name)
    }

    /// PostgreSQL templates (default).
    pub fn postgres() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "${{ param_index + 1 }}".into());
        Self::new(t)
    }

    /// MySQL templates.
    pub fn mysql() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "`".into());
        t.insert("quotes/escape".into(), "``".into());
        t.insert("params/param".into(), "?".into());
        t.insert("functions/COUNT_DISTINCT".into(), "COUNT(DISTINCT {{ args_concat }})".into());
        Self::new(t)
    }

    /// BigQuery templates.
    pub fn bigquery() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "`".into());
        t.insert("quotes/escape".into(), "\\`".into());
        t.insert("params/param".into(), "@p{{ param_index }}".into());
        t.insert("functions/COUNT_DISTINCT".into(), "COUNT(DISTINCT {{ args_concat }})".into());
        Self::new(t)
    }

    /// Snowflake templates.
    pub fn snowflake() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "?".into());
        Self::new(t)
    }

    /// DuckDB templates.
    pub fn duckdb() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "${{ param_index + 1 }}".into());
        Self::new(t)
    }

    /// ClickHouse templates.
    pub fn clickhouse() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "${{ param_index + 1 }}".into());
        t.insert("functions/COUNT_DISTINCT".into(), "uniq({{ args_concat }})".into());
        Self::new(t)
    }

    /// Databricks templates.
    pub fn databricks() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "`".into());
        t.insert("quotes/escape".into(), "``".into());
        t.insert("params/param".into(), "?".into());
        Self::new(t)
    }

    /// Redshift templates.
    pub fn redshift() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "${{ param_index + 1 }}".into());
        Self::new(t)
    }

    /// SQLite templates.
    pub fn sqlite() -> Self {
        let mut t = base_templates();
        t.insert("quotes/identifiers".into(), "\"".into());
        t.insert("quotes/escape".into(), "\"\"".into());
        t.insert("params/param".into(), "?".into());
        Self::new(t)
    }
}

/// Base templates shared across all dialects.
fn base_templates() -> HashMap<String, String> {
    let mut t = HashMap::new();

    // Aggregate functions
    t.insert("functions/SUM".into(), "SUM({{ args_concat }})".into());
    t.insert("functions/MIN".into(), "MIN({{ args_concat }})".into());
    t.insert("functions/MAX".into(), "MAX({{ args_concat }})".into());
    t.insert("functions/COUNT".into(), "COUNT({{ args_concat }})".into());
    t.insert("functions/COUNT_DISTINCT".into(), "COUNT(DISTINCT {{ args_concat }})".into());
    t.insert("functions/AVG".into(), "AVG({{ args_concat }})".into());
    t.insert("functions/COALESCE".into(), "COALESCE({{ args_concat }})".into());

    // Expressions
    t.insert("expressions/column_reference".into(), "{% if table_name %}{{ table_name }}.{% endif %}{{ name }}".into());
    t.insert("expressions/column_aliased".into(), "{{expr}} {{quoted_alias}}".into());
    t.insert("expressions/cast".into(), "CAST({{ expr }} AS {{ data_type }})".into());
    t.insert("expressions/binary".into(), "({{ left }} {{ op }} {{ right }})".into());
    t.insert("expressions/is_null".into(), "({{ expr }} IS {% if negate %}NOT {% endif %}NULL)".into());

    // Filters
    t.insert("filters/equals".into(), "{{ column }} = {{ value }}".into());
    t.insert("filters/not_equals".into(), "{{ column }} <> {{ value }}".into());
    t.insert("filters/in".into(), "{{ column }} IN ({{ values_concat }})".into());
    t.insert("filters/not_in".into(), "{{ column }} NOT IN ({{ values_concat }})".into());
    t.insert("filters/set_where".into(), "{{ column }} IS NOT NULL".into());
    t.insert("filters/not_set_where".into(), "{{ column }} IS NULL".into());
    t.insert("filters/gt".into(), "{{ column }} > {{ param }}".into());
    t.insert("filters/gte".into(), "{{ column }} >= {{ param }}".into());
    t.insert("filters/lt".into(), "{{ column }} < {{ param }}".into());
    t.insert("filters/lte".into(), "{{ column }} <= {{ param }}".into());

    t
}
