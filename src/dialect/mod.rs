use serde::{Deserialize, Serialize};

/// Supported SQL dialects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    Postgres,
    MySQL,
    BigQuery,
    Snowflake,
    DuckDB,
    ClickHouse,
    Databricks,
    Redshift,
    SQLite,
    Domo,
    Presto,
}

impl Dialect {
    /// Quote an identifier for this dialect.
    pub fn quote_identifier(&self, name: &str) -> String {
        match self {
            Dialect::MySQL | Dialect::Domo | Dialect::Databricks => {
                format!("`{}`", name.replace('`', "``"))
            }
            Dialect::BigQuery => format!("`{}`", name.replace('`', "\\`")),
            // Snowflake stores unquoted identifiers as UPPERCASE, so quoted refs
            // must also be uppercase to match the default convention.
            Dialect::Snowflake => format!("\"{}\"", name.to_uppercase().replace('"', "\"\"")),
            _ => format!("\"{}\"", name.replace('"', "\"\"")), // Postgres, DuckDB, ClickHouse, Redshift, SQLite, Presto, etc.
        }
    }

    /// Date truncation expression.
    pub fn date_trunc(&self, granularity: &str, expr: &str) -> String {
        match self {
            Dialect::MySQL | Dialect::Domo => match granularity {
                "year" => format!("DATE_FORMAT({}, '%Y-01-01')", expr),
                "quarter" => format!(
                    "DATE_FORMAT(DATE_SUB({e}, INTERVAL (MONTH({e}) - 1) %% 3 MONTH), '%Y-%m-01')",
                    e = expr
                ),
                "month" => format!("DATE_FORMAT({}, '%Y-%m-01')", expr),
                "week" => format!(
                    "DATE(DATE_SUB({}, INTERVAL DAYOFWEEK({}) - 1 DAY))",
                    expr, expr
                ),
                "day" => format!("DATE({})", expr),
                "hour" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:00:00')", expr),
                "minute" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:%i:00')", expr),
                "second" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:%i:%s')", expr),
                _ => format!("DATE({})", expr),
            },
            Dialect::BigQuery => {
                format!("TIMESTAMP_TRUNC({}, {})", expr, granularity.to_uppercase())
            }
            Dialect::Snowflake | Dialect::Presto => {
                format!("DATE_TRUNC('{}', {})", granularity, expr)
            }
            Dialect::ClickHouse => {
                let func = match granularity {
                    "year" => "toStartOfYear",
                    "quarter" => "toStartOfQuarter",
                    "month" => "toStartOfMonth",
                    "week" => "toMonday",
                    "day" => "toDate",
                    "hour" => "toStartOfHour",
                    "minute" => "toStartOfMinute",
                    "second" => "toStartOfSecond",
                    _ => "toDate",
                };
                format!("{}({})", func, expr)
            }
            _ => format!("date_trunc('{}', {})", granularity, expr),
        }
    }

    /// Convert timezone expression.
    pub fn convert_tz(&self, expr: &str, timezone: &str) -> String {
        if timezone == "UTC" {
            return expr.to_string();
        }
        match self {
            Dialect::Postgres | Dialect::Redshift => {
                format!("({}::timestamptz AT TIME ZONE '{}')", expr, timezone)
            }
            Dialect::MySQL => {
                format!("CONVERT_TZ({}, 'UTC', '{}')", expr, timezone)
            }
            Dialect::BigQuery => {
                format!("DATETIME({}, '{}')", expr, timezone)
            }
            Dialect::Snowflake => {
                format!(
                    "CONVERT_TIMEZONE('UTC', '{}', {}::TIMESTAMP_NTZ)",
                    timezone, expr
                )
            }
            Dialect::DuckDB => {
                format!("timezone('{}', {}::TIMESTAMPTZ)", timezone, expr)
            }
            Dialect::ClickHouse => {
                format!("toTimeZone({}, '{}')", expr, timezone)
            }
            Dialect::Databricks => {
                format!("from_utc_timestamp({}, '{}')", expr, timezone)
            }
            Dialect::Presto => {
                format!("({} AT TIME ZONE '{}')", expr, timezone)
            }
            Dialect::SQLite | Dialect::Domo => expr.to_string(), // no TZ support
        }
    }

    /// Timestamp cast expression.
    pub fn timestamp_cast(&self, expr: &str) -> String {
        match self {
            Dialect::Postgres | Dialect::Redshift => format!("{}::timestamptz", expr),
            Dialect::MySQL => format!("TIMESTAMP({})", expr),
            Dialect::BigQuery => format!("TIMESTAMP({})", expr),
            Dialect::Snowflake => format!("TO_TIMESTAMP({})", expr),
            Dialect::DuckDB => format!("{}::TIMESTAMP", expr),
            Dialect::ClickHouse => format!("toDateTime({})", expr),
            Dialect::Databricks | Dialect::Presto => format!("CAST({} AS TIMESTAMP)", expr),
            Dialect::SQLite => expr.to_string(),
            Dialect::Domo => format!("CAST({} AS TIMESTAMP)", expr),
        }
    }

    /// Interval expression.
    pub fn interval_expr(&self, interval: &str) -> String {
        match self {
            Dialect::BigQuery => format!("INTERVAL {}", interval),
            Dialect::ClickHouse => format!("INTERVAL {}", interval),
            _ => format!("interval '{}'", interval),
        }
    }

    /// Cast an expression to a DATE. Shift comparisons normalize every operand to
    /// DATE so cross-dialect comparisons never mix DATE/TIMESTAMP or rely on
    /// implicit string coercion (which BigQuery/Presto reject).
    pub fn cast_to_date(&self, expr: &str) -> String {
        match self {
            // ClickHouse's idiomatic, always-available date cast.
            Dialect::ClickHouse => format!("toDate({})", expr),
            _ => format!("CAST({} AS DATE)", expr),
        }
    }

    /// A DATE literal for an ISO `YYYY-MM-DD` string. Always wrapped in a cast so
    /// it is a true DATE everywhere (not a bare string the engine must coerce).
    pub fn date_literal(&self, iso: &str) -> String {
        self.cast_to_date(&format!("'{}'", iso))
    }

    /// Add `count` of `unit` (`"month"` or `"day"`, normalized by
    /// [`crate::engine::shift::Interval::base_parts`]) to a DATE-typed `expr`,
    /// returning a DATE. Drives the shift self-join key that aligns the prior
    /// bucket to the current bucket. Each dialect uses its own portable form
    /// (date + INTERVAL is not universal: MySQL/BigQuery/Snowflake/Presto/Spark
    /// all differ).
    pub fn date_add(&self, expr: &str, count: i64, unit: &str) -> String {
        let unit_upper = unit.to_uppercase();
        match self {
            // `date + INTERVAL '...'` yields a TIMESTAMP on these — re-cast to DATE
            // so both sides of the join key stay DATE-typed.
            Dialect::Postgres | Dialect::Redshift | Dialect::DuckDB => {
                self.cast_to_date(&format!("({} + INTERVAL '{} {}')", expr, count, unit))
            }
            Dialect::MySQL | Dialect::Domo => {
                format!("DATE_ADD({}, INTERVAL {} {})", expr, count, unit_upper)
            }
            Dialect::Snowflake => format!("DATEADD({}, {}, {})", unit, count, expr),
            Dialect::BigQuery => {
                format!("DATE_ADD({}, INTERVAL {} {})", expr, count, unit_upper)
            }
            Dialect::ClickHouse => {
                let func = if unit == "month" {
                    "addMonths"
                } else {
                    "addDays"
                };
                format!("{}({}, {})", func, expr, count)
            }
            Dialect::Databricks => {
                if unit == "month" {
                    format!("add_months({}, {})", expr, count)
                } else {
                    format!("date_add({}, {})", expr, count)
                }
            }
            Dialect::Presto => format!("date_add('{}', {}, {})", unit, count, expr),
            // SQLite has no date_trunc, so shift errors before reaching here; this
            // form is provided only for completeness.
            Dialect::SQLite => format!("date({}, '+{} {}s')", expr, count, unit),
        }
    }

    /// Count distinct approximation.
    pub fn count_distinct_approx(&self, expr: &str) -> String {
        match self {
            Dialect::BigQuery => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::Snowflake => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::ClickHouse => format!("uniqHLL12({})", expr),
            Dialect::Databricks | Dialect::Presto => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::Redshift => format!("APPROXIMATE COUNT(DISTINCT {})", expr),
            _ => format!("COUNT(DISTINCT {})", expr), // fallback
        }
    }

    /// Param placeholder for parameterized queries.
    pub fn param_placeholder(&self, index: usize) -> String {
        match self {
            Dialect::Postgres | Dialect::Redshift | Dialect::DuckDB => format!("${}", index + 1),
            Dialect::MySQL | Dialect::SQLite | Dialect::Domo | Dialect::Presto => "?".to_string(),
            Dialect::BigQuery => format!("@p{}", index),
            Dialect::Snowflake => "?".to_string(),
            Dialect::ClickHouse => format!("${}", index + 1),
            Dialect::Databricks => "?".to_string(),
        }
    }

    /// STDDEV_POP function name for this dialect.
    /// Note: bare `STDDEV` is `STDDEV_SAMP` on most dialects (BigQuery, Snowflake, DuckDB,
    /// Databricks). We must use `STDDEV_POP` explicitly for correct anomaly z-scores.
    pub fn stddev_pop(&self) -> &str {
        match self {
            Dialect::ClickHouse => "stddevPop",
            Dialect::MySQL => "STDDEV", // MySQL's STDDEV is population, not sample
            _ => "STDDEV_POP",          // ANSI standard, supported by all other dialects
        }
    }

    /// Whether this dialect supports REGR_SLOPE / REGR_INTERCEPT natively.
    pub fn has_regression_functions(&self) -> bool {
        matches!(
            self,
            Dialect::Postgres
                | Dialect::Snowflake
                | Dialect::BigQuery
                | Dialect::DuckDB
                | Dialect::Redshift
                | Dialect::Databricks
                | Dialect::Presto
        )
    }

    /// CAST expression to a double/float type for this dialect.
    pub fn cast_to_double(&self, expr: &str) -> String {
        match self {
            Dialect::Postgres | Dialect::Redshift => {
                format!("CAST({} AS DOUBLE PRECISION)", expr)
            }
            Dialect::ClickHouse => format!("CAST({} AS Float64)", expr),
            Dialect::BigQuery => format!("CAST({} AS FLOAT64)", expr),
            Dialect::MySQL | Dialect::Domo => format!("CAST({} AS DECIMAL(38,10))", expr),
            _ => format!("CAST({} AS DOUBLE)", expr), // Snowflake, DuckDB, Databricks, Presto, SQLite
        }
    }

    /// Build a fully-qualified table name with proper quoting for each part.
    /// E.g. `"preagg"."events__abc123__20260415"` for Postgres,
    ///      `\`preagg\`.\`events__abc123__20260415\`` for BigQuery.
    pub fn qualify_table(&self, schema: &str, table: &str) -> String {
        format!(
            "{}.{}",
            self.quote_identifier(schema),
            self.quote_identifier(table)
        )
    }

    /// DDL to create the pre-aggregation schema/database, or None if not needed.
    pub fn create_schema_ddl(&self, schema: &str) -> Option<String> {
        match self {
            Dialect::ClickHouse => Some(format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                self.quote_identifier(schema)
            )),
            // BigQuery datasets are created externally; CTAS into an existing dataset works fine.
            Dialect::BigQuery => None,
            _ => Some(format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                self.quote_identifier(schema)
            )),
        }
    }

    /// Whether this dialect supports GROUPING SETS in GROUP BY.
    pub fn has_grouping_sets(&self) -> bool {
        !matches!(self, Dialect::MySQL | Dialect::SQLite | Dialect::Domo)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Dialect> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Some(Dialect::Postgres),
            "mysql" => Some(Dialect::MySQL),
            "bigquery" | "bq" => Some(Dialect::BigQuery),
            "snowflake" | "sf" => Some(Dialect::Snowflake),
            "duckdb" | "duck" | "motherduck" => Some(Dialect::DuckDB),
            "clickhouse" | "ch" => Some(Dialect::ClickHouse),
            "databricks" => Some(Dialect::Databricks),
            "redshift" | "rs" => Some(Dialect::Redshift),
            "sqlite" => Some(Dialect::SQLite),
            "domo" => Some(Dialect::Domo),
            "presto" | "trino" => Some(Dialect::Presto),
            _ => None,
        }
    }
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::Postgres => write!(f, "postgres"),
            Dialect::MySQL => write!(f, "mysql"),
            Dialect::BigQuery => write!(f, "bigquery"),
            Dialect::Snowflake => write!(f, "snowflake"),
            Dialect::DuckDB => write!(f, "duckdb"),
            Dialect::ClickHouse => write!(f, "clickhouse"),
            Dialect::Databricks => write!(f, "databricks"),
            Dialect::Redshift => write!(f, "redshift"),
            Dialect::SQLite => write!(f, "sqlite"),
            Dialect::Domo => write!(f, "domo"),
            Dialect::Presto => write!(f, "presto"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_grouping_sets() {
        assert!(Dialect::Postgres.has_grouping_sets());
        assert!(Dialect::Snowflake.has_grouping_sets());
        assert!(Dialect::BigQuery.has_grouping_sets());
        assert!(Dialect::DuckDB.has_grouping_sets());
        assert!(Dialect::ClickHouse.has_grouping_sets());
        assert!(Dialect::Databricks.has_grouping_sets());
        assert!(Dialect::Presto.has_grouping_sets());
        assert!(Dialect::Redshift.has_grouping_sets());
        assert!(!Dialect::MySQL.has_grouping_sets());
        assert!(!Dialect::SQLite.has_grouping_sets());
        assert!(!Dialect::Domo.has_grouping_sets());
    }
}
