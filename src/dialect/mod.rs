pub mod templates;

use serde::{Deserialize, Serialize};
use templates::SqlTemplates;

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
}

impl Dialect {
    /// Get the SQL templates for this dialect.
    pub fn templates(&self) -> SqlTemplates {
        match self {
            Dialect::Postgres => SqlTemplates::postgres(),
            Dialect::MySQL => SqlTemplates::mysql(),
            Dialect::BigQuery => SqlTemplates::bigquery(),
            Dialect::Snowflake => SqlTemplates::snowflake(),
            Dialect::DuckDB => SqlTemplates::duckdb(),
            Dialect::ClickHouse => SqlTemplates::clickhouse(),
            Dialect::Databricks => SqlTemplates::databricks(),
            Dialect::Redshift => SqlTemplates::redshift(),
            Dialect::SQLite => SqlTemplates::sqlite(),
            Dialect::Domo => SqlTemplates::domo(),
        }
    }

    /// Quote an identifier for this dialect.
    pub fn quote_identifier(&self, name: &str) -> String {
        match self {
            Dialect::MySQL | Dialect::Domo => format!("`{}`", name.replace('`', "``")),
            Dialect::BigQuery => format!("`{}`", name.replace('`', "\\`")),
            // Snowflake stores unquoted identifiers as UPPERCASE, so quoted refs
            // must also be uppercase to match the default convention.
            Dialect::Snowflake => format!("\"{}\"", name.to_uppercase().replace('"', "\"\"")),
            _ => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }

    /// Date truncation expression.
    pub fn date_trunc(&self, granularity: &str, expr: &str) -> String {
        match self {
            Dialect::MySQL | Dialect::Domo => match granularity {
                "year" => format!("DATE_FORMAT({}, '%Y-01-01')", expr),
                "quarter" => format!("DATE_FORMAT(DATE_SUB({e}, INTERVAL (MONTH({e}) - 1) %% 3 MONTH), '%Y-%m-01')", e = expr),
                "month" => format!("DATE_FORMAT({}, '%Y-%m-01')", expr),
                "week" => format!("DATE(DATE_SUB({}, INTERVAL DAYOFWEEK({}) - 1 DAY))", expr, expr),
                "day" => format!("DATE({})", expr),
                "hour" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:00:00')", expr),
                "minute" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:%i:00')", expr),
                "second" => format!("DATE_FORMAT({}, '%Y-%m-%d %H:%i:%s')", expr),
                _ => format!("DATE({})", expr),
            },
            Dialect::BigQuery => format!("TIMESTAMP_TRUNC({}, {})", expr, granularity.to_uppercase()),
            Dialect::Snowflake => format!("DATE_TRUNC('{}', {})", granularity, expr),
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
                format!("CONVERT_TIMEZONE('UTC', '{}', {}::TIMESTAMP_NTZ)", timezone, expr)
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
            Dialect::Databricks => format!("CAST({} AS TIMESTAMP)", expr),
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

    /// Count distinct approximation.
    pub fn count_distinct_approx(&self, expr: &str) -> String {
        match self {
            Dialect::BigQuery => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::Snowflake => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::ClickHouse => format!("uniqHLL12({})", expr),
            Dialect::Databricks => format!("APPROX_COUNT_DISTINCT({})", expr),
            Dialect::Redshift => format!("APPROXIMATE COUNT(DISTINCT {})", expr),
            _ => format!("COUNT(DISTINCT {})", expr), // fallback
        }
    }

    /// Param placeholder for parameterized queries.
    pub fn param_placeholder(&self, index: usize) -> String {
        match self {
            Dialect::Postgres | Dialect::Redshift | Dialect::DuckDB => format!("${}", index + 1),
            Dialect::MySQL | Dialect::SQLite | Dialect::Domo => "?".to_string(),
            Dialect::BigQuery => format!("@p{}", index),
            Dialect::Snowflake => "?".to_string(),
            Dialect::ClickHouse => format!("${}", index + 1),
            Dialect::Databricks => "?".to_string(),
        }
    }

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
        }
    }
}
