//! Interval parsing and calendar date arithmetic for `shift` measures.
//!
//! A `shift` re-evaluates a base measure over a window obtained by shifting the
//! query's current time window by an interval (`"<int> <unit>"`). This module is
//! the standalone, dialect-independent core: it parses the interval string,
//! derives the natural bucket granularity, renders the SQL interval literal, and
//! does the calendar arithmetic on the window *literals* used to build the
//! cohort predicate and the expanded scan window.
//!
//! The SQL assembly that consumes these helpers lives in `sql_generator.rs`
//! (the multi-stage self-join lowering), because it needs the generator's
//! private query-building machinery.
//!
//! TODO(fiscal-calendar): a fiscal/retail calendar step (52/53-week, 4-4-5)
//! would slot in as an alternative `Interval` variant so QSR calendar-shifted
//! comps can align on retail weeks. Intentionally not implemented here.

use chrono::{Datelike, NaiveDate};

/// A calendar unit for a shift interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalUnit {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl IntervalUnit {
    fn parse(s: &str) -> Option<Self> {
        // Tolerate singular/plural ("year" / "years").
        let s = s.trim().to_lowercase();
        let s = s.strip_suffix('s').unwrap_or(&s);
        match s {
            "day" => Some(IntervalUnit::Day),
            "week" => Some(IntervalUnit::Week),
            "month" => Some(IntervalUnit::Month),
            "quarter" => Some(IntervalUnit::Quarter),
            "year" => Some(IntervalUnit::Year),
            _ => None,
        }
    }

    /// Singular SQL keyword for an `INTERVAL '<n> <unit>'` literal.
    fn sql_keyword(&self) -> &'static str {
        match self {
            IntervalUnit::Day => "day",
            IntervalUnit::Week => "week",
            IntervalUnit::Month => "month",
            IntervalUnit::Quarter => "quarter",
            IntervalUnit::Year => "year",
        }
    }

    /// The natural `date_trunc` granularity for bucketing when the query's time
    /// dimension does not specify one.
    pub fn default_granularity(&self) -> &'static str {
        match self {
            IntervalUnit::Day => "day",
            IntervalUnit::Week => "week",
            IntervalUnit::Month => "month",
            IntervalUnit::Quarter => "quarter",
            IntervalUnit::Year => "year",
        }
    }
}

/// A parsed shift interval: a count and a unit (e.g. `1 year`, `14 months`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub n: i64,
    pub unit: IntervalUnit,
}

impl Interval {
    /// Parse a `"<int> <unit>"` string (e.g. `"1 year"`, `"14 months"`).
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!(
                "invalid interval '{}': expected \"<int> <unit>\" (e.g. \"1 year\")",
                s
            ));
        }
        let n: i64 = parts[0]
            .parse()
            .map_err(|_| format!("invalid interval count '{}' in '{}'", parts[0], s))?;
        if n < 0 {
            return Err(format!("interval count must be non-negative, got '{}'", s));
        }
        let unit = IntervalUnit::parse(parts[1])
            .ok_or_else(|| format!("unknown interval unit '{}' in '{}'", parts[1], s))?;
        Ok(Interval { n, unit })
    }

    /// SQL interval literal body for `INTERVAL '<...>'`. Normalized to a base
    /// unit every SQL engine accepts: month-family units render in months,
    /// day-family units in days. (`quarter` is not a valid INTERVAL keyword in
    /// Postgres/DuckDB/Snowflake/etc., and normalizing year/week too keeps the
    /// self-join key arithmetic uniform.)
    pub fn sql_literal(&self) -> String {
        if let Some(m) = self.months() {
            format!("{} month", m)
        } else {
            format!("{} day", self.days().unwrap_or(0))
        }
    }

    /// Number of whole months this interval spans, if it is month-commensurate
    /// (year/quarter/month). `None` for day/week intervals.
    pub(crate) fn months(&self) -> Option<i64> {
        match self.unit {
            IntervalUnit::Year => Some(self.n * 12),
            IntervalUnit::Quarter => Some(self.n * 3),
            IntervalUnit::Month => Some(self.n),
            IntervalUnit::Day | IntervalUnit::Week => None,
        }
    }

    /// Number of days this interval spans, for day/week intervals.
    pub(crate) fn days(&self) -> Option<i64> {
        match self.unit {
            IntervalUnit::Day => Some(self.n),
            IntervalUnit::Week => Some(self.n * 7),
            _ => None,
        }
    }

    /// Verify that `granularity` (the bucket grain) evenly divides this interval,
    /// so the shifted self-join key lands exactly on a bucket boundary. Returns an
    /// error describing the mismatch otherwise. Month-family and day-family units
    /// are never commensurate with each other (a month is not a fixed day count).
    pub fn check_commensurable(&self, granularity: &str) -> Result<(), String> {
        let bucket = Interval { n: 1, unit: IntervalUnit::parse(granularity).ok_or_else(|| {
            format!("unsupported bucket granularity '{}' for a shift", granularity)
        })? };
        let ok = match (self.months(), bucket.months()) {
            (Some(im), Some(bm)) => bm != 0 && im % bm == 0,
            (None, None) => {
                let (id, bd) = (self.days().unwrap_or(0), bucket.days().unwrap_or(0));
                bd != 0 && id % bd == 0
            }
            // one is month-family, the other day-family
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(format!(
                "shift interval '{} {}' is not a whole multiple of the '{}' bucket; the \
                 time dimension granularity must evenly divide the shift (e.g. a 1-year shift \
                 needs month/quarter/year buckets, not week/day)",
                self.n,
                self.unit.sql_keyword(),
                granularity
            ))
        }
    }

    /// Subtract this interval from a date (used for prior windows and the cohort
    /// start-of-life cutoff).
    pub fn subtract_from(&self, d: NaiveDate) -> NaiveDate {
        if let Some(m) = self.months() {
            add_months(d, -m)
        } else {
            d - chrono::Duration::days(self.days().unwrap_or(0))
        }
    }

    /// Add this interval to a date (used for `next` windows).
    pub fn add_to(&self, d: NaiveDate) -> NaiveDate {
        if let Some(m) = self.months() {
            add_months(d, m)
        } else {
            d + chrono::Duration::days(self.days().unwrap_or(0))
        }
    }
}

/// Add `delta` months to a date, clamping the day to the target month's length.
fn add_months(d: NaiveDate, delta: i64) -> NaiveDate {
    let total = (d.year() as i64) * 12 + (d.month() as i64 - 1) + delta;
    let year = total.div_euclid(12) as i32;
    let month = total.rem_euclid(12) as u32 + 1;
    let day = d.day().min(last_day_of_month(year, month));
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(year, month, 1).unwrap())
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(ny, nm, 1).unwrap();
    (first_next - chrono::Duration::days(1)).day()
}

/// Parse an ISO `YYYY-MM-DD` date string (the form carried in a query's
/// `date_range`). The leading 10 chars are taken so a full timestamp also parses.
pub fn parse_iso_date(s: &str) -> Result<NaiveDate, String> {
    let head = s.get(0..10).unwrap_or(s);
    NaiveDate::parse_from_str(head, "%Y-%m-%d")
        .map_err(|_| format!("invalid date '{}': expected YYYY-MM-DD", s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_interval_forms() {
        assert_eq!(
            Interval::parse("1 year").unwrap(),
            Interval { n: 1, unit: IntervalUnit::Year }
        );
        assert_eq!(
            Interval::parse("14 months").unwrap(),
            Interval { n: 14, unit: IntervalUnit::Month }
        );
        assert_eq!(
            Interval::parse("2 quarters").unwrap(),
            Interval { n: 2, unit: IntervalUnit::Quarter }
        );
        assert!(Interval::parse("year").is_err());
        assert!(Interval::parse("1 fortnight").is_err());
    }

    #[test]
    fn sql_literal_normalizes_to_safe_units() {
        // year/quarter normalize to months; week/day to days — units every SQL
        // engine accepts in an INTERVAL literal.
        assert_eq!(Interval::parse("1 year").unwrap().sql_literal(), "12 month");
        assert_eq!(Interval::parse("2 quarters").unwrap().sql_literal(), "6 month");
        assert_eq!(Interval::parse("14 months").unwrap().sql_literal(), "14 month");
        assert_eq!(Interval::parse("1 week").unwrap().sql_literal(), "7 day");
        assert_eq!(Interval::parse("3 days").unwrap().sql_literal(), "3 day");
    }

    #[test]
    fn commensurability_guards_bucket_grid() {
        let year = Interval::parse("1 year").unwrap();
        assert!(year.check_commensurable("year").is_ok());
        assert!(year.check_commensurable("quarter").is_ok());
        assert!(year.check_commensurable("month").is_ok());
        // week/day buckets cannot tile a 1-year (month-family) shift.
        assert!(year.check_commensurable("week").is_err());
        assert!(year.check_commensurable("day").is_err());

        let month = Interval::parse("1 month").unwrap();
        assert!(month.check_commensurable("week").is_err());

        let week = Interval::parse("2 weeks").unwrap();
        assert!(week.check_commensurable("week").is_ok());
        assert!(week.check_commensurable("day").is_ok());
        assert!(week.check_commensurable("month").is_err());
    }

    #[test]
    fn subtracts_year() {
        let d = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let i = Interval::parse("1 year").unwrap();
        assert_eq!(i.subtract_from(d), NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
    }

    #[test]
    fn maturity_offset_pushes_cutoff_earlier() {
        // c_start - 1 year - 14 months = 2025-01-01 - 14 months = 2023-11-01
        let c_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let by = Interval::parse("1 year").unwrap();
        let maturity = Interval::parse("14 months").unwrap();
        let shifted_start = by.subtract_from(c_start);
        let cutoff = maturity.subtract_from(shifted_start);
        assert_eq!(cutoff, NaiveDate::from_ymd_opt(2023, 11, 1).unwrap());
    }

    #[test]
    fn month_subtraction_clamps_day() {
        // 2026-03-31 minus 1 month -> Feb has no 31st, clamp to 2026-02-28.
        let d = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let i = Interval::parse("1 month").unwrap();
        assert_eq!(i.subtract_from(d), NaiveDate::from_ymd_opt(2026, 2, 28).unwrap());
    }
}
