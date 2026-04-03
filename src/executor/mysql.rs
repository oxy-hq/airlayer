//! MySQL query executor.

use super::{ExecutionResult, MySqlConnection};
use crate::engine::EngineError;
use mysql::prelude::Queryable;
use mysql::Row;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &MySqlConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(config.get_host()))
        .tcp_port(config.get_port().parse::<u16>().unwrap_or(3306))
        .user(Some(config.get_user()))
        .pass(Some(config.get_password()?))
        .db_name(Some(config.get_database()));

    let pool = mysql::Pool::new(opts)
        .map_err(|e| EngineError::QueryError(format!("Failed to connect to MySQL: {}", e)))?;

    let mut conn = pool
        .get_conn()
        .map_err(|e| EngineError::QueryError(format!("Failed to get MySQL connection: {}", e)))?;

    let stmt = conn
        .prep(sql)
        .map_err(|e| EngineError::QueryError(format!("MySQL prepare failed: {}", e)))?;

    let params_mysql: Vec<mysql::Value> = params
        .iter()
        .map(|p| mysql::Value::from(p.as_str()))
        .collect();

    let rows: Vec<Row> = conn
        .exec(stmt, params_mysql)
        .map_err(|e| EngineError::QueryError(format!("MySQL query failed: {}", e)))?;

    if rows.is_empty() {
        return Ok(ExecutionResult {
            columns: vec![],
            rows: vec![],
        });
    }

    let columns: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();

    let mut result_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = mysql_value_to_json(row, i);
            obj.insert(col_name.clone(), val);
        }
        result_rows.push(obj);
    }

    Ok(ExecutionResult {
        columns,
        rows: result_rows,
    })
}

fn mysql_value_to_json(row: &Row, idx: usize) -> JsonValue {
    use mysql::Value;

    match row.as_ref(idx) {
        Some(Value::NULL) | None => JsonValue::Null,
        Some(Value::Int(n)) => JsonValue::Number((*n).into()),
        Some(Value::UInt(n)) => JsonValue::Number((*n).into()),
        Some(Value::Float(f)) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(Value::Double(f)) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(Value::Bytes(b)) => {
            match String::from_utf8(b.clone()) {
                Ok(s) => {
                    // MySQL returns DECIMAL/NUMERIC as Bytes — try to parse as number
                    if let Ok(n) = s.parse::<i64>() {
                        JsonValue::Number(n.into())
                    } else if let Ok(f) = s.parse::<f64>() {
                        serde_json::Number::from_f64(f)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::String(s))
                    } else {
                        JsonValue::String(s)
                    }
                }
                Err(_) => JsonValue::String(format!("<bytes {} len>", b.len())),
            }
        }
        Some(Value::Date(y, m, d, h, min, s, _us)) => {
            if *h == 0 && *min == 0 && *s == 0 {
                JsonValue::String(format!("{:04}-{:02}-{:02}", y, m, d))
            } else {
                JsonValue::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    y, m, d, h, min, s
                ))
            }
        }
        Some(Value::Time(neg, d, h, min, s, _us)) => {
            let sign = if *neg { "-" } else { "" };
            let total_hours = (*d as u32) * 24 + (*h as u32);
            JsonValue::String(format!("{}{:02}:{:02}:{:02}", sign, total_hours, min, s))
        }
    }
}
