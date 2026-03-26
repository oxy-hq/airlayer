//! Postgres query executor (also used for Redshift).

use super::{ExecutionResult, PostgresConnection};
use crate::engine::EngineError;
use postgres::types::Type;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &PostgresConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let conn_str = config.connection_string()?;
    let mut client = postgres::Client::connect(&conn_str, postgres::NoTls).map_err(|e| {
        EngineError::QueryError(format!("Failed to connect to Postgres: {}", e))
    })?;

    let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p as &(dyn postgres::types::ToSql + Sync))
        .collect();

    let rows = client.query(sql, &param_refs).map_err(|e| {
        EngineError::QueryError(format!("Postgres query failed: {}", e))
    })?;

    // Use a simple query to get columns even if empty (all rows share the same schema)
    let columns: Vec<String> = if rows.is_empty() {
        vec![]
    } else {
        rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect()
    };

    if rows.is_empty() {
        return Ok(ExecutionResult {
            columns,
            rows: vec![],
        });
    }

    let mut result_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in row.columns().iter().enumerate() {
            let val = pg_value_to_json(&row, i, col.type_());
            obj.insert(col.name().to_string(), val);
        }
        result_rows.push(obj);
    }

    Ok(ExecutionResult {
        columns,
        rows: result_rows,
    })
}

fn pg_value_to_json(row: &postgres::Row, idx: usize, ty: &Type) -> JsonValue {
    // Try common types in order of likelihood
    match *ty {
        Type::BOOL => row
            .get::<_, Option<bool>>(idx)
            .map_or(JsonValue::Null, JsonValue::Bool),
        Type::INT2 => row
            .get::<_, Option<i16>>(idx)
            .map_or(JsonValue::Null, |v| JsonValue::Number(v.into())),
        Type::INT4 => row
            .get::<_, Option<i32>>(idx)
            .map_or(JsonValue::Null, |v| JsonValue::Number(v.into())),
        Type::INT8 => row
            .get::<_, Option<i64>>(idx)
            .map_or(JsonValue::Null, |v| JsonValue::Number(v.into())),
        Type::FLOAT4 => row.get::<_, Option<f32>>(idx).map_or(JsonValue::Null, |v| {
            serde_json::Number::from_f64(v as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }),
        Type::FLOAT8 => row.get::<_, Option<f64>>(idx).map_or(JsonValue::Null, |v| {
            serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }),
        Type::NUMERIC => {
            // Read NUMERIC as f64 (loses precision for very large decimals,
            // but sufficient for aggregated results)
            row.get::<_, Option<f64>>(idx).map_or(JsonValue::Null, |v| {
                // If it's a whole number, emit as integer
                if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
                    JsonValue::Number((v as i64).into())
                } else {
                    serde_json::Number::from_f64(v)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null)
                }
            })
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
            .get::<_, Option<String>>(idx)
            .map_or(JsonValue::Null, JsonValue::String),
        Type::TIMESTAMP | Type::TIMESTAMPTZ => row
            .get::<_, Option<chrono::NaiveDateTime>>(idx)
            .map_or(JsonValue::Null, |dt| JsonValue::String(dt.to_string())),
        Type::DATE => row
            .get::<_, Option<chrono::NaiveDate>>(idx)
            .map_or(JsonValue::Null, |d| JsonValue::String(d.to_string())),
        _ => {
            // Fallback: try to read as string
            row.get::<_, Option<String>>(idx)
                .map_or(JsonValue::Null, JsonValue::String)
        }
    }
}
