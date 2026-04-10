//! Looker LookML parser.
//!
//! Parses the LookML DSL (.lkml files) and converts to airlayer views. Supports:
//! - Views: sql_table_name, derived_table, dimensions, dimension_groups, measures, filters, sets
//! - Dimension types: string, number, yesno, tier, zipcode, date, time, datetime, etc.
//! - Dimension groups with timeframes (time, duration)
//! - Measure types: count, sum, average, min, max, count_distinct, list, etc.
//! - Explores with joins and relationship types
//! - ${TABLE}.column and ${view_name.field} references
//! - Measure filters via filters parameter
//! - Primary key detection

use super::{ConversionResult, parse_foreign_dimension_type, parse_foreign_measure_type,
            relationship_to_entity_type, rewrite_dollar_refs, extract_dollar_join_key};
use crate::schema::models::*;

// ── LookML parser ────────────────────────────────────────────────────

/// Parsed LookML block — a key-value structure where values can be scalars
/// or nested blocks.
#[derive(Debug, Clone)]
enum LkmlValue {
    Scalar(String),
    Block(Vec<(String, LkmlValue)>),
    List(Vec<String>),
}

/// Parse a LookML file into a list of top-level blocks.
fn parse_lkml(content: &str) -> Result<Vec<(String, String, Vec<(String, LkmlValue)>)>, String> {
    let mut blocks = Vec::new();
    let clean = strip_comments(content);
    let chars: Vec<char> = clean.chars().collect();
    let mut pos = 0;

    while pos < chars.len() {
        skip_whitespace(&chars, &mut pos);
        if pos >= chars.len() {
            break;
        }

        // Parse block_type: block_name { ... }
        let block_type = read_identifier(&chars, &mut pos);
        if block_type.is_empty() {
            pos += 1;
            continue;
        }

        skip_whitespace(&chars, &mut pos);
        if pos >= chars.len() || chars[pos] != ':' {
            pos += 1;
            continue;
        }
        pos += 1; // skip ':'
        skip_whitespace(&chars, &mut pos);

        let block_name = read_identifier(&chars, &mut pos);
        skip_whitespace(&chars, &mut pos);

        if pos >= chars.len() || chars[pos] != '{' {
            // Not a block — skip
            continue;
        }
        pos += 1; // skip '{'

        let fields = parse_block_body(&chars, &mut pos)?;
        blocks.push((block_type, block_name, fields));
    }

    Ok(blocks)
}

/// Strip LookML comments (# to end of line, but not inside strings).
fn strip_comments(content: &str) -> String {
    let mut result = String::new();
    let mut in_string = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '"' && !in_string {
            in_string = true;
            result.push(c);
        } else if c == '"' && in_string {
            in_string = false;
            result.push(c);
        } else if c == '#' && !in_string {
            // Skip to end of line
            while let Some(&nc) = chars.peek() {
                if nc == '\n' {
                    break;
                }
                chars.next();
            }
            result.push('\n');
        } else {
            result.push(c);
        }
    }
    result
}

fn skip_whitespace(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_whitespace() {
        *pos += 1;
    }
}

fn read_identifier(chars: &[char], pos: &mut usize) -> String {
    let mut s = String::new();
    while *pos < chars.len() && (chars[*pos].is_alphanumeric() || chars[*pos] == '_') {
        s.push(chars[*pos]);
        *pos += 1;
    }
    s
}

/// Parse fields inside a { ... } block. Handles nested blocks, scalars, and lists.
///
/// LookML patterns:
///   `key: value ;;`          — scalar
///   `key: [a, b, c]`         — list
///   `key: { ... }`           — anonymous block
///   `key: name { ... }`      — named block (dimension, measure, join, etc.)
fn parse_block_body(
    chars: &[char],
    pos: &mut usize,
) -> Result<Vec<(String, LkmlValue)>, String> {
    let mut fields = Vec::new();

    loop {
        skip_whitespace(chars, pos);
        if *pos >= chars.len() {
            break;
        }
        if chars[*pos] == '}' {
            *pos += 1;
            break;
        }

        // Read field name
        let name = read_identifier(chars, pos);
        if name.is_empty() {
            // Skip unexpected characters
            *pos += 1;
            continue;
        }

        skip_whitespace(chars, pos);

        if *pos >= chars.len() {
            break;
        }

        if chars[*pos] == ':' {
            *pos += 1; // skip ':'
            skip_whitespace(chars, pos);

            if *pos < chars.len() && chars[*pos] == '{' {
                // Anonymous block: `key: { ... }`
                *pos += 1;
                let sub_fields = parse_block_body(chars, pos)?;
                fields.push((name, LkmlValue::Block(sub_fields)));
            } else if *pos < chars.len() && chars[*pos] == '[' {
                // List value: `key: [a, b, c]`
                *pos += 1;
                let list = parse_list(chars, pos);
                fields.push((name, LkmlValue::List(list)));
            } else {
                // Could be a scalar OR a named block.
                // Read the text after `:` — if we hit `{`, it's a named block.
                // E.g., `dimension: id { ... }` or `sql: ${TABLE}.id ;;`
                let text = peek_until_brace_or_semicolons(chars, pos);

                if *pos < chars.len() && chars[*pos] == '{' {
                    // Named block: `key: name { ... }`
                    let block_name = text.trim().to_string();
                    *pos += 1; // skip '{'
                    let mut sub_fields = parse_block_body(chars, pos)?;
                    // Insert the block name as a "name" field
                    if !block_name.is_empty() {
                        sub_fields.insert(
                            0,
                            ("name".to_string(), LkmlValue::Scalar(block_name)),
                        );
                    }
                    fields.push((name, LkmlValue::Block(sub_fields)));
                } else {
                    // Scalar value — already consumed up to ;;
                    let value = text.trim().trim_matches('"').to_string();
                    fields.push((name, LkmlValue::Scalar(value)));
                }
            }
        } else if *pos < chars.len() && chars[*pos] == '{' {
            // Block without colon
            *pos += 1;
            let sub_fields = parse_block_body(chars, pos)?;
            fields.push((name, LkmlValue::Block(sub_fields)));
        }
    }

    Ok(fields)
}

/// Read text until we encounter `{`, `;;`, or a newline followed by another field.
/// For `{`, doesn't consume it (caller handles it). For `;;`, consumes them.
/// For a simple value like `type: time\n`, reads until end of line.
fn peek_until_brace_or_semicolons(chars: &[char], pos: &mut usize) -> String {
    let mut value = String::new();
    let mut in_string = false;

    while *pos < chars.len() {
        // Check for `;;`
        if !in_string && *pos + 1 < chars.len() && chars[*pos] == ';' && chars[*pos + 1] == ';' {
            *pos += 2; // consume ;;
            break;
        }
        // Check for `{` — don't consume it (caller handles it).
        // But skip `${` which is a LookML variable reference, not a block start.
        if !in_string && chars[*pos] == '{' {
            let prev_dollar = *pos > 0 && chars[*pos - 1] == '$';
            if !prev_dollar {
                break;
            }
        }
        // Check for `}` — don't consume it (parent block handles it).
        // But skip `}` inside `${ }` references.
        if !in_string && chars[*pos] == '}' {
            // Count unclosed `${` references
            let opens = value.matches("${").count();
            let closes = value.matches('}').count();
            if opens <= closes {
                break;
            }
        }
        // Newline — check if the next non-whitespace is a new field identifier or `}`
        if !in_string && chars[*pos] == '\n' {
            // Peek ahead to see if the next line starts a new field or block end
            let mut peek = *pos + 1;
            while peek < chars.len() && (chars[peek] == ' ' || chars[peek] == '\t') {
                peek += 1;
            }
            if peek < chars.len()
                && (chars[peek] == '}' || (chars[peek].is_alphabetic() || chars[peek] == '_'))
            {
                // Next line starts a new field or closes the block — this value is done
                *pos = peek;
                break;
            }
        }
        if chars[*pos] == '"' {
            in_string = !in_string;
        }
        value.push(chars[*pos]);
        *pos += 1;
    }
    value
}

fn parse_list(chars: &[char], pos: &mut usize) -> Vec<String> {
    let mut items = Vec::new();
    loop {
        skip_whitespace(chars, pos);
        if *pos >= chars.len() || chars[*pos] == ']' {
            *pos += 1;
            // Skip trailing ;;
            skip_semicolons(chars, pos);
            break;
        }
        if chars[*pos] == ',' {
            *pos += 1;
            continue;
        }
        let item = read_list_item(chars, pos);
        if !item.is_empty() {
            items.push(item);
        }
    }
    items
}

fn read_list_item(chars: &[char], pos: &mut usize) -> String {
    skip_whitespace(chars, pos);
    let mut s = String::new();
    while *pos < chars.len() && chars[*pos] != ',' && chars[*pos] != ']' {
        s.push(chars[*pos]);
        *pos += 1;
    }
    s.trim().to_string()
}

fn skip_semicolons(chars: &[char], pos: &mut usize) {
    skip_whitespace(chars, pos);
    while *pos + 1 < chars.len() && chars[*pos] == ';' && chars[*pos + 1] == ';' {
        *pos += 2;
        skip_whitespace(chars, pos);
    }
}

// ── LookML → airlayer conversion ────────────────────────────────────

/// Convert LookML content into airlayer views.
pub fn convert(content: &str, source: &str) -> Result<ConversionResult, String> {
    let blocks = parse_lkml(content)
        .map_err(|e| format!("Failed to parse LookML from {}: {}", source, e))?;

    let mut views = Vec::new();
    let mut explores = Vec::new();
    let mut warnings = Vec::new();

    for (block_type, block_name, fields) in &blocks {
        match block_type.as_str() {
            "view" => {
                let view = convert_view(block_name, fields, &mut warnings);
                views.push(view);
            }
            "explore" => {
                explores.push((block_name.clone(), fields.clone()));
            }
            _ => {
                // model, access_grant, etc. — skip
            }
        }
    }

    // Apply explore joins to views
    for (explore_name, explore_fields) in &explores {
        apply_explore_joins(&mut views, explore_name, explore_fields, &mut warnings);
    }

    if views.is_empty() {
        return Err(format!("No views found in LookML file {}", source));
    }

    Ok(ConversionResult { views, warnings })
}

fn convert_view(
    name: &str,
    fields: &[(String, LkmlValue)],
    warnings: &mut Vec<String>,
) -> View {
    let mut table = None;
    let mut sql = None;
    let mut dimensions = Vec::new();
    let mut measures = Vec::new();
    let mut segments = Vec::new();
    let mut label = None;
    let mut description = None;

    for (key, value) in fields {
        match key.as_str() {
            "sql_table_name" => {
                if let LkmlValue::Scalar(v) = value {
                    table = Some(v.clone());
                }
            }
            "derived_table" => {
                if let LkmlValue::Block(dt_fields) = value {
                    for (dk, dv) in dt_fields {
                        if dk == "sql" {
                            if let LkmlValue::Scalar(s) = dv {
                                sql = Some(rewrite_dollar_refs(s, name));
                            }
                        }
                    }
                }
            }
            "label" => {
                if let LkmlValue::Scalar(v) = value {
                    label = Some(v.clone());
                }
            }
            "description" => {
                if let LkmlValue::Scalar(v) = value {
                    description = Some(v.clone());
                }
            }
            "dimension" | "dimension_group" => {
                if let LkmlValue::Block(dim_fields) = value {
                    let is_group = key == "dimension_group";
                    let mut dims = convert_lookml_dimension(dim_fields, name, is_group, warnings);
                    dimensions.append(&mut dims);
                }
            }
            "measure" => {
                if let LkmlValue::Block(m_fields) = value {
                    if let Some(m) = convert_lookml_measure(m_fields, name, warnings) {
                        measures.push(m);
                    }
                }
            }
            "filter" => {
                if let LkmlValue::Block(f_fields) = value {
                    // LookML filter fields become segments
                    if let Some(seg) = convert_lookml_filter_to_segment(f_fields, name) {
                        segments.push(seg);
                    }
                }
            }
            _ => {}
        }
    }

    // Default table to view name if no sql_table_name or derived_table
    if table.is_none() && sql.is_none() {
        table = Some(name.to_string());
    }

    // Build primary entity from primary key dimensions
    let mut entities = Vec::new();
    let pk_dim = dimensions.iter().find(|d| d.primary_key == Some(true));
    if let Some(pk) = pk_dim {
        entities.push(Entity {
            name: name.to_string(),
            entity_type: EntityType::Primary,
            description: None,
            key: Some(pk.name.clone()),
            keys: None,
            inherits_from: None,
            meta: None,
        });
    }

    View {
        name: name.to_string(),
        description: description
            .unwrap_or_else(|| format!("Converted from LookML view '{}'", name)),
        label,
        datasource: None,
        dialect: None,
        table,
        sql,
        entities,
        dimensions,
        measures: if measures.is_empty() {
            None
        } else {
            Some(measures)
        },
        segments,
        meta: None,
    }
}

fn convert_lookml_dimension(
    fields: &[(String, LkmlValue)],
    view_name: &str,
    is_group: bool,
    _warnings: &mut Vec<String>,
) -> Vec<Dimension> {
    let mut name = String::new();
    let mut dim_type = String::new();
    let mut sql_expr = String::new();
    let mut desc = None;
    let mut primary_key = None;
    let mut timeframes = Vec::new();
    let mut label = None;

    for (key, value) in fields {
        match key.as_str() {
            "name" => {
                if let LkmlValue::Scalar(v) = value {
                    name = v.clone();
                }
            }
            "type" => {
                if let LkmlValue::Scalar(v) = value {
                    dim_type = v.clone();
                }
            }
            "sql" => {
                if let LkmlValue::Scalar(v) = value {
                    sql_expr = v.clone();
                }
            }
            "description" => {
                if let LkmlValue::Scalar(v) = value {
                    desc = Some(v.clone());
                }
            }
            "label" => {
                if let LkmlValue::Scalar(v) = value {
                    label = Some(v.clone());
                }
            }
            "primary_key" => {
                if let LkmlValue::Scalar(v) = value {
                    primary_key = Some(v == "yes" || v == "true");
                }
            }
            "timeframes" => {
                if let LkmlValue::List(items) = value {
                    timeframes = items.clone();
                }
            }
            _ => {}
        }
    }

    if is_group && dim_type == "time" {
        // Dimension group generates multiple dimensions — one per timeframe
        if timeframes.is_empty() {
            timeframes = vec![
                "raw".into(),
                "time".into(),
                "date".into(),
                "week".into(),
                "month".into(),
                "quarter".into(),
                "year".into(),
            ];
        }
        let rewritten_sql = rewrite_dollar_refs(&sql_expr, view_name);
        return timeframes
            .iter()
            .map(|tf| {
                let dim_name = format!("{}_{}", name, tf);
                let dimension_type = match tf.as_str() {
                    "raw" | "time" => DimensionType::Datetime,
                    "date" | "week" | "month" | "quarter" | "year" => DimensionType::Date,
                    _ => DimensionType::String,
                };
                Dimension {
                    name: dim_name,
                    dimension_type,
                    description: desc.clone(),
                    expr: rewritten_sql.clone(),
                    original_expr: Some(sql_expr.clone()),
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                    meta: None,
                }
            })
            .collect();
    }

    if is_group && dim_type == "duration" {
        // Duration dimension groups — generate intervals
        let rewritten = rewrite_dollar_refs(&sql_expr, view_name);
        let intervals = if timeframes.is_empty() {
            vec!["day".to_string(), "hour".to_string(), "minute".to_string()]
        } else {
            timeframes
        };
        return intervals
            .iter()
            .map(|interval| Dimension {
                name: format!("{}_{}", name, interval),
                dimension_type: DimensionType::Number,
                description: desc.clone(),
                expr: rewritten.clone(),
                original_expr: Some(sql_expr.clone()),
                samples: None,
                synonyms: None,
                primary_key: None,
                sub_query: None,
                inherits_from: None,
                meta: None,
            })
            .collect();
    }

    let dimension_type = parse_foreign_dimension_type(&dim_type);

    let rewritten = rewrite_dollar_refs(&sql_expr, view_name);

    vec![Dimension {
        name: name.clone(),
        dimension_type,
        description: desc.or(label),
        expr: if rewritten.is_empty() {
            name
        } else {
            rewritten
        },
        original_expr: if sql_expr.is_empty() {
            None
        } else {
            Some(sql_expr)
        },
        samples: None,
        synonyms: None,
        primary_key,
        sub_query: None,
        inherits_from: None,
        meta: None,
    }]
}

fn convert_lookml_measure(
    fields: &[(String, LkmlValue)],
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Option<Measure> {
    let mut name = String::new();
    let mut measure_type_str = String::new();
    let mut sql_expr = String::new();
    let mut desc = None;
    let mut label = None;
    let mut filters = Vec::new();

    for (key, value) in fields {
        match key.as_str() {
            "name" => {
                if let LkmlValue::Scalar(v) = value {
                    name = v.clone();
                }
            }
            "type" => {
                if let LkmlValue::Scalar(v) = value {
                    measure_type_str = v.clone();
                }
            }
            "sql" => {
                if let LkmlValue::Scalar(v) = value {
                    sql_expr = v.clone();
                }
            }
            "description" => {
                if let LkmlValue::Scalar(v) = value {
                    desc = Some(v.clone());
                }
            }
            "label" => {
                if let LkmlValue::Scalar(v) = value {
                    label = Some(v.clone());
                }
            }
            "filters" => {
                if let LkmlValue::Block(filter_fields) = value {
                    // LookML measure filters: `filters: [field: "view.dim", value: "val"]`
                    let mut field = String::new();
                    let mut val = String::new();
                    for (fk, fv) in filter_fields {
                        match fk.as_str() {
                            "field" => {
                                if let LkmlValue::Scalar(v) = fv {
                                    field = v.clone();
                                }
                            }
                            "value" => {
                                if let LkmlValue::Scalar(v) = fv {
                                    val = v.clone();
                                }
                            }
                            _ => {}
                        }
                    }
                    if !field.is_empty() {
                        let filter_expr = format!("{} = '{}'", rewrite_dollar_refs(&field, view_name), val);
                        filters.push(MeasureFilter {
                            expr: filter_expr,
                            original_expr: None,
                            description: None,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    if name.is_empty() {
        return None;
    }

    let measure_type = parse_foreign_measure_type(&measure_type_str);

    let rewritten = rewrite_dollar_refs(&sql_expr, view_name);
    let expr = if rewritten.is_empty() {
        None
    } else {
        Some(rewritten)
    };

    let rolling_window = if measure_type_str == "running_total" {
        Some(RollingWindow {
            trailing: Some("unbounded".to_string()),
            leading: None,
            offset: None,
        })
    } else {
        None
    };

    Some(Measure {
        name,
        measure_type,
        description: desc.or(label),
        expr,
        original_expr: if sql_expr.is_empty() {
            None
        } else {
            Some(sql_expr)
        },
        filters: if filters.is_empty() {
            None
        } else {
            Some(filters)
        },
        samples: None,
        synonyms: None,
        rolling_window,
        inherits_from: None,
        meta: None,
    })
}

fn convert_lookml_filter_to_segment(
    fields: &[(String, LkmlValue)],
    view_name: &str,
) -> Option<Segment> {
    let mut name = String::new();
    let mut sql_expr = String::new();
    let mut desc = None;

    for (key, value) in fields {
        match key.as_str() {
            "name" => {
                if let LkmlValue::Scalar(v) = value {
                    name = v.clone();
                }
            }
            "sql" => {
                if let LkmlValue::Scalar(v) = value {
                    sql_expr = v.clone();
                }
            }
            "description" => {
                if let LkmlValue::Scalar(v) = value {
                    desc = Some(v.clone());
                }
            }
            _ => {}
        }
    }

    if name.is_empty() || sql_expr.is_empty() {
        return None;
    }

    Some(Segment {
        name,
        expr: rewrite_dollar_refs(&sql_expr, view_name),
        description: desc,
        inherits_from: None,
        meta: None,
    })
}

/// Apply explore-level joins to the corresponding view entities.
fn apply_explore_joins(
    views: &mut [View],
    explore_name: &str,
    fields: &[(String, LkmlValue)],
    _warnings: &mut Vec<String>,
) {
    for (key, value) in fields {
        if key == "join" {
            if let LkmlValue::Block(join_fields) = value {
                let mut join_name = String::new();
                let mut relationship = String::new();
                let mut sql_on = String::new();

                for (jk, jv) in join_fields {
                    match jk.as_str() {
                        "name" => {
                            if let LkmlValue::Scalar(v) = jv {
                                join_name = v.clone();
                            }
                        }
                        "relationship" => {
                            if let LkmlValue::Scalar(v) = jv {
                                relationship = v.clone();
                            }
                        }
                        "sql_on" => {
                            if let LkmlValue::Scalar(v) = jv {
                                sql_on = v.clone();
                            }
                        }
                        _ => {}
                    }
                }

                if join_name.is_empty() {
                    continue;
                }

                let entity_type = relationship_to_entity_type(&relationship);

                // Extract the join key from sql_on
                let fk = extract_dollar_join_key(&sql_on, explore_name);

                // Add foreign entity to the base explore view
                if let Some(base_view) = views.iter_mut().find(|v| v.name == *explore_name) {
                    base_view.entities.push(Entity {
                        name: join_name,
                        entity_type,
                        description: None,
                        key: fk,
                        keys: None,
                        inherits_from: None,
                        meta: None,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_lkml_raw_blocks() {
        let lkml = r#"
view: orders {
  sql_table_name: public.orders ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, date, month, year]
    sql: ${TABLE}.created_at ;;
  }
}
"#;
        let blocks = parse_lkml(lkml).unwrap();
        assert_eq!(blocks.len(), 1, "Should have 1 top-level block");
        let (_bt, _bn, fields) = &blocks[0];

        // Check that dimension_group was parsed
        let dim_group = fields.iter().find(|(k, _)| k == "dimension_group");
        assert!(dim_group.is_some(), "Should have dimension_group field. Fields: {:?}", fields.iter().map(|(k, _)| k).collect::<Vec<_>>());

        if let Some((_, LkmlValue::Block(df))) = dim_group {
            // Check name was inserted
            let name_field = df.iter().find(|(k, _)| k == "name");
            assert!(name_field.is_some(), "Should have name field. Block fields: {:?}", df.iter().map(|(k, _)| k).collect::<Vec<_>>());

            // Check timeframes list
            let tf_field = df.iter().find(|(k, _)| k == "timeframes");
            assert!(tf_field.is_some(), "Should have timeframes field");
            if let Some((_, LkmlValue::List(items))) = tf_field {
                assert_eq!(items.len(), 4, "Should have 4 timeframes, got {:?}", items);
            } else {
                panic!("timeframes should be a List");
            }
        } else {
            panic!("dimension_group should be a Block");
        }
    }


    #[test]
    fn test_rewrite_dollar_refs() {
        assert_eq!(rewrite_dollar_refs("${TABLE}.id", "orders"), "id");
        assert_eq!(
            rewrite_dollar_refs("${orders.id}", "orders"),
            "id"
        );
        assert_eq!(
            rewrite_dollar_refs("${users.id}", "orders"),
            "{{users.id}}"
        );
    }

    #[test]
    fn test_parse_simple_view() {
        let lkml = r#"
view: orders {
  sql_table_name: public.orders ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, date, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${TABLE}.amount ;;
  }
}
"#;

        let result = convert(lkml, "test.lkml").unwrap();
        assert_eq!(result.views.len(), 1);

        let view = &result.views[0];
        assert_eq!(view.name, "orders");
        assert_eq!(view.table, Some("public.orders".to_string()));

        // id + status + 4 time dimensions (raw, date, month, year)
        assert_eq!(view.dimensions.len(), 6);
        assert_eq!(view.dimensions[0].name, "id");
        assert_eq!(view.dimensions[0].primary_key, Some(true));
        assert_eq!(view.dimensions[1].name, "status");

        // Time dimension group
        assert_eq!(view.dimensions[2].name, "created_raw");
        assert_eq!(view.dimensions[2].dimension_type, DimensionType::Datetime);
        assert_eq!(view.dimensions[3].name, "created_date");
        assert_eq!(view.dimensions[3].dimension_type, DimensionType::Date);

        let measures = view.measures_list();
        assert_eq!(measures.len(), 2);
        assert_eq!(measures[0].measure_type, MeasureType::Count);
        assert_eq!(measures[1].measure_type, MeasureType::Sum);
    }

    #[test]
    fn test_parse_explore_with_joins() {
        let lkml = r#"
view: orders {
  sql_table_name: orders ;;
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }
  measure: count {
    type: count
  }
}

view: users {
  sql_table_name: users ;;
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
  measure: count {
    type: count
  }
}

explore: orders {
  join: users {
    sql_on: ${orders.user_id} = ${users.id} ;;
    relationship: many_to_one
  }
}
"#;

        let result = convert(lkml, "test.lkml").unwrap();
        assert_eq!(result.views.len(), 2);

        let orders = &result.views[0];
        // Should have primary entity + foreign entity from explore join
        let foreign = orders
            .entities
            .iter()
            .find(|e| e.name == "users")
            .expect("Should have foreign entity for users");
        assert_eq!(foreign.entity_type, EntityType::Foreign);
        assert_eq!(foreign.key, Some("user_id".to_string()));
    }

    #[test]
    fn test_parse_derived_table() {
        let lkml = r#"
view: active_users {
  derived_table: {
    sql: SELECT * FROM users WHERE active = true ;;
  }
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  measure: count {
    type: count
  }
}
"#;

        let result = convert(lkml, "test.lkml").unwrap();
        let view = &result.views[0];
        assert!(view.table.is_none());
        assert!(view.sql.is_some());
        assert!(view.sql.as_ref().unwrap().contains("SELECT * FROM users"));
    }

    #[test]
    fn test_parse_yesno_dimension() {
        let lkml = r#"
view: orders {
  sql_table_name: orders ;;
  dimension: id {
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: is_completed {
    type: yesno
    sql: ${TABLE}.status = 'completed' ;;
  }
}
"#;

        let result = convert(lkml, "test.lkml").unwrap();
        let dim = result.views[0]
            .dimensions
            .iter()
            .find(|d| d.name == "is_completed")
            .unwrap();
        assert_eq!(dim.dimension_type, DimensionType::Boolean);
    }

    #[test]
    fn test_strip_comments() {
        let input = r#"
view: orders { # This is a comment
  dimension: id { # Another comment
    type: number
  }
}
"#;
        let cleaned = strip_comments(input);
        assert!(!cleaned.contains("This is a comment"));
        assert!(cleaned.contains("view"));
    }
}
