use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Shared regex matching `{{entity.field}}` — two dot-separated identifiers in double braces.
/// Used by member_sql, sql_generator, and validator modules.
pub fn dotted_ref_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{\{(\w+)\.(\w+)\}\}").unwrap())
}

/// Shared regex matching `{{ param }}` — a single identifier in double braces (with optional whitespace).
/// Used by motifs and validator modules.
pub fn param_ref_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{\{\s*(\w+)\s*\}\}").unwrap())
}

/// Resolves `{{entity.field}}` cross-entity references in SQL expressions.
/// Converts them to proper join-qualified column references.
pub struct MemberSqlResolver;

impl MemberSqlResolver {
    /// Parse an expression and extract all cross-entity references.
    /// Returns Vec of (entity_name, field_name).
    pub fn extract_entity_refs(expr: &str) -> Vec<(String, String)> {
        let re = dotted_ref_regex();
        re.captures_iter(expr)
            .map(|cap| (cap[1].to_string(), cap[2].to_string()))
            .collect()
    }

    /// Check if an expression contains cross-entity references.
    pub fn has_entity_refs(expr: &str) -> bool {
        dotted_ref_regex().is_match(expr)
    }

    /// Resolve cross-entity references in an expression.
    /// `entity_to_view` maps entity_name -> (view_name, is_primary_in_this_view).
    /// The resolver replaces `{{entity.field}}` with `"view_alias"."field"`.
    pub fn resolve_refs(
        expr: &str,
        entity_to_alias: &std::collections::HashMap<String, String>,
        quote_fn: &dyn Fn(&str) -> String,
    ) -> String {
        let re = dotted_ref_regex();
        re.replace_all(expr, |caps: &regex::Captures<'_>| {
            let entity = &caps[1];
            let field = &caps[2];
            if let Some(alias) = entity_to_alias.get(entity) {
                format!("{}.{}", quote_fn(alias), quote_fn(field))
            } else {
                // Leave unresolved (will be caught during SQL generation)
                format!("{{{{{}.{}}}}}", entity, field)
            }
        })
        .to_string()
    }

    /// Check if an expression contains variable references.
    pub fn has_variable_refs(expr: &str) -> bool {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"\{\{variables\.[^}]+\}\}").unwrap());
        re.is_match(expr)
    }

    /// Check if an expression contains {{TABLE}} self-references.
    pub fn has_table_ref(expr: &str) -> bool {
        expr.contains("{{TABLE}}")
    }

    /// Resolve {{TABLE}} self-references in an expression.
    pub fn resolve_table_ref(
        expr: &str,
        view_alias: &str,
        quote_fn: &dyn Fn(&str) -> String,
    ) -> String {
        expr.replace("{{TABLE}}", &quote_fn(view_alias))
    }

    /// Extract variable names from an expression.
    pub fn extract_variable_refs(expr: &str) -> Vec<String> {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"\{\{(variables\.[^}]+)\}\}").unwrap());
        re.captures_iter(expr)
            .map(|cap| cap[1].to_string())
            .collect()
    }

    /// Substitute `{{ param }}` references in an expression with resolved values.
    /// Returns an error listing any unresolved param names.
    pub fn substitute_params(
        expr: &str,
        resolved: &HashMap<String, String>,
    ) -> Result<String, String> {
        let re = param_ref_regex();
        let mut unresolved: Vec<String> = Vec::new();
        let result = re
            .replace_all(expr, |caps: &regex::Captures| {
                let param_name = &caps[1];
                match resolved.get(param_name) {
                    Some(val) => val.clone(),
                    None => {
                        unresolved.push(param_name.to_string());
                        format!("{{{{ {} }}}}", param_name)
                    }
                }
            })
            .to_string();
        if unresolved.is_empty() {
            Ok(result)
        } else {
            Err(format!("unresolved param(s): {}", unresolved.join(", ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_entity_refs() {
        let refs =
            MemberSqlResolver::extract_entity_refs("{{order_item.quantity}} * {{product.price}}");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0], ("order_item".to_string(), "quantity".to_string()));
        assert_eq!(refs[1], ("product".to_string(), "price".to_string()));
    }

    #[test]
    fn test_no_entity_refs() {
        assert!(!MemberSqlResolver::has_entity_refs("simple_column"));
        assert!(!MemberSqlResolver::has_entity_refs("a + b"));
    }

    #[test]
    fn test_resolve_refs() {
        let mut entity_map = std::collections::HashMap::new();
        entity_map.insert("order_item".to_string(), "order_items".to_string());

        let result =
            MemberSqlResolver::resolve_refs("SUM({{order_item.quantity}})", &entity_map, &|s| {
                format!("\"{}\"", s)
            });
        assert_eq!(result, "SUM(\"order_items\".\"quantity\")");
    }

    #[test]
    fn test_variable_refs() {
        assert!(MemberSqlResolver::has_variable_refs(
            "{{variables.schema}}.table"
        ));
        assert!(!MemberSqlResolver::has_variable_refs("plain text"));

        let vars = MemberSqlResolver::extract_variable_refs("{{variables.db.schema}}.orders");
        assert_eq!(vars, vec!["variables.db.schema"]);
    }
}
