use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Global semantic definitions that views can inherit from.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalSemantics {
    #[serde(default)]
    pub entities: HashMap<String, GlobalEntity>,
    #[serde(default)]
    pub dimensions: HashMap<String, GlobalDimension>,
    #[serde(default, deserialize_with = "deserialize_measures")]
    pub measures: HashMap<String, GlobalMeasure>,
    #[serde(default)]
    pub descriptions: HashMap<String, String>,
}

/// Measures can be either:
/// 1. A HashMap<String, GlobalMeasure> (standard map format)
/// 2. A list of flat maps where the first key is the measure name (YAML quirk):
///    ```yaml
///    measures:
///      - total_sales:
///        name: total_sales
///        type: sum
///    ```
///    This parses as [{total_sales: null, name: "total_sales", type: "sum"}]
fn deserialize_measures<'de, D>(deserializer: D) -> Result<HashMap<String, GlobalMeasure>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_yaml::Value::deserialize(deserializer)?;
    let mut result = HashMap::new();

    match value {
        serde_yaml::Value::Mapping(map) => {
            // Standard map format: measures: {name: {type: ..., expr: ...}}
            for (key, val) in map {
                if let serde_yaml::Value::String(name) = key {
                    if let Ok(measure) = serde_yaml::from_value::<GlobalMeasure>(val) {
                        result.insert(name, measure);
                    }
                }
            }
        }
        serde_yaml::Value::Sequence(list) => {
            // List format with flat keys
            for item in list {
                if let serde_yaml::Value::Mapping(map) = item {
                    // The measure name and type/expr/etc are all at the same level
                    let mut name = None;
                    let mut measure_type = None;
                    let mut expr = None;
                    let mut description = None;

                    for (k, v) in &map {
                        if let serde_yaml::Value::String(key) = k {
                            match key.as_str() {
                                "name" => {
                                    name = v.as_str().map(|s| s.to_string());
                                }
                                "type" => {
                                    measure_type = v.as_str().map(|s| s.to_string());
                                }
                                "expr" => {
                                    expr = v.as_str().map(|s| s.to_string());
                                }
                                "description" => {
                                    description = v.as_str().map(|s| s.to_string());
                                }
                                _ => {
                                    // The first null-valued key is likely the measure name
                                    if v.is_null() && name.is_none() {
                                        name = Some(key.clone());
                                    }
                                }
                            }
                        }
                    }

                    if let (Some(n), Some(mt)) = (name, measure_type) {
                        result.insert(
                            n,
                            GlobalMeasure {
                                measure_type: mt,
                                expr,
                                description,
                                filters: None,
                                samples: None,
                                synonyms: None,
                                name: None,
                            },
                        );
                    }
                }
            }
        }
        _ => {}
    }

    Ok(result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalEntity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalDimension {
    #[serde(rename = "type")]
    pub dimension_type: String,
    pub expr: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalMeasure {
    #[serde(rename = "type")]
    pub measure_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<GlobalMeasureFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalMeasureFilter {
    pub expr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Wrapper for the globals file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalsFile {
    #[serde(default)]
    pub semantics: Option<GlobalSemantics>,
}

impl GlobalSemantics {
    /// Load from a YAML file. Returns empty if the file doesn't exist.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read globals file: {}", e))?;

        // Try parsing directly as GlobalSemantics first, then as GlobalsFile wrapper
        if let Ok(direct) = serde_yaml::from_str::<GlobalSemantics>(&content) {
            if !direct.entities.is_empty()
                || !direct.dimensions.is_empty()
                || !direct.measures.is_empty()
            {
                return Ok(direct);
            }
        }
        // Try as a wrapper with semantics key
        if let Ok(file) = serde_yaml::from_str::<GlobalsFile>(&content) {
            if let Some(semantics) = file.semantics {
                return Ok(semantics);
            }
        }
        // Fallback: try direct again and report error
        serde_yaml::from_str::<GlobalSemantics>(&content)
            .map_err(|e| format!("Failed to parse globals: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_example_globals() {
        let path = std::path::Path::new(
            "/Users/robertyi/repos/oxy-internal/examples/globals/semantics.yml",
        );
        if !path.exists() {
            return; // skip if file not available
        }
        let g = GlobalSemantics::load_from_file(path).unwrap();
        println!("Entities: {:?}", g.entities.keys().collect::<Vec<_>>());
        println!("Dimensions: {:?}", g.dimensions.keys().collect::<Vec<_>>());
        println!("Measures: {:?}", g.measures.keys().collect::<Vec<_>>());
        assert!(!g.entities.is_empty(), "entities should not be empty");
        assert!(!g.dimensions.is_empty(), "dimensions should not be empty");
    }
}
