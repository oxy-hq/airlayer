use std::fmt;

#[derive(Debug)]
pub enum EngineError {
    SchemaError(String),
    QueryError(String),
    JoinError(String),
    SqlGenerationError(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            EngineError::QueryError(msg) => write!(f, "Query error: {}", msg),
            EngineError::JoinError(msg) => write!(f, "Join error: {}", msg),
            EngineError::SqlGenerationError(msg) => write!(f, "SQL generation error: {}", msg),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<String> for EngineError {
    fn from(s: String) -> Self {
        EngineError::SchemaError(s)
    }
}
