pub mod schema;
pub mod engine;
pub mod dialect;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(not(feature = "wasm"))]
pub mod executor;

#[cfg(feature = "wasm")]
pub mod wasm;

pub use engine::{SemanticEngine, DatasourceDialectMap, DatabaseConfig, PartialConfig};
pub use schema::models::{
    View, Dimension, Measure, Entity, Topic, SemanticLayer,
    Motif, MotifKind, MotifParam, MotifOutputColumn,
    Sequence, SequenceStep, SequenceParam,
};
pub use dialect::Dialect;

#[cfg(not(feature = "wasm"))]
pub use executor::{ExecutionConfig, ExecutionResult, QueryEnvelope};
