pub mod dialect;
pub mod engine;
pub mod schema;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(not(feature = "wasm"))]
pub mod executor;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "python")]
pub mod python;

pub use dialect::Dialect;
pub use engine::{DatabaseConfig, DatasourceDialectMap, PartialConfig, SemanticEngine};
pub use schema::foreign::{self, ForeignFormat};
pub use schema::models::{
    Dimension, Entity, Measure, Motif, MotifKind, MotifOutputColumn, MotifParam, SavedQuery,
    SavedQueryParam, SavedQueryStep, SemanticLayer, Topic, View,
};

#[cfg(not(feature = "wasm"))]
pub use executor::{ExecutionConfig, ExecutionResult, QueryEnvelope};
