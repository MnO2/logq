extern crate chrono;
extern crate nom;
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate pdatastructs;

pub mod app;
pub mod common;
mod diagnostic;
pub mod execution;
pub mod functions;
pub mod logical;
pub mod simd;
pub mod syntax;

#[cfg(feature = "bench-internals")]
pub mod bench_internals {
    // Parser – wrapped to avoid exposing private ast::Query type
    pub fn parse_query(input: &str) -> bool {
        let result = crate::syntax::parser::query(input);
        std::hint::black_box(&result);
        matches!(result, Ok((rest, _)) if rest.trim().is_empty())
    }

    // AST types
    pub use crate::syntax::ast::{PathExpr, PathSegment};

    // Stream types
    pub use crate::execution::stream::{
        FilterStream, GroupByStream, InMemoryStream, LimitStream, MapStream, Record, RecordStream,
    };

    // Execution plan types
    pub use crate::execution::types::{
        Aggregate, Expression, Formula, Named, NamedAggregate, Node, Ordering, Relation, StreamResult,
    };

    // Datasource
    pub use crate::execution::datasource::{Reader, ReaderBuilder, RecordRead};

    // Common types
    pub use crate::common::types::{DataSource, DataSourceRegistry, Value, VariableName, Variables};

    // PrefixSort
    pub use crate::execution::prefix_sort::PrefixSortEncoder;

    // SIMD foundation types
    pub use crate::simd::bitmap::Bitmap;
    pub use crate::simd::filter_cache::evaluate_cached_two_pass;
    pub use crate::simd::kernels::*;
    pub use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
    pub use crate::simd::selection::SelectionVector;

    // Batch execution types
    pub use crate::execution::batch::{BATCH_SIZE, BatchSchema, BatchStream, ColumnBatch, ColumnType, TypedColumn};
}
