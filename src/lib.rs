extern crate chrono;
extern crate nom;
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate pdatastructs;

pub mod app;
pub mod common;
pub mod execution;
pub mod functions;
pub mod logical;
pub mod syntax;

#[cfg(feature = "bench-internals")]
pub mod bench_internals {
    // Parser – wrapped to avoid exposing private ast::Query type
    pub fn parse_query(input: &str) -> bool {
        let result = crate::syntax::parser::query(input);
        std::hint::black_box(&result);
        result.is_ok()
    }

    // AST types
    pub use crate::syntax::ast::{PathExpr, PathSegment};

    // Stream types
    pub use crate::execution::stream::{
        Record, RecordStream, InMemoryStream,
        MapStream, FilterStream, GroupByStream, LimitStream,
    };

    // Execution plan types
    pub use crate::execution::types::{
        Named, Formula, Expression, Relation, Node,
        NamedAggregate, Aggregate, Ordering, StreamResult,
    };

    // Datasource
    pub use crate::execution::datasource::{ReaderBuilder, RecordRead, Reader};

    // Common types
    pub use crate::common::types::{Value, Variables, VariableName, DataSource};
}
