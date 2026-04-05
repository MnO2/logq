pub mod arithmetic;
pub mod array;
pub mod bitwise;
pub mod datetime;
pub mod host;
pub mod json;
pub mod map;
pub mod regexp;
pub mod registry;
pub mod string;
pub mod url;

pub use registry::{FunctionRegistry, FunctionDef, Arity, NullHandling, RegistryError};

pub fn register_all() -> Result<FunctionRegistry, RegistryError> {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry)?;
    array::register(&mut registry)?;
    bitwise::register(&mut registry)?;
    string::register(&mut registry)?;
    url::register(&mut registry)?;
    host::register(&mut registry)?;
    datetime::register(&mut registry)?;
    regexp::register(&mut registry)?;
    json::register(&mut registry)?;
    map::register(&mut registry)?;
    Ok(registry)
}
