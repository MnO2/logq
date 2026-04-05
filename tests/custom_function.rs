use logq::functions::{FunctionRegistry, FunctionDef, Arity, NullHandling, register_all};
use logq::common::types::Value;

#[test]
fn test_custom_function_registration() {
    let mut registry = register_all().unwrap();
    registry.register(FunctionDef {
        name: "double".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(i) => Ok(Value::Int(i * 2)),
            _ => panic!("unexpected type"),
        }),
    }).unwrap();

    assert_eq!(registry.call("double", &[Value::Int(21)]), Ok(Value::Int(42)));
    assert_eq!(registry.call("double", &[Value::Null]), Ok(Value::Null));
}
